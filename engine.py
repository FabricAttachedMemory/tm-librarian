#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian engine module
#---------------------------------------------------------------------------

import errno
import uuid
import time
import math
import stat
import sys
import traceback
from operator import attrgetter
from pdb import set_trace

from book_policy import BookPolicy
from book_shelf_bos import TMBook, TMShelf, TMBos
from cmdproto import LibrarianCommandProtocol
from frdnode import FRDnode

_ZERO_PREFIX = '.lfs_pending_zero_'     # agree with lfs_fuse.py


class LibrarianCommandEngine(object):

    @staticmethod
    def argparse_extend(parser):
        parser.add_argument('--nozombies',
                            help='Do not use ZOMBIE state in book lifecycle',
                            action='store_true')

    _book_size_bytes = 0
    _nvm_bytes_total = 0  # read from DB

    @property
    def book_size_bytes(self):
        return self._book_size_bytes

    @property
    def nvm_bytes_total(self):
        return self._nvm_bytes_total

    @classmethod
    def _nbooks(cls, nbytes):
        return int(math.ceil(float(nbytes) / float(cls._book_size_bytes)))

    def cmd_version(self, cmdict):
        """ Return librarian version
            In (dict)---
                None
            Out (dict) ---
                librarian version
        """
        return self.db.get_globals(only='version')

    def cmd_get_fs_stats(self, cmdict):
        """ Return globals
            In (dict)---
                None
            Out (dict) ---
                librarian version
        """
        globals = self.db.get_globals()
        globals['books_per_IG'] = self.books_per_IG
        return globals

    def cmd_create_shelf(self, cmdict):
        """ Create a new shelf
            In (dict)---
                name
            Out (dict) ---
                shelf data
        """
        # POSIX: if extant, open it; else create and then open
        try:
            shelf = self.cmd_open_shelf(cmdict)
            return shelf
        except Exception as e:
            pass
        self.errno = errno.EINVAL
        shelf = TMShelf(cmdict)
        self.db.create_shelf(shelf)
        self.db.create_xattr(shelf,
            BookPolicy.XATTR_ALLOCATION_POLICY,
            BookPolicy.DEFAULT_ALLOCATION_POLICY)

        # Will be ignored until AllocationPolicy set to RequestIG.  I just
        # want it to show up in a full xattr dump (getfattr -d /lfs/xxxx)
        self.db.create_xattr(shelf, BookPolicy.XATTR_IG_REQ, '')
        self.db.create_xattr(shelf, BookPolicy.XATTR_IG_REQ_POS, '')

        return self.cmd_open_shelf(cmdict)  # Does the handle thang

    def cmd_get_shelf(self, cmdict, match_id=False):
        """ List a given shelf.
            In (dict)---
                name
                optional flag to force a match on id (ie, already open)
            Out (TMShelf object) ---
                TMShelf object
        """
        self.errno = errno.EINVAL
        shelf = TMShelf(cmdict)
        assert shelf.name, 'Missing shelf name'
        if match_id:
            shelf.matchfields = ('name', 'id')
        else:
            shelf.matchfields = ('name', )
        shelf = self.db.get_shelf(shelf)
        if shelf is None:
            self.errno = errno.ENOENT  # FIXME: raise OSError instead?
            raise AssertionError('no such shelf %s' % cmdict['name'])
        # consistency checks
        self.errno = errno.EBADF
        assert self._nbooks(shelf.size_bytes) == shelf.book_count, (
            '%s size metadata mismatch' % shelf.name)
        self.errno = errno.ESTALE
        return shelf

    def cmd_list_shelves(self, cmdict):
        '''Returns a list.'''
        return self.db.get_shelf_all()

    def cmd_list_open_shelves(self, cmdict):
        '''Returns a list.'''
        return self.db.get_open_shelf_all()

    def cmd_open_shelf(self, cmdict):
        """ Open a shelf for access by a node.
            In (dict)---
                name
            Out ---
                TMShelf object
        """
        shelf = self.cmd_get_shelf(cmdict)  # may raise ENOENT
        self.db.modify_opened_shelves(shelf, 'get', cmdict['context'])
        return shelf

    def cmd_close_shelf(self, cmdict):
        """ Close a shelf against access by a node.
            In (dict)---
                shelf_id, name, handle
            Out (dict) ---
                TMShelf object
        """
        shelf = TMShelf(cmdict)
        shelf = self.db.modify_opened_shelves(shelf, 'put', cmdict['context'])
        return shelf

    def _list_shelf_books(self, shelf):
        self.errno = errno.EBADF
        assert shelf.id, '%s not open' % shelf.name
        bos = self.db.get_bos_by_shelf_id(shelf.id)

        # consistency checks.  Leave them both as different paths may
        # have been followed to retrieve the passed-in shelf.
        self.errno = errno.EREMOTEIO
        assert len(bos) == shelf.book_count, (
            '%s book count mismatch' % shelf.name)
        assert self._nbooks(shelf.size_bytes) == shelf.book_count, (
            '%s size metadata mismatch' % shelf.name)
        return bos

    def cmd_list_shelf_books(self, cmdict):
        shelf = self.cmd_get_shelf(cmdict)
        return self._list_shelf_books(shelf)

    def _set_book_alloc(self, bookorbos, newalloc):
        self.errno = errno.EUCLEAN
        try:    # What was passed in?
            _ = bookorbos.allocated
            book = bookorbos
        except AttributeError as e:
            book = self.db.get_book_by_id(bookorbos.book_id)
        if newalloc == book.allocated:
            return book
        msg = 'Book allocation %d -> %d' % (book.allocated, newalloc)
        if newalloc == TMBook.ALLOC_INUSE:
            assert book.allocated == TMBook.ALLOC_FREE, msg
        elif newalloc == TMBook.ALLOC_ZOMBIE:
            assert book.allocated == TMBook.ALLOC_INUSE, msg
            if self.nozombies:
                newalloc = TMBook.ALLOC_FREE
        elif newalloc == TMBook.ALLOC_FREE:
            assert book.allocated == TMBook.ALLOC_ZOMBIE, msg
        else:
            raise RuntimeError('Bad book allocation %d' % newalloc)
        book.allocated = newalloc
        book.matchfields = 'allocated'
        book = self.db.modify_book(book)
        self.errno = errno.ENOENT
        assert book, 'Book allocation change to %d failed' % newalloc
        return book

    def cmd_rename_shelf(self, cmdict):
        """Rename a shelf
            In (dict)---
                name (current)
                id
                newname
            Out (dict) ---
                shelf
        """
        self.errno = errno.ENOENT
        shelf = self.cmd_get_shelf(cmdict)
        shelf.name = cmdict['newname']
        shelf.matchfields = 'name'
        shelf = self.db.modify_shelf(shelf, commit=True)
        if shelf.name.startswith(_ZERO_PREFIX):  # zombify and unblock
            bos = self.db.get_bos_by_shelf_id(shelf.id)
            while bos:
                thisbos = bos.pop()
                _ = self._set_book_alloc(thisbos, TMBook.ALLOC_ZOMBIE)
            if shelf.mode & stat.S_IFBLK:
                # Turn into normal file so zeroing tools don't choke
                shelf.mode = stat.S_IFREG
                shelf.matchfields = 'mode'
                shelf = self.db.modify_shelf(shelf)
        return shelf

    # This is a protocol operation, not necessarily POSIX flow.  Search
    # in here for "unlink workflow", which first marks all books ZOMBIE,
    # zeroes them via dd, the zero truncates the shelf, and FINALLY
    # calls this with an emtpy shelf.  IOW calling this with a shelf
    # that still has books is an oddity.
    def cmd_destroy_shelf(self, cmdict):
        """ For a shelf, zombify books (mark for zeroing) and remove xattrs
            In (dict)---
                shelf
                node
            Out (dict) ---
                shelf data
        """
        self.errno = errno.EBUSY
        shelf = self.cmd_get_shelf(cmdict)
        assert not self.db.open_count(
            shelf), '%s has active opens' % shelf.name
        bos = self._list_shelf_books(shelf)
        xattrs = self.db.list_xattrs(shelf)
        for thisbos in bos:
            self.db.delete_bos(thisbos)
            _ = self._set_book_alloc(thisbos, TMBook.ALLOC_ZOMBIE)
        for xattr in xattrs:
            self.db.remove_xattr(shelf, xattr)
        return self.db.delete_shelf(shelf, commit=True)

    def cmd_kill_zombie_books(self, cmdict):
        '''repl_client command to "zero" zombie books.  Needs work.'''

        # On injured reserve for the moment.  FIXME Should be recoded as a
        # helper to gather orphan ZOMBIE books into a shelf to be
        # submitted for a node to unlink.
        return None
        node_id = cmdict['context']['node_id']
        zombies = self.db.get_book_by_node(
            node_id, TMBook.ALLOC_ZOMBIE, 9999)
        for book in zombies:
            _ = self._set_book_alloc(book, TMBook.ALLOC_FREE)
        self.db.commit()
        return None

    def cmd_resize_shelf(self, cmdict):
        """ Resize given shelf to new size in bytes.
            In (dict)---
                name
                id
                size_bytes
                zero_enabled
            Out (dict) ---
                z_shelf_name
        """
        shelf = self.cmd_get_shelf(cmdict, match_id=True)
        bos = self._list_shelf_books(shelf)
        new_size_bytes = int(cmdict['size_bytes'])
        self.errno = errno.EINVAL
        assert new_size_bytes >= 0, 'Bad size'
        new_book_count = self._nbooks(new_size_bytes)
        out_buf = {'z_shelf_name': None}
        if bos:
            seqs = [ b.seq_num for b in bos ]
            self.errno = errno.EBADFD
            assert set(seqs) == set(range(1, shelf.book_count + 1)), (
                'Corrupt BOS sequence progression for %s' % shelf.name)

        # Can I leave real early?
        if new_size_bytes == shelf.size_bytes:
            return out_buf

        # Can this call be rejected?
        openers = self.db.get_shelf_openers(
            shelf, cmdict['context'], include_me=False)
        self.errno = errno.EMFILE
        assert not openers or new_size_bytes > shelf.size_bytes, \
            'Cannot shrink multiply-opened shelf'

        # Go for it
        shelf.size_bytes = new_size_bytes

        # How about a little early?
        if new_book_count == shelf.book_count:
            shelf.matchfields = 'size_bytes'
            shelf = self.db.modify_shelf(shelf, commit=True)
            return out_buf

        books_needed = new_book_count - shelf.book_count
        if books_needed > 0:
            policy = BookPolicy(self, shelf, cmdict['context'])
            freebooks = policy(books_needed)
            self.errno = errno.ENOSPC
            assert len(freebooks) == books_needed, \
                'out of space for "%s"' % shelf.name
            seq_num = shelf.book_count
            for book in freebooks:  # Mark book in use and create BOS entry
                book = self._set_book_alloc(book, TMBook.ALLOC_INUSE)
                seq_num += 1
                thisbos = TMBos(
                    shelf_id=shelf.id, book_id=book.id, seq_num=seq_num)
                thisbos = self.db.create_bos(thisbos)
        elif books_needed < 0:
            books_2bdel = -books_needed  # it all reads so much better
            self.errno = errno.EREMOTEIO
            assert len(bos) >= books_2bdel, 'Book removal problem'
            # The unlink workflow has one step which zero truncates
            # a file with a certain name.  IOW not all shelves are USED,
            # some may be full of ZOMBIES
            freeing = shelf.name.startswith(_ZERO_PREFIX) and not new_book_count
            zero_enabled = cmdict['zero_enabled']

            if not freeing and zero_enabled:
                # Create a zeroing shelf for the books being removed
                z_shelf_name = (_ZERO_PREFIX + shelf.name + '_' +
                    str(time.time()) + '_' + cmdict['context']['physloc'])
                z_shelf_data = {}
                z_shelf_data.update({'context':cmdict['context']})
                z_shelf_data.update({'name':z_shelf_name})
                self.errno = errno.EINVAL
                z_shelf = TMShelf(z_shelf_data)
                self.db.create_shelf(z_shelf)
                z_seq_num = 1

            while books_2bdel > 0:
                try:
                    thisbos = bos.pop()
                    self.db.delete_bos(thisbos)         # Orphans the book
                    new_state = TMBook.ALLOC_ZOMBIE
                    book = self.db.get_book_by_id(thisbos.book_id)
                    if freeing and book.allocated == TMBook.ALLOC_ZOMBIE:
                        new_state = TMBook.ALLOC_FREE
                    if new_state != book.allocated:     # staying ZOMBIE
                        _ = self._set_book_alloc(book, new_state)
                    books_2bdel -= 1

                    if not freeing and zero_enabled:
                        # Add removed book to zeroing shelf
                        z_thisbos = TMBos(shelf_id=z_shelf.id, book_id=book.id, seq_num=z_seq_num)
                        z_thisbos = self.db.create_bos(z_thisbos)
                        z_shelf.size_bytes += self.book_size_bytes
                        z_shelf.book_count += 1
                        z_seq_num += 1

                except Exception as e:
                    self.db.rollback()
                    self.errno = errno.EREMOTEIO
                    raise RuntimeError('Resizing shelf smaller failed: %s' % str(e))

            if not freeing and zero_enabled:
                z_shelf.matchfields = ('size_bytes', 'book_count')
                z_shelf = self.db.modify_shelf(z_shelf, commit=True)
                out_buf = {'z_shelf_name': z_shelf_name}

        else:
            self.db.rollback()
            self.errno = errno.EREMOTEIO
            raise RuntimeError('Bad code path in cmd_resize_shelf()')

        shelf.book_count = new_book_count
        shelf.matchfields = ('size_bytes', 'book_count')
        shelf = self.db.modify_shelf(shelf, commit=True)
        return out_buf

    def cmd_get_shelf_zaddr(cmd_data, cmdict):
        """
            In (dict)---
                ?
            Out (dict) ---
                ?
        """
        raise NotImplementedError

    def cmd_get_book(self, cmdict):
        """ List a given book
            In (dict)---
                book_id
            Out (dict) ---
                book data
        """
        book_id = cmdict['id']
        book = self.db.get_book_by_id(book_id)
        return book

    def cmd_list_xattrs(self, cmdict):
        """ Retrieve names of all extendend attributes of a shelf.
            In (dict)---
                name
            Out (list) ---
                value
        """
        # While you can set and retrieve things by name that don't start with
        # user/system/security/etc, and they show up in value[], something
        # in Linux strips them out before getting back to the user.  POSIX?
        try:
            shelf = self.cmd_get_shelf(cmdict)
        except AssertionError as e:
            if cmdict['name']:
                raise
            # It's the root of the file system.  Return the default policy
            xattrs = [
                'user.LFS.AllocationPolicyDefault',
                'user.LFS.AllocationPolicyList'     # Vanna can I buy a clue?
            ]
            return { 'value': xattrs }

        value = self.db.list_xattrs(shelf)
        # AllocationPolicy is auto-added at shelf creation and can't be
        # removed.  Artificially add these "intrinsic" xattrs.
        value.append('user.LFS.AllocationPolicyList')
        value.append('user.LFS.Interleave')
        return { 'value': sorted(value) }

    def cmd_get_xattr(self, cmdict):
        """ Retrieve name/value pair for an extendend attribute of a shelf.
            In (dict)---
                name
                id
                xattr
            Out (dict) ---
                value
        """
        xattr, value = BookPolicy.xattr_assist(self, cmdict)
        if value is None:   # it was a "non-magic" xattr
            shelf = self.cmd_get_shelf(cmdict)
            value = self.db.get_xattr(shelf, xattr)
        return { 'value': value }

    def cmd_set_xattr(self, cmdict):
        """ Set/update name/value pair for an extended attribute of a shelf.
            In (dict)---
                name
                id
                xattr
                value
            Out (dict) ---
                None or raise error
        """
        # XATTR_CREATE/REPLACE option is not being set on the other side.
        xattr, value = BookPolicy.xattr_assist(self, cmdict, setting=True)
        try:
            shelf = self.cmd_get_shelf(cmdict)
        except AssertionError as e:
            if cmdict['name']:
                raise
            return None

        if self.db.get_xattr(shelf, xattr, exists_only=True):
            return self.db.modify_xattr(shelf, xattr, value)
        return self.db.create_xattr(shelf, xattr, value)

    def cmd_remove_xattr(self, cmdict):
        xattr, value = BookPolicy.xattr_assist(self, cmdict, removing=True)
        shelf = self.cmd_get_shelf(cmdict)
        return self.db.remove_xattr(shelf, xattr)

    def cmd_set_am_time(self, cmdict):
        """ Set access and modified times, usually of a shelf but
            maybe also the librarian itself.  For now we ignore atime.
            In (dict)---
                name
                atime
                mtime
            Out (list) ---
                None or error
        """
        shelf = self.cmd_get_shelf(cmdict)
        shelf.matchfields = 'mtime'  # special case
        shelf.mtime = cmdict['mtime']
        self.db.modify_shelf(shelf, commit=True)
        return None

    def cmd_send_OOB(self, cmdict):
        '''In general any command that creates an OOB condition
           needs to attach the OOB resolution for all clients.
           This API is merely for testing purposes.'''
        return {
            'value': cmdict['context']['node_id'],
            'OOBmsg': cmdict['msg']
        }

    def cmd_get_book_ig(self, cmdict):
        allocated = [ TMBook.ALLOC_FREE, TMBook.ALLOC_INUSE, TMBook.ALLOC_ZOMBIE ]
        return self.db.get_books_by_intlv_group(9999, cmdict['intlv_group'], allocated)

    def cmd_get_book_all(self, cmdict):
        return self.db.get_book_all()

    def cmd_get_book_info_all(self, cmdict):
        return self.db.get_book_info_all(cmdict['intlv_group'])

    def cmd_update_node_soc_status(self, cmdict):
        self.db.modify_node_soc_status(cmdict['context']['node_id'], cmdict['status'])

    def cmd_update_node_mc_status(self, cmdict):
        self.db.modify_node_mc_status(cmdict['context']['node_id'], cmdict['status'])

    #######################################################################

    _commands = None

    def __init__(self, backend, optargs=None, cooked=False):
        innerE = None
        self.verbose = getattr(optargs, 'verbose', 0)
        self.nozombies = getattr(optargs, 'nozombies', False)
        try:
            self.db = backend
            globals = self.db.get_globals()
            (self.__class__._book_size_bytes,
             self.__class__._nvm_bytes_total) = (
                globals.book_size_bytes,
                globals.nvm_bytes_total
            )
            self.__class__.nodes = self.db.get_nodes()
            self.__class__.IGs = self.db.get_interleave_groups()
            assert self.nodes and self.IGs, 'Database is corrupt'

            racknum = 1
            racknodes = [ n for n in self.nodes if n.rack == racknum ]
            while racknodes:
                for encnum in range(1, 9):
                    encnodes = [ n for n in racknodes if n.enc == encnum ]
                    if not encnodes:
                        continue
                    encnodes = sorted(encnodes, key=attrgetter('node'))
                    outstr = [ '%d:%d:%d' % (racknum, encnum, n.node) for
                               n in encnodes ]
                    if self.verbose:
                        print('Rack %d Enc %2d nodes:' % (racknum, encnum),
                            ' '.join(outstr))
                racknum += 1
                racknodes = [ n for n in self.nodes if n.rack == racknum ]

            # Calculations for flat-space shadow backing were being done
            # on every node after pulling down allbooks[].  Send over
            # summary data instead.  Although the math doesn't care, human
            # debugging will be easier if these are ordered.  Will need to
            # send full IGs at some point for RAS help.  This is good for now.

            self.IGs = sorted(self.IGs, key=attrgetter('groupId'))
            self.__class__.books_per_IG = dict([(ig.groupId, ig.total_books)
                                          for ig in self.IGs])

            # Skip 'cmd_' prefix
            self.__class__._commands = dict(
                [(name[4:], func)
                 for (name, func) in self.__class__.__dict__.items() if
                 name.startswith('cmd_')])
            self._cooked = cooked  # return style: raw = dict, cooked = obj
        except Exception as e:      # raising here is not clean
            innerE = '%s line %d: %s' % (
                __file__, sys.exc_info()[2].tb_lineno, str(e))
        if innerE is not None:
            raise RuntimeError('INITIALIZATION ERROR: %s' % str(innerE))

    def __call__(self, cmdict):
        '''Discern the command routine from the command name and call it.'''
        # FIXME: untrapped errors will dump the librarian, specifically the
        # error handling code.
        try:
            self.errno = 0
            context = cmdict['context']
            command = self._commands[cmdict['command']]
            self.db.modify_node_soc_status(cmdict['context']['node_id'], None)
        except KeyError as e:
            # This comment might go better in the module that imports json.
            # From StackOverflow: NULL is not zero. It's not a value, per se:
            # it is a value outside the domain of the variable's type,
            # indicating missing or unknown data.  There is only one way to
            # represent null in JSON. Per the specs (RFC 4627 and json.org):
            # 2.1.  Values.  A JSON value MUST be an object, array, number,
            # or string, OR one of the following three literal names:
            # false null true
            # Python's json handler turns None into 'null' and vice verse.
            errmsg = 'engine failed lookup on "%s"' % str(e)
            print('!' * 20, errmsg, file=sys.stderr)
            # Higher-order internal error
            return { 'errmsg': errmsg, 'errno': errno.ENOSYS }, None

        try:
            # If this is a performance problem, cache node_id
            assert FRDnode(int(cmdict['context']['node_id'])) in self.nodes, \
                'Node is not configured in Librarian topology'
            errmsg = ''  # High-level internal errors, not LFS state errors
            self.errno = 0
            ret = OOBmsg = None
            ret = command(self, cmdict)
        except (AssertionError, RuntimeError) as e:  # programmed checks
            errmsg = str(e)
        except Exception as e:  # the Unknown Idiot needs some help
            traceback.print_exception(*sys.exc_info())
            errmsg = 'INTERNAL CODING ERROR: %s' % str(e)

        if errmsg:  # Looks better _cooked
            if self.verbose > 2:
                print('%s failed: %s: %s' %
                    (cmdict['command'],
                    errno.errorcode.get(self.errno, 'EEEEEEEK!'),
                    errmsg),
                    file=sys.stderr)
            return { 'errmsg': errmsg, 'errno': self.errno }, None

        if isinstance(ret, dict):
            OOBmsg = ret.get('OOBmsg', None)
            if OOBmsg is not None:
                OOBmsg = { 'OOBmsg': OOBmsg }
                del ret['OOBmsg']
        if self._cooked:  # for self-test
            return ret, OOBmsg

        # Create a dict to which context will be added
        if type(ret) in (dict, str) or ret is None:
            value = { 'value': ret }
        elif isinstance(ret, list):
            try:
                value = { 'value': [ r.dict for r in ret ] }
            except Exception as e:
                value = { 'value': ret }
        else:
            value = { 'value': ret.dict }
        value['context'] = context  # has sequence
        return value, OOBmsg

    @property
    def commandset(self):
        return tuple(sorted(self._commands.keys()))
