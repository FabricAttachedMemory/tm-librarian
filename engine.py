#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian engine module
#---------------------------------------------------------------------------

import errno
import uuid
import time
import math
import sys
from pdb import set_trace

from book_shelf_bos import TMBook, TMShelf, TMBos
from cmdproto import LibrarianCommandProtocol
from genericobj import GenericObject

class LibrarianCommandEngine(object):

    @staticmethod
    def argparse_extend(parser):
        pass

    _book_size = 0
    _total_nvm = 0  # read from DB

    @property
    def book_size(self):
        return self._book_size

    @property
    def total_nvm(self):
        return self._total_nvm

    @classmethod
    def _nbooks(cls, nbytes):
        return int(math.ceil(float(nbytes) / float(cls._book_size)))

    def cmd_version(self):
        """ Return librarian version
            In (dict)---
                None
            Out (dict) ---
                librarian version
        """
        return self.db.get_version()

    def cmd_create_shelf(self):
        """ Create a new shelf
            In (dict)---
                name
            Out (dict) ---
                shelf data
        """
        shelf = TMShelf(self._cmdict)
        ret = self.db.create_shelf(shelf, commit=True)
        # open_count is 0 now, but a flush/release screws it up
        return ret

    def cmd_list_shelf(self, name_only=True):
        """ List a given shelf.
            In (dict)---
                name
                optional id
            Out (TMShelf object) ---
                TMShelf object
        """
        shelf = TMShelf(self._cmdict)
        if name_only:
            shelf.matchfields = ('name', )
        else:
            assert shelf.id, '%s not already open' % shelf.name
            shelf.matchfields = ('name', 'id')
        shelf = self.db.get_shelf(shelf)
        if shelf is None:
            if not name_only:
                raise AssertionError('no such shelf %s' % shelf.name)
        else:   # consistency checks
            assert self._nbooks(shelf.size_bytes) == shelf.book_count, (
                '%s size metadata mismatch' % shelf.name)
        return shelf

    def cmd_list_shelves(self):
        return self.db.get_shelf_all()

    def cmd_open_shelf(self):
        """ Open a shelf for access by a node.
            In (dict)---
                name
            Out ---
                TMShelf object
        """
        shelf = self.cmd_list_shelf()
        shelf.open_count += 1
        shelf.matchfields = 'open_count'
        self.db.modify_shelf(shelf, commit=True)
        return shelf

    def cmd_close_shelf(self):
        """ Close a shelf against access by a node.
            In (dict)---
                shelf_id
            Out (dict) ---
                TMShelf object
        """
        # todo: check if node/user really has this shelf open
        # todo: ensure open count does not go below zero

        shelf = self.cmd_list_shelf(name_only=False)
        assert shelf.open_count >= 0, '%s negative open count' % shelf.name
        # FIXME: == 0 occurs right after a create.  What's up?
        if shelf.open_count > 0:
            shelf.open_count -= 1
            shelf.matchfields = 'open_count'
            self.db.modify_shelf(shelf, commit=True)
        return shelf

    def cmd_destroy_shelf(self):
        """ Destroy a shelf and free any books associated with it.
            In (dict)---
                shelf_id
                node_id
                uid
                gid
            Out (dict) ---
                shelf data
        """
        # Do my own join, start with lookup by name
        shelf = self.cmd_list_shelf(name_only=False)
        # FIXME: make this an assertion, wedge a "force" option in somewhere
        if shelf.open_count:
            print('%s open count is %d' % (shelf.name, shelf.open_count),
                  file=sys.stderr)

        bos = self.db.get_bos_by_shelf_id(shelf.id)
        for thisbos in bos:
            self.db.delete_bos(thisbos)
            book = self.db.get_book_by_id(thisbos.book_id)
            assert book, 'Book lookup failed'
            book.allocated = TMBook.ALLOC_ZOMBIE
            book.matchfields = 'allocated'
            book = self.db.modify_book(book)
            assert book, 'Book allocation modify failed'
        return self.db.delete_shelf(shelf, commit=True)

    def _list_shelf_books(self, shelf):
        assert shelf.id, '%s is not open' % shelf.name
        bos = self.db.get_bos_by_shelf_id(shelf.id)

        # consistency checks
        assert len(bos) == shelf.book_count, (
            '%s book count mismatch' % shelf.name)
        assert self._nbooks(shelf.size_bytes) == shelf.book_count, (
            '%s size metadata mismatch' % shelf.name)
        return bos

    def cmd_resize_shelf(self):
        """ Resize given shelf to new size in bytes.
            In (dict)---
                name
                id
                size_bytes
            Out (dict) ---
                shelf data
        """
        shelf = self.cmd_list_shelf(name_only=False)

        bos = self._list_shelf_books(shelf)
        assert len(bos) == shelf.book_count, (
            '%s book count mismatch' % shelf.name)

        # other consistency checks
        assert self._nbooks(shelf.size_bytes) == shelf.book_count, (
            '%s size metadata mismatch' % shelf.name)

        new_size_bytes = int(self._cmdict['size_bytes'])
        assert new_size_bytes >= 0, 'Bad size'
        new_book_count = self._nbooks(new_size_bytes)
        if bos:
            seqs = [ b.seq_num for b in bos ]
            assert set(seqs) == set(range(1, shelf.book_count + 1)), (
                'Corrupt BOS sequence progression for %s' % shelf.name)

        # Can I leave real early?
        if new_size_bytes == shelf.size_bytes:
            return shelf
        shelf.size_bytes = new_size_bytes

        # How about a little early?
        if new_book_count == shelf.book_count:
            shelf.matchfields = 'size_bytes'
            shelf = self.db.modify_shelf(shelf, commit=True)
            return shelf

        books_needed = new_book_count - shelf.book_count
        node_id = self._cmdict['context']['node_id']
        if books_needed > 0:
            seq_num = shelf.book_count
            freebooks = self.db.get_book_by_node( node_id, 0, books_needed)
            assert len(freebooks) == books_needed, (
                'ENOSPC on node %d for "%s"' % (node_id, shelf.name))
            for book in freebooks: # Mark book in use and create BOS entry
                book.allocated = TMBook.ALLOC_INUSE
                book.matchfields = 'allocated'
                book = self.db.modify_book(book)
                seq_num += 1
                thisbos = TMBos(
                    shelf_id=shelf.id, book_id=book.id, seq_num=seq_num)
                thisbos = self.db.create_bos(thisbos)
        elif books_needed < 0:
            books_2bdel = -books_needed    # it all reads so much better
            assert len(bos) >= books_2bdel, 'Book removal problem'
            while books_2bdel > 0:
                thisbos = bos.pop()
                self.db.delete_bos(thisbos)
                book = self.db.get_book_by_id(thisbos.book_id)
                book.allocated = TMBook.ALLOC_ZOMBIE
                book.matchfields = ('allocated')
                self.db.modify_book(book)
                books_2bdel -= 1
        else:
            self.db.rollback()
            raise RuntimeError('Bad code path in shelf_resize()')

        shelf.book_count = new_book_count
        shelf.matchfields = ('size_bytes', 'book_count')
        shelf = self.db.modify_shelf(shelf, commit=True)
        return shelf


    def cmd_get_shelf_zaddr(cmd_data):
        """
            In (dict)---
                ?
            Out (dict) ---
                ?
        """
        return '{"error":"Command not implemented"}'

    def cmd_list_book(cmd_data):
        """ List a given book
            In (dict)---
                book_id
            Out (dict) ---
                book data
        """
        set_trace()
        book_id = cmd_data["book_id"]
        resp = db.get_book_by_id(book_id)
        # todo: fail if book does not exist
        recvd = dict(zip(book_columns, resp))
        return recvd

    def cmd_list_bos(self):
        """ List all the books on a given shelf.
            In (dict)---
                shelf_id
            Out (dict) ---
                bos data
        """
        shelf_id = self._cmdict['shelf_id']
        bos = db.get_bos_by_shelf_id(shelf_id)

    def cmd_get_xattr(self):
        """ Retrieve name/value pair for an extendend attribute of a shelf.
            In (dict)---
                name
                id
                xattr
            Out (dict) ---
                value
        """
        shelf = self.cmd_list_shelf()
        if shelf is None:
            return None
        return self.db.get_xattr(shelf, self._cmdict['xattr'])

    _handlers = { }

    def __init__(self, backend, optargs=None, cooked=False):
        try:
            self.db = backend
            (self.__class__._book_size,
             self.__class__._total_nvm) = self.db.get_nvm_parameters()
            assert self._book_size >= 1024*1024, 'Bad book size in DB'
            assert self._total_nvm >= 16 * self._book_size, (
                'Less than 16 books in NVM pool')

            # Skip 'cmd_' prefix
            tmp = dict( [ (name[4:], func)
                        for (name, func) in self.__class__.__dict__.items() if
                            name.startswith('cmd_')
                        ]
            )
            self._handlers.update(tmp)
            self._cooked = cooked   # return style: raw = dict, cooked = obj
        except Exception as e:
            raise RuntimeError('FATAL INITIALIZATION ERROR: %s' % str(e))

    def __call__(self, cmdict):
        errmsg = ''
        try:
            self._cmdict = cmdict
            handler = self._handlers[self._cmdict['command']]
        except KeyError as e:
            # This comment might go better in the module that imports json.
            # From StackOverflow: NULL is not zero. It is not a value, per se:
            # it is a value outside the domain of the variable's type,
            # indicating missing or unknown data.  There is only one way to
            # represent null in JSON. Per the specs (RFC 4627 and json.org):
            # 2.1.  Values.  A JSON value MUST be an object, array, number,
            # or string, OR one of the following three literal names:
            # false null true
            # Python's json handler turns None into 'null' and vice verse.
            errmsg = 'Bad lookup on "%s"' % str(e)
            return None

        try:
            errmsg = ''
            assert not errmsg, errmsg
            ret = handler(self)
        except AssertionError as e:     # consistency checks
            errmsg = str(e)
        except (AttributeError, RuntimeError) as e: # idiot checks
            errmsg = 'INTERNAL ERROR @ %s[%d]: %s' % (
                self.__class__.__name__, sys.exc_info()[2].tb_lineno,str(e))
        except Exception as e:          # the Unknown Idiot
            errmsg = 'UNEXPECTED ERROR @ %s[%d]: %s' % (
                self.__class__.__name__, sys.exc_info()[2].tb_lineno,str(e))
        finally:    # whether it worked or not
            if errmsg:
                ret = GenericObject(error=errmsg)

            if self._cooked:    # for self-test
                return ret

            # for the net
            if ret is None:
                return None # see comment elsewhere about JSON(None)
            if isinstance(ret, list):
                return [ r.dict for r in ret ] # generator didn't work?
            if isinstance(ret, dict):
                return ret
            return ret.dict

    @property
    def commandset(self):
        return tuple(sorted(self._handlers.keys()))

###########################################################################
# Use LCP to construct command dictionaries from fixed data.  Those
# dictionaries are what would be "received" from real clients.
# Exercises are written against an SQLite3 database so create it
# beforehand with book_register.py.

if __name__ == '__main__':


    import os
    from argparse import Namespace # the result of an argparse sequence.
    from pprint import pprint

    from backend_sqlite3 import LibrarianDBackendSQLite3

    def pp(recvd, data):
        print('Command:', dict(recvd))
        print('DB action results:')
        pprint(data)
        print()

    umask = os.umask(0) # The Pythonic way to get the current umask.
    os.umask(umask)     # FIXME: move this into...somewhere?
    context = {
        'uid': os.geteuid(),
        'gid': os.getegid(),
        'pid': os.getpid(),
        'umask': umask,
        'node_id': 1,
    }

    lcp = LibrarianCommandProtocol(context)
    print(lcp.commandset)

    # For self test, look at prettier results than dictionaries.
    args = Namespace(db_file=sys.argv[1])
    lce = LibrarianCommandEngine(
                    LibrarianDBackendSQLite3(args),
                    cooked=True)
    print(lce.commandset)

    print()
    print('Engine missing:',set(lcp.commandset) - set(lce.commandset))
    print('Engine extras: ',set(lce.commandset) - set(lcp.commandset))

    recvd = lcp('version')
    version = lce(recvd)
    pp(recvd, version)

    for name in ('xyzzy', 'shelf22', 'coke', 'pepsi'):
        recvd = lcp('create_shelf', name=name)
        try:
            shelf = lce(recvd)   # only works on fresh DB
        except Exception as e:
            shelf = str(e)
            if shelf.startswith('INTERNAL ERROR'):
                set_trace()
                raise e
        pp(recvd, shelf)

    # Two ways to get started
    name = 'xyzzy'
    recvd = lcp('list_shelf', name=name)
    shelf = lce(recvd)
    pp(recvd, shelf)

    recvd = lcp('list_shelf', shelf)    # a shelf object with 'name'
    shelf = lce(recvd)
    pp(recvd, shelf)

    recvd = lcp('list_shelves')
    shelves = lce(recvd)
    assert len(shelves) >= 4, 'not good'
    pp(recvd, shelf)

    # Some FS operations (in FuSE) first require an open shelf.  The
    # determination of that state is simple: does it have a shelf id?
    recvd = lcp('open_shelf', shelf)
    shelf = lce(recvd)
    pp(recvd, shelf)
    if shelf is None:
        raise SystemExit('Shelf ' + name + ' has disappeared (open)')

    shelf.size_bytes = (70 * lce.book_size)
    recvd = lcp('resize_shelf', shelf)
    shelf = lce(recvd)
    pp(recvd, shelf)
    if shelf is None or hasattr(shelf, 'error'):
        raise SystemExit('Shelf ' + name + ' problems (resize down)')

    shelf.size_bytes = (50 * lce.book_size)
    recvd = lcp('resize_shelf', shelf)
    shelf = lce(recvd)
    pp(recvd, shelf)
    if shelf is None or hasattr(shelf, 'error'):
        raise SystemExit('Shelf ' + name + ' problems (resize down)')

    recvd = lcp('close_shelf', shelf)
    shelf = lce(recvd)
    pp(recvd, shelf)
    if shelf is None or hasattr(shelf, 'error'):
        raise SystemExit('Shelf ' + name + ' problems (resize down)')

    # destroy shelf is just based on the name
    recvd = lcp('destroy_shelf', shelf)
    shelf = lce(recvd)
    pp(recvd, shelf)
    if shelf is None or hasattr(shelf, 'error'):
        raise SystemExit('Shelf ' + name + ' problems (resize down)')

    raise SystemExit(0)
