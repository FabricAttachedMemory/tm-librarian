#!/usr/bin/python3 -tt

# Copyright 2017 Hewlett Packard Enterprise Development LP

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2 as
# published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along
# with this program.  If not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

#---------------------------------------------------------------------------
# This is really an interface implementation of generic Librarian methods.
# We should write an abstract base class to inherit from.   The implmentation
# is crafted toward SQL backends.  As such the schema, while truly defined
# in book_register.py, is heavily reflected here, hence the SCHEMA_VERSION.
#
# You can't instantiate this class but you'll see most methods depend on
# self._cur, an extended cursor object.  That object is declared in another
# "pure SQL" (Librarian-agnostic) class.
#
# In a larger application, sublcass this module, along with one of those
# "pure SQL" objects such as SQLite3Assist.   Then set self._cur to that
# cursor object.  In essence, an instance of this "superclass" shows a
# "HAS-A" relationship via self._cur.
#---------------------------------------------------------------------------

import stat
import time

from pdb import set_trace

from book_shelf_bos import TMBook, TMShelf, TMBos, TMOpenedShelves

from frdnode import FRDnode, FRDFAModule, FRDintlv_group

#--------------------------------------------------------------------------


class LibrarianDBackendSQL(object):

    SCHEMA_VERSION = 'LIBRARIAN 0.997'
    # 0.995     Added heartbeat to SOC
    # 0.996     Added CPU and root FS percent to SOC; add link table
    # 0.997     Added network_in, network_out and mem_percent to SOC

    @staticmethod
    def argparse_extend(parser):
        ''' From a larger main application, pass in an argparse argument to
            extend the argument list.
            Input---
              parser - initialized return from argarse.xxxxx
            Output---
              None
        '''
        pass

    def __init__(self, args):
        ''' Override this to supply a self._cur (HAS-A relationship) with
            a target DB backend).   A target database must first be
            initialized via appropriate tools.
            Input---
              args - usually a Namespace argument from argparse with
                     info to set up the cursor to the database.
            Output---
              None, it's __init__
        '''
        raise NotImplementedError('Write a child class to override.')

    # Broken out in case we switch to a UUID or something else.
    def _getnextid(self, table):
        return self._cur.getnextid(table)

    #
    # DB globals
    #

    def get_globals(self, only=None):
        ''' Retrieve global information from the DB.
            Input---
              only - a specific field, only "version" is supported
            Output---
              an object with schema_version, book_size, total books, total
              nodes, and total FAM bytes.
        '''
        try:
            if only == 'version':
                self._cur.execute('SELECT schema_version FROM globals LIMIT 1')
                tmp = self._cur.fetchone()[0]
                assert tmp == self.SCHEMA_VERSION, 'DB schema mismatch'
                return tmp
            self._cur.execute('SELECT * FROM globals LIMIT 1')
            # rowcount not valid after select in SQLite3
            self._cur.iterclass = 'default'
            r = iter(self._cur).__next__()
            assert r.schema_version == self.SCHEMA_VERSION, 'DB schema mismatch'
        except StopIteration:
            raise RuntimeError(
                '%s is corrupt (missing "globals")' %
                self._cur.db_file)
        except Exception as e:
            raise RuntimeError(str(e))

        self._cur.execute('SELECT COUNT() FROM books WHERE allocated > 0')
        r.books_used = self._cur.fetchone()[0]
        if r.books_used is None:    # empty DB
            r.books_used = 0
        return r

    def get_nodes(self):
        ''' Retrieve info about all nodes configured into the DB.
            Input---
              None
            Output---
              a list of objects describing each node.
        '''
        self._cur.execute('SELECT * FROM FRDnodes')
        self._cur.iterclass = 'default'
        # Module_size_books was only used during book_register.py
        nodes = [ FRDnode(node=r.node,
                          enc=r.enc,
                          module_size_books=-1)
                  for r in self._cur ]
        return nodes

    def get_interleave_groups(self):
        ''' Retrieve info about all interleave groups configured into the DB.
            Input---
              None
            Output---
              a list of objects describing each interleave group.
        '''
        self._cur.execute('SELECT * FROM FAModules')
        self._cur.iterclass = 'default'
        tmpIGs = { }
        # First collect all the MCs by IG as they may be scattered in DB.
        # Loops could be rolled up for purity but this is clearer.
        for r in self._cur:
            val = FRDFAModule(STRorCID=r.rawCID,
                              module_size_books=r.module_size_books)
            try:
                tmpIGs[r.IG].append(val)
            except KeyError as e:
                tmpIGs[r.IG] = [ val, ]
        IGs = [ FRDintlv_group(IG, MCs) for IG, MCs in tmpIGs.items() ]
        return IGs

    def get_nvm_parameters(self):
        ''' Returns tuple(book_size, total_nvm)
        '''
        self._cur.execute(
            'SELECT book_size_bytes, nvm_bytes_total FROM globals')
        return self._cur.fetchone()

    #
    # DB modifies.  Grouped like this to see similarites.
    #

    @staticmethod
    def _fields2qmarks(fields, joiner):
        q = [ '%s=?' % f for f in fields ]
        return joiner.join(q)

    # matchfields provide the core of the SET clause and were set by
    # the importer.  Localmods fields were done in the direct caller
    # of this routine.  Those two field sets must be disjoint.

    def _modify_table(self, table, obj, localmods, commit):
        assert hasattr(obj, 'id'), 'object ineligible for this routine'
        objid = ('id', )

        # objid will be the WHERE clause and thus must come last
        if obj.matchfields:
            cantmatch = localmods + objid
            tmp = set(obj.matchfields).intersection(set(cantmatch))
            assert not tmp, 'Bad matchfields %s' % str(tmp)

        fields = obj.matchfields + localmods
        qmarks = self._fields2qmarks(fields, ', ')
        setwhere = '%s WHERE id=?' % qmarks
        self._cur.UPDATE(table, setwhere, obj.tuple(fields + objid))
        if commit:
            self._cur.commit()
        return obj

    def modify_book(self, book, commit=False):
        """ Modify book data in "books" table.
            Input---
              book_data - list of new book data
            Output---
              book_data or error message
        """
        return self._modify_table('books', book, (), commit)

    def modify_shelf(self, shelf, commit=False):
        """ Modify data for an individual shelf.
            Input---
              shelf - object containing fixed shelf ID info plus updated
                      fields listed in the "match_fields" attribute.
              commit - persist the transaction now
            Output---
              the shelf (possible with mtime update) or raise error
        """
        now = int(time.time())
        if shelf.matchfields == ('mtime', ):    # called from set_am_time
            shelf.matchfields = ()              # only modify mtime...
            if not shelf.mtime:                 # ...if explicitly set...
                shelf.mtime = now               # ...else now is fine
        else:
            shelf.mtime = now                   # Normal operation
        return self._modify_table('shelves', shelf, ('mtime', ), commit)

    def modify_opened_shelves(self, shelf, action, context):
        ''' Deprecated?
        '''
        if action == 'get':
            shelf.open_handle = self._cur.INSERT(
                'opened_shelves',
                (None, shelf.id, context['node_id'], context['pid']))
        elif action == 'put':
            self._cur.DELETE('opened_shelves',
                             'id=? AND shelf_id=? AND node_id=?',
                             (shelf.open_handle,
                              shelf.id,
                              context['node_id']),
                             commit=True)
            shelf.open_handle = None
        else:
            raise RuntimeError(
                'Bad action %s for modify_open_shelves' % action)
        return shelf

    def open_count(self, shelf):
        ''' Return number of opens against the specified shelf
        '''
        self._cur.execute('''SELECT COUNT(*) FROM opened_shelves
                             WHERE shelf_id=?''', (shelf.id,))
        return self._cur.fetchone()[0]

    def modify_xattr(self, shelf, xattr, value, commit=True):
        """Modify data in corresponding to extended attributes for a shelf.
           Input---
              shelf: mainly for the shelf id
              xattr: existing key
              value: (new) value
              commit: persist the update now
           Output---
              None or error exception
        """
        self._cur.UPDATE(
            'shelf_xattrs',
            'value=? WHERE shelf_id=? AND xattr=?',
            (value, shelf.id, xattr))
        if commit:
            self._cur.commit()
        shelf.matchfields = ()    # time only
        self.modify_shelf(shelf, commit=commit)

    def modify_node_soc_status(self, node_id,
            status=None, cpu_percent=-44, rootfs_percent=-45, network_in=-46,
                   network_out=-47, mem_percent=-48):
        ''' Update the current heartbeat status for the SoC
        '''
        if status is None:      # Just advance the last-known-contact time
            self._cur.UPDATE('SOCs', 'heartbeat=?WHERE node_id=?',
                (int(time.time()), node_id))
        else:
            self._cur.UPDATE(
                'SOCs',
                'status=?, heartbeat=?, cpu_percent=?, rootfs_percent=?, network_in=?, network_out=?, mem_percent=?  WHERE node_id=?',
                (status, int(time.time()), cpu_percent, rootfs_percent,
                    network_in, network_out, mem_percent, node_id))
        self._cur.commit()

    def modify_node_mc_status(self, node_id, status):
        ''' Update the current status for a media controller
        '''
        self._cur.execute(
            'SELECT * FROM FAModules WHERE node_id=?', (node_id,))
        self._cur.iterclass = 'default'
        MCs = [ r for r in self._cur ]
        for m in MCs:
            self._cur.UPDATE(
                'FAModules',
                'status=? WHERE rawCID=?',
                (status, m.rawCID))
        self._cur.commit()

    #
    # DB books
    #

    def get_book_by_id(self, book_id):
        """ Retrieve one book by its book_id, aka the LZA.
            Input---
              book_id - id of book to get
            Output---
              TMBook object, None (no match) or raise error
        """
        self._cur.execute('SELECT * FROM books WHERE id=?', (book_id,))
        self._cur.iterclass = TMBook
        books = [ r for r in self._cur ]
        assert len(books) <= 1, 'Matched more than one book'
        return books[0] if books else None

    def get_books_by_intlv_group(self, max_books, IGs,
                                 allocated=None,
                                 exclude=False,
                                 ascending=True):
        """ Retrieve available book(s) from given interleave group.
            Input---
              max_books - maximum number of books
              IGs - list of interleave groups to filter: IN (....)
              allocated - list of filter(s) for "allocated" field
                          None=FREE
                          'ANY'=any
              exclude - treat IGs as exclusion filter: NOT IN (....)
              ascending - order by book_id == LZA
            Output---
              List of TMBooks up to max_books or raised error
        """
        if allocated is None:
            allocated = [TMBook.ALLOC_FREE]
        elif allocated == 'ANY':
            allocated = range(6)    # get them all and then some
        if IGs:
            INclause = 'AND intlv_group %%s IN (%s)' % ','.join(
                (str(i) for i in IGs))
            INclause = INclause % ('NOT' if exclude else '')
        else:
            INclause = ''
        ALLOCclause = 'WHERE allocated IN (%s)' % ','.join(
            (str(i) for i in allocated))
        order = 'ASC' if ascending else 'DESC'
        # SQL injection yeah yeah yeah can't avoid these
        sql = '''SELECT * from books
                 %s
                 %s
                 ORDER BY id %s
                 LIMIT ?''' % (ALLOCclause, INclause, order)
        self._cur.execute(sql, (max_books))
        self._cur.iterclass = TMBook
        book_data = [ r for r in self._cur ]
        return book_data

    def get_books_on_shelf(self, shelf):
        """ Retrieve all books on a shelf.
            Input---
              shelf
            Output---
              book data or None
        """
        self._cur.execute('''
            SELECT id, intlv_group, book_num, allocated, attributes
            FROM books JOIN books_on_shelves ON books.id = book_id
            WHERE shelf_id = ? ORDER BY seq_num''',
            shelf.id)
        self._cur.iterclass = TMBook
        books = [ r for r in self._cur ]
        return books

    def get_book_all(self):
        """ Retrieve book-level info about all books in all interleave groups.
            Input---
              None
            Output---
              list of TMBook objects or raise error
        """
        self._cur.execute('SELECT * FROM books ORDER BY id')
        self._cur.iterclass = TMBook
        books = [ r for r in self._cur ]
        return books

    def get_book_info_all(self, intlv_group):
        """ Retrieve maximum info on all books in an interleave group,
            including shelf ownership if applicable.
            Input---
              intlv_group
            Output---
              list of objects of book data (could be emtpy) or raise error
        """
        # Retrieve books from a given interleave group from "books"
        # table joined with "books_on_shelves" and "shelves" tables.
        db_query = """SELECT books.id,
                             books.allocated,
                             books.attributes,
                             books.intlv_group,
                             books_on_shelves.shelf_id,
                             books_on_shelves.seq_num,
                             shelves.creator_id,
                             shelves.size_bytes,
                             shelves.book_count,
                             shelves.ctime,
                             shelves.mtime,
                             shelves.name
                             FROM books
                             LEFT OUTER JOIN books_on_shelves
                             ON books.id = books_on_shelves.book_id
                             LEFT OUTER JOIN shelves
                             ON books_on_shelves.shelf_id = shelves.id
                             WHERE books.intlv_group = ?
            """
        self._cur.execute(db_query, (intlv_group))
        self._cur.iterclass = 'default'
        book_data = [ r for r in self._cur ]
        return(book_data)

    #
    # Shelves.  Since they're are indexed on 'name', dupes fail nicely.
    # Always commit, it's where the shelf.id comes from.
    #

    def create_shelf(self, shelf):
        """ Create one new shelf in the database.
            Input---
              shelf_data - list of shelf data to insert
            Output---
              shelf_data or error message
        """
        shelf.id = None     # DB engine will autochoose next id
        if not shelf.mode:
            shelf.mode = stat.S_IFREG + 0o666
        tmp = int(time.time())
        shelf.ctime = shelf.mtime = tmp
        if shelf.parent_id == 0:
            shelf.parent_id = 2  # is now a default for if it given to go in root
        shelf.id = self._cur.INSERT('shelves', shelf.tuple())
        return shelf

    def create_symlink(self, shelf, target):
        """ Creates a link entry to hold symlink path in link table
            Input---
                shelf - contains shelf data to insert
                target - target path to store that link points to
            Output---
                target or error message
        """
        self._cur.INSERT('links', (shelf.id, target, None))
        return target

    def get_symlink_target(self, shelf):
        """ Fetches symlink path from links table
            Input---
                shelf - contains shelf id of symlink file
            Output---
                target path
        """
        self._cur.execute(
            'SELECT target FROM links WHERE shelf_id=?', shelf.id)
        tmp = self._cur.fetchone()
        return tmp[0]

    def get_shelf(self, shelf):
        """ Retrieve one shelf from the database
            Input---
              shelf - TMShelf object with minimum info to key a lookup.
                      Key fields must be set in "matchfields" attribute.
            Output---
              shelf object with details or RAISED error message
        """
        fields = shelf.matchfields
        qmarks = self._fields2qmarks(fields, ' AND ')
        sql = 'SELECT * FROM shelves WHERE %s' % qmarks
        self._cur.execute(sql, shelf.tuple(fields))
        self._cur.iterclass = TMShelf
        shelves = [ r for r in self._cur ]
        assert len(shelves) <= 1, 'Matched more than one shelf'
        if not shelves:
            return None
        shelf = shelves[0]
        # FIXME: flesh out opened_shelves (dict of lists)->shelf.open_handle.
        # It's okay now for single node, but multinode opens may need it
        # and may take surgery on the socket data return values.
        return shelf

    def get_shelf_openers(self, shelf, context, include_me=False):
        """ Retrieve a list of actors holding a shelf open.
            Input---
              shelf - shelf object with minimum info to key a lookup
              context - contains (node_id, pid) of requestor
              except_me - filter requestor out of results
            Output---
              List of (NodeID, PID) tuples holding the shelf open.
        """
        self._cur.execute('''SELECT node_id, pid
                             FROM opened_shelves
                             WHERE shelf_id = ?''', shelf.id)
        self._cur.iterclass = None
        tmp = self._cur.fetchall()
        if include_me:
            tmp = [ (node_id, pid) for node_id, pid in tmp ]
        else:
            tmp = [ (node_id, pid) for node_id, pid in tmp if
                    node_id != context['node_id'] or pid != context['pid'] ]
        return tmp

    def get_directory_shelves(self, parent_shelf):
        """ Retrieve all shelves from a single parent directory
            Input---
              parent_shelf (parent directory)
            Output---
              List of TMShelf objects (could be empty) or raise error
        """
        parent_id = parent_shelf.id
        self._cur.execute(
            'SELECT * FROM shelves WHERE parent_id = %d ORDER BY id' % parent_id)
        self._cur.iterclass = TMShelf
        shelves = [ r for r in self._cur ]
        return shelves

    def get_shelf_all(self):
        """ Retrieve all shelves from the database.
            Input---
              None
            Output---
              List of TMShelf objects (could be empty) or raise error
        """
        self._cur.execute('SELECT * FROM shelves ORDER BY id')
        self._cur.iterclass = TMShelf
        shelves = [ r for r in self._cur ]
        return shelves

    def get_open_shelf_all(self):
        """ Retrieve all open shelves.
            Input---
              None
            Output---
              List of TMShelf objects (could be empty) or raise error
        """
        self._cur.execute('SELECT * FROM opened_shelves ORDER BY id')
        self._cur.iterclass = TMOpenedShelves
        shelves = [ r for r in self._cur ]
        return shelves

    def delete_shelf(self, shelf, commit=False):
        """ Delete one shelf from the database.  Any books should be
            release before this.
            Input---
              shelf - TMShelf object with name, id of target shelf
              commit - persist the update now
            Output---
              TMShelf object or raise error
        """
        where = self._fields2qmarks(shelf.schema, ' AND ')
        self._cur.DELETE('shelves', where, shelf.tuple())
        if commit:
            self._cur.commit()
        return(shelf)

    #
    # books_on_shelves.  Gets are very specific compared to books and shelves.
    #

    def create_bos(self, bos, commit=False):
        """ Insert one new book-on-shelf mapping into the database.
            Input---
              bos_data - list of bos data to insert
            Output---
              status: success = bos
                      failure = raise XXXXX
        """
        self._cur.INSERT('books_on_shelves', bos.tuple(), commit=commit)
        return(bos)

    def get_bos_by_shelf_id(self, shelf_id):
        """ Retrieve all bos entries from "books_on_shelves" table
            given a shelf_id.
            Input---
              shelf_id - shelf identifier
            Output---
              bos_data or error message
        """
        # FIXME: is it faster to let SQL sort or do it here?

        self._cur.execute('''SELECT * FROM books_on_shelves
                             WHERE shelf_id=? ORDER BY seq_num''',
                          shelf_id)
        self._cur.iterclass = TMBos
        bos = [ r for r in self._cur ]
        return bos

    def get_bos_by_book_id(self, book_id):
        """ Retrieve THE bos entries given a book_id.
            Input---
              book_id - book identifier
            Output---
              bos_data or error message
        """
        self._cur.execute('''SELECT * FROM books_on_shelves
                             WHERE book_id=? ORDER BY seq_num''',
                          book_id)
        self._cur.iterclass = TMBos
        bos = [ r for r in self._cur ]
        assert len(bos) < 2, 'Book belongs to more than one shelf'
        return bos

    def get_bos_all(self):
        """ Retrieve all bos from database.
            Input---
              None
            Output---
              bos_data or error message
        """
        self._cur.execute('SELECT * FROM books_on_shelves')
        self._cur.iterclass = TMBos
        return [ r for r in self._cur ]

    def delete_bos(self, bos, commit=False):
        """ Delete one bos mapping from the database.  This needs to be
            repeated for every bos before the shelf can be deleted.
            Input---
              bos_data - list of bos data
            Output---
              bos_data or error message
        """
        where = self._fields2qmarks(bos.schema, ' AND ')
        self._cur.DELETE('books_on_shelves', where, bos.tuple())
        if commit:
            self._cur.commit()
        return(bos)

    def get_xattr(self, shelf, xattr, exists_only=False):
        '''Retrieve the data for a the specified extended attribute for the
           given shelf.
        '''
        if exists_only:
            self._cur.execute(
                'SELECT COUNT() FROM shelf_xattrs '
                'WHERE shelf_id=? AND xattr=?',
                (shelf.id, xattr))
            return self._cur.fetchone()[0] == 1
        # Get the whole thing
        self._cur.execute(
            'SELECT value FROM shelf_xattrs WHERE shelf_id=? AND xattr=?',
            (shelf.id, xattr))
        tmp = self._cur.fetchone()
        return tmp if tmp is None else tmp[0]

    def list_xattrs(self, shelf):
        '''Retrieve the list of all extended attribute names for a shelf.
        '''
        self._cur.execute(
            'SELECT xattr FROM shelf_xattrs WHERE shelf_id=?', (shelf.id,))
        tmp = [ f[0] for f in self._cur.fetchall() ]
        return tmp

    def create_xattr(self, shelf, xattr, value):
        '''Store a new extended attribute name and value for a shelf.
        '''
        self._cur.INSERT('shelf_xattrs', (shelf.id, xattr, value))

    def remove_xattr(self, shelf, xattr):
        '''Retrieve an extended attribute name and value for a shelf.
        '''
        self._cur.DELETE('shelf_xattrs', 'shelf_id=? AND xattr=?',
                                         (shelf.id, xattr),
                                         commit=True)

    # Basic operations (SELECT, custom DELETEs) need the cursor but
    # using it directly feels "clunky".  Hide it behind facades for
    # anything not matching the above methods.  Iteration requires
    # special handling.   Everything else is delegated to __getattr__.

    @property
    def iterclass(self):
        return self._cur.iterclass

    @iterclass.setter
    def iterclass(self, value):
        self._cur.iterclass = value

    def __iter__(self):
        return self._cur

    def __getattr__(self, name):
        return getattr(self._cur, name)
