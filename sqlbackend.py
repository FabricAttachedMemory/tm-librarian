#!/usr/bin/python3 -tt

#---------------------------------------------------------------------------
# Librarian database interface module for SQL backends.  Originally
# written against SQLite but it's so generic it's probably okay
# for MariaDB.  I'm not sure about Postgres but it could be close.
#---------------------------------------------------------------------------

import time

from pdb import set_trace

from book_shelf_bos import TMBook, TMShelf, TMBos, TMOpenedShelves

#--------------------------------------------------------------------------


class LibrarianDBackendSQL(object):

    @staticmethod
    def argparse_extend(parser):
        pass

    def __init__(self, args):
        raise NotImplementedError

    # Broken out in case we switch to a UUID or something else.
    def _getnextid(self, table):
        return self._cur.getnextid(table)

    #
    # DB globals
    #

    def get_globals(self, only=None):
        if only == 'version':
            self._cur.execute('SELECT schema_version FROM globals LIMIT 1')
            return self._cur.fetchone()[0]
        self._cur.execute('SELECT * FROM globals LIMIT 1')
        self._cur.iterclass = 'default'
        for r in self._cur:
            pass
        self._cur.execute('SELECT COUNT() FROM books WHERE allocated > 0')
        r.books_used = self._cur.fetchone()[0]
        if r.books_used is None:    # empty DB
            r.books_used = 0
        return r

    def get_nvm_parameters(self):
        '''Returns duple(book_size, total_nvm)'''
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
        """ Modify shelf data in "shelves" table.  Usually, modify
            the mtime with any other attributes.
            Input---
              shelf_data - list of new shelf data
            Output---
              shelf_data or error message
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
        self._cur.execute('''SELECT COUNT(*) FROM opened_shelves
                             WHERE shelf_id=?''', (shelf.id,))
        return self._cur.fetchone()[0]

    def modify_xattr(self, shelf, xattr, value, commit=True):
        """ Modify data in "shelf_xattrs" table.  Known to exist:
            Input---
              shelf: mainly for the shelf id
              xattr: existing key
              value: new value
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

    #
    # DB books
    #

    def get_book_by_id(self, book_id):
        """ Retrieve one book from "books" table.
            Input---
              book_id - id of book to get
            Output---
              book_data or error message
        """
        self._cur.execute('SELECT * FROM books WHERE id=?', (book_id,))
        self._cur.iterclass = TMBook
        books = [ r for r in self._cur ]
        assert len(books) <= 1, 'Matched more than one book'
        return books[0] if books else None

    def get_book_by_node(self, node_id, allocated_value, num_books):
        """ Retrieve book(s) from "books" table using node
            Input---
              node_id - id of node to filter on
              status - status of book to filter on
              num_rows - max number of rows to return
            Output---
              book_data or error message
        """
        if allocated_value == -1:   # special case for demo/status
            self._cur.execute('''SELECT allocated FROM books
                                 WHERE node_id=? ORDER BY id''',
                              node_id)
            self._cur.iterclass = 'default'
            return [ r[0] for r in self._cur.fetchall() ]

        db_query = """
                SELECT * FROM books
                WHERE node_id = ? AND allocated = ?
                LIMIT ?
            """
        self._cur.execute(db_query, (node_id, allocated_value, num_books))
        self._cur.iterclass = TMBook
        book_data = [ r for r in self._cur ]
        return(book_data)

    def get_book_all(self):
        """ Retrieve all books from "books" table.
            Input---
              None
            Output---
              book_data or error message
        """
        self._cur.execute('SELECT * FROM books ORDER BY book_id')
        self._cur.iterclass = TMBook
        return [ r for r in self._cur ]

    #
    # Shelves.  Since they're are indexed on 'name', dupes fail nicely.
    # Always commit, it's where the shelf.id comes from.
    #

    def create_shelf(self, shelf):
        """ Insert one new shelf into "shelves" table.
            Input---
              shelf_data - list of shelf data to insert
            Output---
              shelf_data or error message
        """
        shelf.id = None     # DB engine will autochoose next id
        tmp = int(time.time())
        shelf.ctime = shelf.mtime = tmp
        shelf.id = self._cur.INSERT('shelves', shelf.tuple())
        return shelf

    def get_shelf(self, shelf):
        """ Retrieve one shelf from "shelves" table.
            Input---
              shelf_id - id of shelf to get
            Output---
              shelf_id or error message
        """
        fields = shelf.matchfields
        qmarks = self._fields2qmarks(fields, ' AND ')
        sql = 'SELECT * FROM shelves WHERE %s' % qmarks
        self._cur.execute(sql, shelf.tuple(fields))
        self._cur.iterclass = TMShelf
        shelves = [ r for r in self._cur ]  # FIXME: make an r.getone()?
        assert len(shelves) <= 1, 'Matched more than one shelf'
        return shelves[0] if shelves else None

    def get_shelf_all(self):
        """ Retrieve all shelves from "shelves" table.
            Input---
              None
            Output---
              shelf_data or error message
        """
        self._cur.execute('SELECT * FROM shelves ORDER BY id')
        self._cur.iterclass = TMShelf
        shelves = [ r for r in self._cur ]
        return shelves

    def get_open_shelf_all(self):
        """ Retrieve all open shelves from "opened_shelves" table.
            Input---
              None
            Output---
              shelf_data or error message
        """
        self._cur.execute('SELECT * FROM opened_shelves ORDER BY id')
        self._cur.iterclass = TMOpenedShelves
        shelves = [ r for r in self._cur ]
        return shelves

    def delete_shelf(self, shelf, commit=False):
        """ Delete one shelf from "shelves" table.
            Input---
              shelf_id - id of shelf to delete
            Output---
              shelf_data or error message
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
        """ Insert one new bos into "books_on_shelves" table.
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
        """ Retrieve all bos entries from "books_on_shelves" table
            given a book_id.
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
        return bos

    def get_bos_all(self):
        """ Retrieve all bos from "books_on_shelves" table.
            Input---
              None
            Output---
              bos_data or error message
        """
        self._cur.execute('SELECT * FROM books_on_shelves')
        self._cur.iterclass = TMBos
        return [ r for r in self._cur ]

    def delete_bos(self, bos, commit=False):
        """ Delete one bos from "books_on_shelves" table.
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
        self._cur.execute(
            'SELECT xattr FROM shelf_xattrs WHERE shelf_id=?', (shelf.id,))
        tmp = [ f[0] for f in self._cur.fetchall() ]
        return tmp

    def create_xattr(self, shelf, xattr, value):
        self._cur.INSERT('shelf_xattrs', (shelf.id, xattr, value))

    def delete_xattr(self, shelf, xattr):
        self._cur.DELETE('shelf_xattrs', 'shelf_id=? AND xattr=?',
                                         (shelf.id, xattr),
                                         commit=True)

    def commit(self):
        self._cur.commit()

    def close(self):
        self._cur.close()

#--------------------------------------------------------------------------

if __name__ == '__main__':
    raise SystemError('Write a child class and do your testing there.')
