#!/usr/bin/python3 -tt

#---------------------------------------------------------------------------
# Librarian database interface module for SQL backends.  Originally
# written against SQLite but it's so generic it's probably okay
# for MariaDB.  I'm not sure about Postgres but it could be close.
#---------------------------------------------------------------------------

import time

from book_shelf_bos import TMBook, TMShelf, TMBos

from pdb import set_trace

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

    def get_version(self):
        self._cur.execute('SELECT schema_version FROM globals')
        return self._cur.fetchone()[0]

    def get_nvm_parameters(self):
        '''Returns duple(book_size, total_nvm)'''
        self._cur.execute(
            'SELECT book_size_bytes, total_nvm_bytes FROM globals')
        return self._cur.fetchone()

    #
    # DB modifies.  Grouped like this to see similarites.
    #

    @staticmethod
    def _fields2qmarks(fields, joiner):
        q = [ '%s=?' % f for  f in fields ]
        return joiner.join(q)

    # matchfields provide the core of the SET clause and were set by
    # the importer.  Localmods fields were done in the direct caller
    # of this routine.  Those two field sets must be disjoint.

    def _modify_table(self, table, obj, localmods, commit):
        assert hasattr(obj, 'id'), 'object ineligible for this routine'
        assert obj.matchfields, 'Missing field(s) for matching'

        # id will be the WHERE clause and thus must come last
        id = ('id', )
        cantmatch = localmods + id
        tmp = set(obj.matchfields).intersection(set(cantmatch))
        assert not tmp, 'Bad matchfields %s' % str(tmp)

        fields = obj.matchfields + localmods
        qmarks = self._fields2qmarks(fields, ', ')
        setwhere = '%s WHERE id=?' % qmarks
        self._cur.UPDATE(table, setwhere, obj.tuple(fields + id))
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
        """ Modify shelf data in "shelves" table.
            Input---
              shelf_data - list of new shelf data
            Output---
              shelf_data or error message
        """
        shelf.mtime = int(time.time())
        return self._modify_table('shelves', shelf, ('mtime', ), commit)

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
        db_query = """
                SELECT * FROM books
                WHERE node_id = ? AND
                allocated = ?
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
    #

    def create_shelf(self, shelf, commit=False):
        """ Insert one new shelf into "shelves" table.
            Input---
              shelf_data - list of shelf data to insert
            Output---
              shelf_data or error message
        """
        shelf.id = self._getnextid('shelves')   # Could take a second
        tmp = int(time.time())
        shelf.ctime = shelf.mtime = tmp
        self._cur.INSERT('shelves', shelf.tuple())
        if commit:
            self._cur.commit()
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
        shelves = [ r for r in self._cur ]
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
              status: success = 0
                      failure = -1
        """
        self._cur.INSERT('books_on_shelves', bos.tuple())
        if commit:
            self._cur.commit()
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

    def get_xattr(self, shelf, xattr):
        self._cur.execute(
            'SELECT value FROM shelf_xattrs WHERE shelf_id=? AND xattr=?',
            (shelf.id, xattr))
        return self._cur.fetchone()[0]

    def close(self):
        self._cur.close()

#--------------------------------------------------------------------------

if __name__ == '__main__':
    raise SystemError('Write a child class and do your testing there.')
