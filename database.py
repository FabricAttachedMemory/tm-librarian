#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian database interface module for SQL backends
#---------------------------------------------------------------------------

import time

from bookshelves import TMBook, TMShelf, TMBos

from sqlcursors import SQLiteCursor

from pdb import set_trace

class LibrarianDBackendSQL(object):

    @staticmethod
    def argparse_extend(parser):
        group = parser.add_mutually_exclusive_group()
        group.add_argument("--db_file",
                           help="specify the SQLite3 database file")

    def __init__(self, *args, **kwargs):
        self._cur = SQLiteCursor(DBfile=kwargs['DBfile'])

    # sqlite: if PRIMARY, but not AUTOINC, you get autoinc behavior and
    # hole-filling.  Explicitly setting id overrides that.  Break it out
    # in case we switch to a UUID or something else.
    def _getnextid(self, table):
        self._cur.execute('SELECT MAX(id) FROM %s' % table)
        id = self._cur.fetchone()
        if isinstance(id[0], int):
            return id[0] + 1
        # no rows? double check
        self._cur.execute('SELECT COUNT(id) FROM %s' % table)
        id = self._cur.fetchone()[0]
        if id == 0:
            return 1    # first id is non-zero
        raise RuntimeError('Cannot calculate nextid for ' + table)

    #
    # DB globals
    #

    def get_version(self):
        self._cur.execute('SELECT schema_version FROM globals')
        return self._cur.fetchone()[0]

    def get_nvm_parameters(self):
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
        print("get all books from db")
        try:
            db_query = "SELECT * FROM books ORDER BY book_id"
            self.cur.execute(db_query)
            book_data = self.cur.fetchall()
            return(book_data)
        except sqlite3.Error:
            return("get_book_all: error retrieving all books")

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
        print("get all bos from db")
        try:
            db_query = "SELECT * FROM books_on_shelves"
            self.cur.execute(db_query)
            bos_data = self.cur.fetchall()
            return(bos_data)
        except sqlite3.Error:
            return("get_bos_all: error retrieving all bos")

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

    #
    # Testing - SQLite 3
    #

    def check_consisency(self):
        print("check_tables()")

        # select all shelves by name, no dupes#

        total_tables = 0
        table_names = []
        tables_to_ignore = ["sqlite_sequence"]
        db_query = """
            SELECT name FROM sqlite_master
            WHERE type='table' ORDER BY Name
            """
        self.cur.execute(db_query)
        tables = map(lambda t: t[0], self.cur.fetchall())

        for table in tables:

            if (table in tables_to_ignore):
                continue

            db_query = "PRAGMA table_info(%s)" % (table)
            self.cur.execute(db_query)
            number_of_columns = len(self.cur.fetchall())

            db_query = "PRAGMA table_info(%s)" % (table)
            self.cur.execute(db_query)
            columns = self.cur.fetchall()

            db_query = "SELECT Count() FROM %s" % (table)
            self.cur.execute(db_query)
            number_of_rows = self.cur.fetchone()[0]

            print("Table: %s (columns = %d, rows = %d)" %
                  (table, number_of_columns, number_of_rows))

            for column in columns:
                print("  ", column)

            table_names.append(table)
            total_tables += 1

        print("Total number of tables: %d" % total_tables)

    def close(self):
        self._cur.close()

###########################################################################

if __name__ == '__main__':

    db = LibrarianDB()
    db.check_consistency()

    #
    # Test "books" methods
    #

    # Add a single book to the database and then get it
    book_id = 0x0DEAD0000000BEEF
    node_id = 0x1010101010101010
    size_bytes = 0x200000000
    book_data = (book_id, node_id, 0, 0, size_bytes)
    status_create = db.create_book(book_data)
    book_info = db.get_book_by_id(book_id)
    print("create/get book:")
    print("  book_id = 0x%016x" % book_id)
    print("  node_id = 0x%016x" % node_id)
    print("  book_data =", book_data)
    print("  status_create:", status_create)
    print("  book_info =", book_info)

    # Modify a single book entry in database
    book_id = 0x0DEAD0000000BEEF
    node_id = 0x2020202020202020
    size_bytes = 0x200000000
    book_data = (book_id, node_id, 1, 1, size_bytes)
    status_modify = db.modify_book(book_data)
    book_info = db.get_book_by_id(book_id)
    print("modify/get book:")
    print("  book_id = 0x%016x" % book_id)
    print("  node_id = 0x%016x" % node_id)
    print("  book_data =", book_data)
    print("  status_modify =", status_modify)
    print("  book_info =", book_info)

    # Delete single book entry in database
    book_id = 0x0DEAD0000000BEEF
    status_delete = db.delete_book(book_id)
    book_info = db.get_book_by_id(book_id)
    print("delete/get book:")
    print("  book_id = 0x%016x" % book_id)
    print("  status_delete =", status_modify)
    print("  book_info =", book_info)

    # Add two books to the database and then get them
    book_id1 = 0x0DEAD0000000AAAA
    node_id1 = 0x1010101010101010
    size_bytes1 = 0x200000000
    book_data1 = (book_id1, node_id1, 1, 1, size_bytes1)
    book_id2 = 0x0DEAD0000000BBBB
    node_id2 = 0x2020202020202020
    size_bytes2 = 0x200000000
    book_data2 = (book_id2, node_id2, 0, 1, size_bytes2)
    status_create1 = db.create_book(book_data1)
    status_create2 = db.create_book(book_data2)
    book_info = db.get_book_all()
    print("create/get two books:")
    print("  book_id1 = 0x%016x" % book_id1)
    print("  node_id1 = 0x%016x" % node_id1)
    print("  book_data1 =", book_data1)
    print("  status_create1 = ", status_create1)
    print("  book_id2 = 0x%016x" % book_id2)
    print("  node_id2 = 0x%016x" % node_id2)
    print("  book_data2 =", book_data2)
    print("  status_create2 = ", status_create2)
    for book in book_info:
        print("  book =", book)

    # Add four books to the database and then get them
    node_id = 0x2020202020202020
    status = 0
    max_books = 3
    book_id1 = 0x0DEAD0000000CCCC
    node_id1 = 0x2020202020202020
    size_bytes1 = 0x200000000
    book_data1 = (book_id1, node_id1, 0, 2, size_bytes1)
    book_id2 = 0x0DEAD0000000DDDD
    node_id2 = 0x2020202020202020
    size_bytes2 = 0x200000000
    book_data2 = (book_id2, node_id2, 1, 3, size_bytes2)
    book_id3 = 0x0DEAD0000000EEEE
    node_id3 = 0x2020202020202020
    size_bytes3 = 0x200000000
    book_data3 = (book_id3, node_id3, 0, 4, size_bytes3)
    book_id4 = 0x0DEAD0000000FFFF
    node_id4 = 0x2020202020202020
    size_bytes4 = 0x200000000
    book_data4 = (book_id4, node_id4, 0, 5, size_bytes4)
    status_create1 = db.create_book(book_data1)
    status_create2 = db.create_book(book_data2)
    status_create3 = db.create_book(book_data3)
    status_create4 = db.create_book(book_data4)
    book_info = db.get_book_by_node(node_id1, status, max_books)
    print("create/get three free books by node:")
    print("  book_id1 = 0x%016x" % book_id1)
    print("  node_id1 = 0x%016x" % node_id1)
    print("  book_data1 =", book_data1)
    print("  status_create1 = ", status_create1)
    print("  book_id2 = 0x%016x" % book_id2)
    print("  node_id2 = 0x%016x" % node_id2)
    print("  book_data2 =", book_data2)
    print("  status_create3 = ", status_create3)
    print("  book_id3 = 0x%016x" % book_id3)
    print("  node_id3 = 0x%016x" % node_id3)
    print("  book_data3 =", book_data3)
    print("  status_create4 = ", status_create4)
    print("  book_id4 = 0x%016x" % book_id4)
    print("  node_id4 = 0x%016x" % node_id4)
    print("  book_data4 =", book_data4)
    print("  status_create4 = ", status_create4)
    for book in book_info:
        print("  book =", book)

    #
    # Test "shelves" methods
    #

    # Add a single shelf to the database and then get it
    shelf_id = 0x0DEAD0000000BEEF
    c_time = time.time()
    m_time = time.time()
    shelf_data = (shelf_id, 0, 0, 0, c_time, m_time)
    status_create = db.create_shelf(shelf_data)
    shelf_info = db.get_shelf(shelf_id)
    print("create/get shelf:")
    print("  shelf_id = 0x%016x" % shelf_id)
    print("  c_time = %f (%s)" % (c_time, time.ctime(c_time)))
    print("  m_time = %f (%s)" % (m_time, time.ctime(m_time)))
    print("  shelf_data =", shelf_data)
    print("  status_create =", status_create)
    print("  shelf_info =", shelf_info)

    # Modify a single shelf entry in database
    shelf_id = 0x0DEAD0000000BEEF
    shelf_info = db.get_shelf(shelf_id)
    shelf_id, size_bytes, book_count, open_count, c_time, m_time = (shelf_info)
    m_time = time.time()
    shelf_data = (shelf_id, 1, 1, 1, c_time, m_time)
    status_modify = db.modify_shelf(shelf_data)
    shelf_info = db.get_shelf(shelf_id)
    print("get/modify/get shelf:")
    print("  shelf_id = 0x%016x" % shelf_id)
    print("  c_time = %f (%s)" % (c_time, time.ctime(c_time)))
    print("  m_time = %f (%s)" % (m_time, time.ctime(m_time)))
    print("  shelf_data =", shelf_data)
    print("  status_modify =", status_modify)
    print("  shelf_info =", shelf_info)

    # Delete single shelf entry in database
    shelf_id = 0x0DEAD0000000BEEF
    status_delete = db.delete_shelf(shelf_id)
    shelf_info = db.get_shelf(shelf_id)
    print("delete/get shelf:")
    print("  shelf_id = 0x%016x" % shelf_id)
    print("  status_delete =", status_modify)
    print("  shelf_info =", shelf_info)

    # Add two shelves to the database and then get them
    shelf_id1 = 0x0DEAD0000000AAAA
    c_time1 = time.time()
    m_time1 = time.time()
    shelf_data1 = (shelf_id1, 1, 1, 1, c_time1, m_time1)
    shelf_id2 = 0x0DEAD0000000BBBB
    c_time2 = time.time()
    m_time2 = time.time()
    shelf_data2 = (shelf_id2, 2, 2, 2, c_time2, m_time2)
    status_create1 = db.create_shelf(shelf_data1)
    status_create2 = db.create_shelf(shelf_data2)
    shelf_info = db.get_shelf_all()
    print("create/get two shelves:")
    print("  shelf_id1 = 0x%016x" % shelf_id1)
    print("  shelf_data1 =", shelf_data1)
    print("  status_create1 =", status_create1)
    print("  shelf_id2 = 0x%016x" % shelf_id2)
    print("  shelf_data2 =", shelf_data2)
    print("  status_create2 =", status_create2)
    for shelf in shelf_info:
        print("  shelf =", shelf)

    #
    # Test "books_on_shelves" methods
    #

    # Add a single "books on shelf" (bos) to the database and then get it
    shelf_id = 0x0DEAD0000000AAAA
    book_id = 0x1010101010101010
    bos_data = (shelf_id, book_id, 1)
    status_create = db.create_bos(bos_data)
    bos_info_shelf = db.get_bos_by_shelf(shelf_id)
    bos_info_book = db.get_bos_by_book(book_id)
    print("create/get bos:")
    print("  shelf_id = 0x%016x" % shelf_id)
    print("  book_id = 0x%016x" % book_id)
    print("  bos_data =", bos_data)
    print("  status_create =", status_create)
    print("  bos_info_shelf =", bos_info_shelf)
    print("  bos_info_book =", bos_info_book)

    # Delete single bos entry in database
    shelf_id = 0x0DEAD0000000AAAA
    book_id = 0x1010101010101010
    bos_data = (shelf_id, book_id, 1)
    status_delete = db.delete_bos(bos_data)
    bos_info = db.get_bos_by_shelf(shelf_id)
    print("delete/get bos:")
    print("  shelf_id = 0x%016x" % shelf_id)
    print("  status_delete =", status_modify)
    print("  bos_info =", bos_info)

    # Add two bos to the database and then get them
    shelf_id1 = 0x0DEAD0000000AAAA
    book_id1 = 0x1010101010101010
    bos_data1 = (shelf_id1, book_id1, 3)
    shelf_id2 = 0x0DEAD0000000BBBB
    book_id2 = 0x2020202020202020
    bos_data2 = (shelf_id2, book_id2, 4)
    status_create1 = db.create_bos(bos_data1)
    status_create2 = db.create_bos(bos_data2)
    bos_info = db.get_bos_all()
    print("create/get two shelf reservations:")
    print("  shelf_id1 = 0x%016x" % shelf_id1)
    print("  book_id1 = 0x%016x" % book_id1)
    print("  bos_data1 =", bos_data1)
    print("  status_create1 = ", status_create1)
    print("  shelf_id2 = 0x%016x" % shelf_id2)
    print("  book_id2 = 0x%016x" % book_id2)
    print("  bos_data2 =", bos_data2)
    print("  status_create2 =s", status_create2)
    for bos in bos_info:
        print("  bos =", bos)

    # Cleanup
    db.close()
