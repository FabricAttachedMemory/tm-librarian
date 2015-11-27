#!/usr/bin/python3 -tt
###########################################################################
# Some of this might elevate nicely to SQLassist.  The primmary concern
# is cursor.rowcount, which doesn't seemt to be valid after SELECT.

from pdb import set_trace

import os
import sqlite3

from sqlassist import SQLassist
from sqlbackend import LibrarianDBackendSQL


class SQLite3assist(SQLassist):

    _SQLshowtables = 'SELECT name FROM main.sqlite_master WHERE type="table";'
    _SQLshowschema = 'PRAGMA table_info({});'

    def DBconnect(self):
        self._conn = sqlite3.connect(self.db_file,
                                     isolation_level='EXCLUSIVE')
        self._cursor = self._conn.cursor()
        # WAL: https://www.sqlite.org/wal.html
        self.execute('PRAGMA journal_mode=WAL')
        pass

    def __init__(self, **kwargs):
        if 'db_file' in kwargs:
            fname = kwargs['db_file']
            if fname != ':memory:':
                assert (os.path.isfile(fname) and
                        os.access(fname, os.R_OK)), 'Cannot read %s' % fname
        else:
            kwargs['db_file'] = ':memory:'
        super(self.__class__, self).__init__(**kwargs)

    def schema(self, table):
        self.execute(self._SQLshowschema.format(table))
        tmp = tuple([str(row[1]) for row in self.fetchall()])
        assert tmp, 'Empty schema'
        return tmp

    def INSERT(self, table, values, commit=True):
        '''Values are a COMPLETE tuple following ordered schema.  Primary
           key (always 'id')  can be "None" for DB to autoselect.'''
        assert values, 'missing values for INSERT'
        qmarks = ', '.join(['?'] * len(values))
        self.execute(
            'INSERT INTO %s VALUES (%s)' % (table, qmarks),
            values
        )
        # First is explicit failure like UNIQUE collision, second is generic
        if self.execfail:
            raise AssertionError(
                'INSERT %s failed: %s' % (table, self.execfail))
        if self.rowcount != 1:
            self.rollback()
            raise AssertionError('INSERT INTO %s failed' % table)
        if commit:
            self.commit()
        return self.lastrowid   # good with or without commit

    def UPDATE(self, table, setclause, values, commit=False):
        '''setclause is effective key=val, key=val sequence'''
        sql = 'UPDATE %s SET %s' % (table, setclause)
        self.execute(sql, values)
        if self.rowcount != 1:
            self.rollback()
            raise AssertionError('UPDATE %s failed' % table)
        if commit:
            self.commit()

    # FIXME: move fields2qmarks in here (makes sense), then just pass
    # the data object?  Or do schema manips belong "one level up" ?
    def DELETE(self, table, where, values, commit=False):
        '''Values are a COMPLETE tuple following ordered schema'''
        assert values, 'missing values for DELETE'
        sql = 'DELETE FROM %s WHERE %s' % (table, where)
        self.execute(sql, values)
        if self.rowcount != 1:
            self.rollback()
            raise AssertionError('DELETE FROM %s %s failed' % (table, values))
        if commit:
            self.commit()

    #
    # sqlite: if PRIMARY, but not AUTOINC, you get autoinc behavior and
    # hole-filling.  Explicitly setting id overrides that.
    #
    def getnextid(self, table):
        self.execute('SELECT MAX(id) FROM %s' % table)
        id = self.fetchone()
        if isinstance(id[0], int):
            return id[0] + 1
        # no rows? double check
        self.execute('SELECT COUNT(id) FROM %s' % table)
        id = self.fetchone()[0]
        if id == 0:
            return 1    # first id is non-zero
        raise RuntimeError('Cannot calculate nextid for %s' % table)

    #
    # To assist testing
    #

    def check_tables(self):
        print("Checking data tables()")

        total_tables = 0
        table_names = []
        tables_to_ignore = ( 'sqlite_sequence', )
        self.execute("""SELECT name FROM sqlite_master
                             WHERE type='table'
                             ORDER BY Name""")
        tables = [ t[0] for t in self.fetchall() if
                   t[0] not in tables_to_ignore ]

        for table in tables:

            db_query = "PRAGMA table_info(%s)" % (table)
            self.execute(db_query)
            number_of_columns = len(self.fetchall())

            db_query = "PRAGMA table_info(%s)" % (table)
            self.execute(db_query)
            columns = self.fetchall()

            db_query = "SELECT Count() FROM %s" % (table)
            self.execute(db_query)
            number_of_rows = self.fetchone()[0]

            print("Table: %s (columns = %d, rows = %d)" %
                  (table, number_of_columns, number_of_rows))

            for column in columns:
                print("  ", column)

            table_names.append(table)
            total_tables += 1

        print("Total number of tables: %d" % total_tables)

###########################################################################


class LibrarianDBackendSQLite3(LibrarianDBackendSQL):

    @staticmethod
    def argparse_extend(parser):
        # group = parser.add_mutually_exclusive_group()
        parser.add_argument(
            '--db_file',
            help='SQLite3 database backing store file',
            required=True)

    def __init__(self, args):
        self._cur = SQLite3assist(db_file=args.db_file)

###########################################################################

if __name__ == '__main__':

    import time
    from book_register import create_empty_db
    from book_shelf_bos import TMBook, TMShelf, TMBos

    # old_main_from_sqlcursors()
    # old_main_from_database()

#--------------------------------------------------------------------------


def old_main_from_sqlcursors():
    print("--> Setup empty database, create and check table schemas")
    cur = SQLite3Cursor()
    create_empty_db(cur)
    print("    Table (Globals):", cur.schema('Globals'))
    print("    Table (Books):", cur.schema('Books'))
    print("    Table (Shelves):", cur.schema('Shelves'))
    print("    Table (Books_on_shelf):", cur.schema('Books_on_shelf'))

    print("--> Write book size to the globals table, then print table")
    bs_size = (1024 * 1024 * 1024 * 8)
    cur.execute('INSERT INTO globals VALUES(?)', bs_size)
    cur.commit()
    cur.execute('SELECT * FROM globals')
    cur.iterclass = 'default'
    for r in cur:
        print("   ", r)

    print("--> Add five books to database, then retrieve and print them")
    node1 = 0x000000000000AAAA
    node2 = 0x000000000000BBBB
    book_id1 = 0x1111111111111111
    book_id2 = 0x2222222222222222
    book_id3 = 0x3333333333333333
    book_id4 = 0x4444444444444444
    book_id5 = 0x5555555555555555
    book1 = TMBook(book_id1, node1, 0, 0)
    book2 = TMBook(book_id2, node1, 0, 0)
    book3 = TMBook(book_id3, node1, 0, 0)
    book4 = TMBook(book_id4, node2, 0, 0)
    book5 = TMBook(book_id5, node2, 0, 0)
    cur.execute('INSERT INTO books VALUES(?, ?, ?, ?)', book1.tuple())
    cur.execute('INSERT INTO books VALUES(?, ?, ?, ?)', book2.tuple())
    cur.execute('INSERT INTO books VALUES(?, ?, ?, ?)', book3.tuple())
    cur.execute('INSERT INTO books VALUES(?, ?, ?, ?)', book4.tuple())
    cur.execute('INSERT INTO books VALUES(?, ?, ?, ?)', book5.tuple())
    cur.commit()
    cur.execute('SELECT * FROM books')
    cur.iterclass = 'None'
    print("    all books (iterclass=None) ---")
    for r in cur.fetchall():
        print("     ", r)
    cur.execute('SELECT * FROM books')
    cur.iterclass = 'default'
    print("    all books (iterclass=default) ---")
    for r in cur:
        print("     ", r)
    cur.execute('SELECT * FROM books')
    cur.iterclass = TMBook
    print("    all books (iterclass=TMBook) ---")
    for r in cur:
        print(r)

    cur.execute('SELECT * FROM books WHERE node_id = ?', node1)
    cur.iterclass = 'default'
    print("    node1 books (default) ---")
    for r in cur:
        print("     ", r)

    print("--> Delete one book and modify one book then print them")
    book_id1 = 0x1111111111111111
    book_id2 = 0x2222222222222222
    cur.execute('DELETE FROM books WHERE book_id = ?', book_id1)
    cur.execute('UPDATE books SET allocated = ? WHERE book_id = ?',
                (1, book_id2))
    cur.commit()
    cur.execute('SELECT * FROM books')
    cur.iterclass = 'default'
    print("    all books (default) ---")
    for r in cur:
        print("     ", r)

    print("--> Create three shelves, then print them")
    shelf_id1 = 0x00000000AAAA0000
    shelf_id2 = 0x00000000BBBB0000
    shelf_id3 = 0x00000000CCCC0000
    c_id1 = 0x1111000011110000
    c_id2 = 0x2222000022220000
    c_id3 = 0x3333000033330000
    c_time = time.time()
    m_time = time.time()
    shelf1 = TMShelf(shelf_id1, c_id1, 0, 0, 0, c_time, m_time, "shelf_1")
    shelf2 = TMShelf(shelf_id2, c_id2, 0, 0, 0, c_time, m_time, "shelf_2")
    shelf3 = TMShelf(shelf_id3, c_id3, 0, 0, 0, c_time, m_time, "shelf_3")
    cur.execute('INSERT INTO shelves VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
                shelf1.tuple())
    cur.execute('INSERT INTO shelves VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
                shelf2.tuple())
    cur.execute('INSERT INTO shelves VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
                shelf3.tuple())
    cur.commit()
    cur.execute('SELECT * FROM shelves')
    cur.iterclass = 'default'
    print("    all shelves (default) ---")
    for r in cur:
        print("     ", r)

    print("--> Delete one shelf and modify one shelf then print them all")
    shelf_id1 = 0x00000000AAAA0000
    shelf_id2 = 0x00000000BBBB0000
    cur.execute('DELETE FROM shelves WHERE shelf_id = ?', shelf_id1)
    cur.execute('UPDATE shelves SET open_count = ? WHERE shelf_id = ?',
                (1, shelf_id2))
    cur.commit()
    cur.execute('SELECT * FROM shelves')
    cur.iterclass = 'default'
    print("    all shelves (default )---")
    for r in cur:
        print("     ", r)

    print("--> Create four BOS records, then print them")
    shelf_id1 = 0x00000000AAAA0000
    shelf_id2 = 0x00000000BBBB0000
    shelf_id3 = 0x00000000CCCC0000
    book_id1 = 0x1111111111111111
    book_id2 = 0x2222222222222222
    book_id3 = 0x3333333333333333
    book_id4 = 0x4444444444444444
    book_id5 = 0x5555555555555555
    bos1 = TMBos(shelf_id1, book_id1, 1)
    bos2 = TMBos(shelf_id1, book_id2, 2)
    bos3 = TMBos(shelf_id2, book_id3, 1)
    bos4 = TMBos(shelf_id2, book_id4, 2)
    bos5 = TMBos(shelf_id3, book_id5, 1)
    cur.execute('INSERT INTO books_on_shelf VALUES(?, ?, ?)', bos1.tuple())
    cur.execute('INSERT INTO books_on_shelf VALUES(?, ?, ?)', bos2.tuple())
    cur.execute('INSERT INTO books_on_shelf VALUES(?, ?, ?)', bos3.tuple())
    cur.execute('INSERT INTO books_on_shelf VALUES(?, ?, ?)', bos4.tuple())
    cur.execute('INSERT INTO books_on_shelf VALUES(?, ?, ?)', bos5.tuple())
    cur.commit()
    cur.execute('SELECT * FROM books_on_shelf')
    cur.iterclass = 'default'
    print("    all bos (default) ---")
    for r in cur:
        print("     ", r)

    print("--> Delete all shelf1 bos, then print them all")
    shelf_id1 = 0x00000000AAAA0000
    shelf_id2 = 0x00000000BBBB0000
    cur.execute('DELETE FROM books_on_shelf WHERE shelf_id = ?', shelf_id1)
    cur.commit()
    cur.execute('SELECT * FROM books_on_shelf')
    cur.iterclass = 'default'
    print("    all shelves (default)---")
    for r in cur:
        print("     ", r)

    print("--> Create two shelf_open records, then print them")
    shelf_id1 = 0x00000000AAAA0000
    shelf_id2 = 0x00000000BBBB0000
    node1 = 0x000000000000AAAA
    node2 = 0x000000000000BBBB
    pid1 = 0x0000000000001111
    pid2 = 0x0000000000002222
    so1 = (shelf_id1, node1, pid1)
    so2 = (shelf_id2, node2, pid2)
    cur.execute('INSERT INTO shelf_open VALUES(?, ?, ?)', so1)
    cur.execute('INSERT INTO shelf_open VALUES(?, ?, ?)', so2)
    cur.commit()
    cur.execute('SELECT * FROM shelf_open')
    cur.iterclass = 'default'
    print("    all shelf_open items (default) ---")
    for r in cur:
        print("     ", r)

    print("--> Create four BOS records, then print them")
    shelf_id1 = 0x00000000AAAA0000
    shelf_id2 = 0x00000000BBBB0000
    shelf_id3 = 0x00000000CCCC0000
    book_id1 = 0x1111111111111111
    book_id2 = 0x2222222222222222
    book_id3 = 0x3333333333333333
    book_id4 = 0x4444444444444444
    book_id5 = 0x5555555555555555
    bos1 = TMBos(shelf_id1, book_id1, 1)
    bos2 = TMBos(shelf_id1, book_id2, 2)
    bos3 = TMBos(shelf_id2, book_id3, 1)
    bos4 = TMBos(shelf_id2, book_id4, 2)
    bos5 = TMBos(shelf_id3, book_id5, 1)
    cur.execute('INSERT INTO books_on_shelf VALUES(?, ?, ?)', bos1.tuple())
    cur.execute('INSERT INTO books_on_shelf VALUES(?, ?, ?)', bos2.tuple())
    cur.execute('INSERT INTO books_on_shelf VALUES(?, ?, ?)', bos3.tuple())
    cur.execute('INSERT INTO books_on_shelf VALUES(?, ?, ?)', bos4.tuple())
    cur.execute('INSERT INTO books_on_shelf VALUES(?, ?, ?)', bos5.tuple())
    cur.commit()
    cur.execute('SELECT * FROM books_on_shelf')
    cur.iterclass = 'default'
    print("    all bos (default) ---")
    for r in cur:
        print("     ", r)

    cur.close()

#--------------------------------------------------------------------------


def old_main_from_database():

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
