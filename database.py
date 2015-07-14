#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian database interface module
#---------------------------------------------------------------------------

import os
import sqlite3
import time

DB_FILE = "./librarian_db"


class LibrarianDB(object):
    con = None
    cur = None
    db_file = None

    def db_init(self, args):
        """ Initialize database and create tables if they do not exist.
            Input---
              args - command line arguments
            Output---
              None
        """
        if args.db_memory is True:
            db_file = ":memory:"
            print ("Using in memory database: %s" % (db_file))
        elif args.db_file:
            db_file = args.db_file
            print ("Using custom database file: %s" % (db_file))
        else:
            db_file = DB_FILE
            print ("Using default database file: %s" % (db_file))

        print("Connecting to database: %s" % (db_file))
        self.con = sqlite3.connect(db_file)
        self.cur = self.con.cursor()

        print("Creating tables in database")
        table_create = """
            CREATE TABLE IF NOT EXISTS books (
            book_id INT PRIMARY KEY,
            node_id INT,
            status INT,
            attributes INT,
            size_bytes INT
            )
            """
        self.cur.execute(table_create)

        table_create = """
            CREATE TABLE IF NOT EXISTS shelves (
            shelf_id INT PRIMARY KEY,
            size_bytes INT,
            book_count INT,
            open_count INT,
            c_time REAL,
            m_time REAL
            )
            """
        self.cur.execute(table_create)

        table_create = """
            CREATE TABLE IF NOT EXISTS books_on_shelf (
            shelf_id INT,
            book_id INT,
            seq_num INT
            )
            """
        self.cur.execute(table_create)

        self.con.commit()
        return("pass")

    #
    # books
    #
# CREATE TABLE IF NOT EXISTS books (
# book_id INT PRIMARY KEY,
# node_id INT,
# status INT,
# attributes INT,
# size_bytes INT

    def create_book(self, book_data):
        """ Insert one new book into "books" table.
            Input---
              book_data - list of book data to insert
            Output---
              book_data or error message
        """
        print("add book to db:", book_data)
        try:
            db_query = "INSERT INTO books VALUES (?,?,?,?,?)"
            self.cur.execute(db_query, book_data)
            self.con.commit()
            return(book_data)
        except sqlite3.Error:
            return("create_book: error inserting new book")

    def get_book(self, book_id):
        """ Retrieve one book from "books" table.
            Input---
              book_id - id of book to get
            Output---
              book_data or error message
        """
        print("get book from db:", book_id)
        try:
            db_query = "SELECT * FROM books WHERE book_id = ?"
            self.cur.execute(db_query, (book_id,))
            book_data = self.cur.fetchone()
            return(book_data)
        except sqlite3.Error:
            return("get_book: error retrieving book [book_id: 0x016xd]"
                   % (book_id))

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

    def modify_book(self, book_data):
        """ Modify book data in "books" table.
            Input---
              book_data - list of new book data
            Output---
              book_data or error message
        """
        print("modify book in db:", book_data)
        book_id, node_id, status, attributes, size_bytes = (book_data)
        try:
            db_query = """
                UPDATE books SET
                node_id = ?,
                status = ?,
                attributes = ?,
                size_bytes = ?
                WHERE book_id = ?
                """
            self.cur.execute(db_query, (node_id, status,
                             attributes, size_bytes, book_id))
            self.con.commit()
            return(book_data)
        except sqlite3.Error:
            return("modify_book: error modifying existing book data")

    def delete_book(self, book_id):
        """ Delete one book from "books" table.
            Input---
              book_id - id of book to delete
            Output---
              book_data or error message
        """
        print("delete book from db:", book_id)
        try:
            db_query = "DELETE FROM books WHERE book_id = ?"
            self.cur.execute(db_query, (book_id,))
            return(book_id)
        except sqlite3.Error:
            return("delete_book: error deleting book [book_id: 0x016xd]"
                   % (book_id))

    #
    # shelves
    #

    def create_shelf(self, shelf_data):
        """ Insert one new shelf into "shelves" table.
            Input---
              shelf_data - list of shelf data to insert
            Output---
              shelf_data or error message
        """
        print("add shelf to db:", shelf_data)
        try:
            db_query = "INSERT INTO shelves VALUES (?,?,?,?,?,?)"
            self.cur.execute(db_query, shelf_data)
            self.con.commit()
            return(shelf_data)
        except sqlite3.Error:
            return("create_shelf: error inserting new shelf")

    def get_shelf(self, shelf_id):
        """ Retrieve one shelf from "shelves" table.
            Input---
              shelf_id - id of shelf to get
            Output---
              shelf_id or error message
        """
        print("get shelf from db:", shelf_id)
        try:
            db_query = "SELECT * FROM shelves WHERE shelf_id = ?"
            self.cur.execute(db_query, (shelf_id,))
            shelf_data = self.cur.fetchone()
            return(shelf_data)
        except sqlite3.Error:
            return("get_shelf: error retrieving shelf [shelf_id: 0x016xd]"
                   % (shelf_id))

    def get_shelf_all(self):
        """ Retrieve all shelves from "shelves" table.
            Input---
              None
            Output---
              shelf_data or error message
        """
        print("get all shelves from db")
        try:
            db_query = "SELECT * FROM shelves ORDER BY shelf_id"
            self.cur.execute(db_query)
            shelf_data = self.cur.fetchall()
            return(shelf_data)
        except sqlite3.Error:
            return("get_shelf_all: error retrieving all shelf")

    def modify_shelf(self, shelf_data):
        """ Modify shelf data in "shelves" table.
            Input---
              shelf_data - list of new shelf data
            Output---
              shelf_data or error message
        """
        print("modify shelf in db:", shelf_data)
        shelf_id, size_bytes, book_count, open_count, \
            c_time, m_time = (shelf_data)
        try:
            db_query = """
                UPDATE shelves SET
                size_bytes = ?,
                book_count = ?,
                open_count = ?,
                c_time = ?,
                m_time = ?
                WHERE shelf_id = ?
                """
            self.cur.execute(db_query, (size_bytes, book_count, open_count,
                             c_time, m_time, shelf_id))
            self.con.commit()
            return(shelf_data)
        except sqlite3.Error:
            return("modify_shelf: error modifying existing shelf data")

    def delete_shelf(self, shelf_id):
        """ Delete one shelf from "shelves" table.
            Input---
              shelf_id - id of shelf to delete
            Output---
              shelf_data or error message
        """
        print("delete shelf from db:", shelf_id)
        try:
            db_query = "DELETE FROM shelves WHERE shelf_id = ?"
            self.cur.execute(db_query, (shelf_id,))
            return(shelf_id)
        except sqlite3.Error:
            return("delete_shelf: error deleting shelf \
                  [shelf_id: 0x016xd]" % (shelf_id))

    #
    # books_on_shelf
    #
# CREATE TABLE IF NOT EXISTS books_on_shelf (
# shelf_id INT,
# book_id INT,
# seq_num INT

    def create_bos(self, bos_data):
        """ Insert one new bos into "books_on_shelf" table.
            Input---
              bos_data - list of bos data to insert
            Output---
              status: success = 0
                      failure = -1
        """
        print("add bos to db:", bos_data)
        try:
            db_query = "INSERT INTO books_on_shelf VALUES (?,?,?)"
            self.cur.execute(db_query, bos_data)
            self.con.commit()
            return(bos_data)
        except sqlite3.Error:
            return("create_bos: error inserting new bos")

    def get_bos_by_shelf(self, shelf_id):
        """ Retrieve all bos entries from "books_on_shelf" table
            given a shelf_id.
            Input---
              shelf_id - shelf identifier
            Output---
              bos_data or error message
        """
        print("get bos by shelf from db:", shelf_id)
        try:
            db_query = """
                SELECT rowid,* FROM books_on_shelf
                WHERE shelf_id = ?
                """
            self.cur.execute(db_query, (shelf_id,))
            bos_data = self.cur.fetchall()
            return(bos_data)
        except sqlite3.Error:
            return("get_bos_by_shelf: error retrieving bos \
                   [shelf_id: 0x016xd]" % (book_id))

    def get_bos_by_book(self, book_id):
        """ Retrieve all bos entries from "books_on_shelf" table
            given a book_id.
            Input---
              book_id - book identifier
            Output---
              bos_data or error message
        """
        print("get bos by book from db:", book_id)
        try:
            db_query = """
                SELECT rowid,* FROM books_on_shelf
                WHERE book_id = ?
                """
            self.cur.execute(db_query, (book_id,))
            bos_data = self.cur.fetchall()
            return(bos_data)
        except sqlite3.Error:
            return("get_bos_by_book: error retrieving bos \
                   [book_id: 0x016xd]" % (book_id))

    def get_bos_all(self):
        """ Retrieve all bos from "books_on_shelf" table.
            Input---
              None
            Output---
              bos_data or error message
        """
        print("get all bos from db")
        try:
            db_query = "SELECT rowid,* FROM books_on_shelf"
            self.cur.execute(db_query)
            bos_data = self.cur.fetchall()
            return(bos_data)
        except sqlite3.Error:
            return("get_bos_all: error retrieving all bos")

    def delete_bos(self, bos_data):
        """ Delete one bos from "books_on_shelf" table.
            Input---
              bos_data - list of bos data
            Output---
              bos_data or error message
        """
        print("delete bos from db:", bos_data)
        try:
            db_query = """
                DELETE FROM books_on_shelf
                WHERE shelf_id = ? AND
                book_id = ? AND
                seq_num = ?
                """
            self.cur.execute(db_query, bos_data)
            return(bos_data)
        except sqlite3.Error:
            return("delete_bos: error deleting bos", bos_data)

    #
    # Testing
    #

    def check_tables(self):
        print("check_tables()")
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
        self.con.close()


def db_args_init(parser):
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--db_memory", action="store_true",
                       help="use an in memory database")
    group.add_argument("--db_file",
                       help="specify the database file, (default = %s)"
                       % (DB_FILE))

if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    db_args_init(parser)
    args = parser.parse_args()

    # Initialize database and check tables
    db = LibrarianDB()
    db.db_init(args)
    db.check_tables()

    #
    # Test "books" methods
    #

    # Add a single book to the database and then get it
    book_id = 0x0DEAD0000000BEEF
    node_id = 0x1010101010101010
    size_bytes = 0x200000000
    book_data = (book_id, node_id, 0, 0, size_bytes)
    status_create = db.create_book(book_data)
    book_info = db.get_book(book_id)
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
    book_info = db.get_book(book_id)
    print("modify/get book:")
    print("  book_id = 0x%016x" % book_id)
    print("  node_id = 0x%016x" % node_id)
    print("  book_data =", book_data)
    print("  status_modify =", status_modify)
    print("  book_info =", book_info)

    # Delete single book entry in database
    book_id = 0x0DEAD0000000BEEF
    status_delete = db.delete_book(book_id)
    book_info = db.get_book(book_id)
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
    book_data2 = (book_id2, node_id2, 2, 2, size_bytes2)
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
    # Test "books_on_shelf" methods
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
