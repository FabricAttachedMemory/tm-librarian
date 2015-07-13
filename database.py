#!/usr/bin/python3
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
              None
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
            status INT,
            shelf_handle INT,
            sequence INT,
            attributes INT,
            owner_id INT
            )
            """
        self.cur.execute(table_create)

        table_create = """
            CREATE TABLE IF NOT EXISTS shelves (
            shelf_handle INT PRIMARY KEY,
            size_bytes INT,
            book_count INT,
            open_count INT,
            owner_id INT,
            creation_date INT
            )
            """
        self.cur.execute(table_create)

        table_create = """
            CREATE TABLE IF NOT EXISTS shelf_reservations (
            reservation_id INT PRIMARY KEY,
            shelf_handle INT,
            date_reserved REAL
            )
            """
        self.cur.execute(table_create)

        table_create = """
            CREATE TABLE IF NOT EXISTS firewall_managers (
            node_id INT PRIMARY KEY,
            host TEXT,
            port INT,
            date_registered REAL
            )
            """
        self.cur.execute(table_create)

        self.con.commit()

    #
    # books
    #

    def create_book(self, book_data):
        """ Insert one new book into "books" table.
            Input---
              book_data - list of book data to insert
            Output---
              status: success = 0
                      failure = -1
        """
        print("add book to db:", book_data)
        try:
            db_query = "INSERT INTO books VALUES (?,?,?,?,?,?)"
            self.cur.execute(db_query, book_data)
            self.con.commit()
            return(0)
        except sqlite3.Error:
            print("create_book: error inserting new book")
            return(-1)

    def get_book(self, book_id):
        """ Retrieve one book from "books" table.
            Input---
              book_id - id of book to retrieve
            Output---
              status: success = 0
                      failure = -1
              book_data: success = list of book data
                         failure = None
        """
        print("get book from db:", book_id)
        try:
            db_query = "SELECT * FROM books WHERE book_id = ?"
            self.cur.execute(db_query, (book_id,))
            book_data = self.cur.fetchone()
            return(0, book_data)
        except sqlite3.Error:
            print("get_book: error retrieving book [book_id: 0x016xd]"
                  % (book_id))
            return(-1, None)

    def get_book_all(self):
        """ Retrieve all books from "books" table.
            Input---
              None
            Output---
              status: success = 0
                      failure = -1
              book_data: success = list of book data
                         failure =  None
        """
        print("get all books from db")
        try:
            db_query = "SELECT * FROM books"
            self.cur.execute(db_query)
            book_data = self.cur.fetchall()
            return(0, book_data)
        except sqlite3.Error:
            print("get_book_all: error retrieving all books")
            return(-1, None)

    def modify_book(self, book_data):
        """ Modify book data in "books" table.
            Input---
              book_data - list of new book data
            Output---
              status: success = 0
                      failure = -1
        """
        print("modify book in db:", book_data)
        book_id, status, shelf_handle, sequence, \
            attributes, owner_id = (book_data)
        try:
            db_query = """
                UPDATE books SET
                status = ?,
                shelf_handle = ?,
                sequence = ?,
                attributes = ?,
                owner_id = ?
                WHERE book_id = ?
                """
            self.cur.execute(db_query, (status, shelf_handle,
                             sequence, attributes, owner_id, book_id))
            self.con.commit()
            return(0)
        except sqlite3.Error:
            print("modify_book: error modifying existing book data")
            return(-1)

    def delete_book(self, book_id):
        """ Delete one book from "books" table.
            Input---
              book_id - id of book to delete
            Output---
              status: success = 0
                      failure = -1
        """
        print("delete book from db:", book_id)
        try:
            db_query = "DELETE FROM books WHERE book_id = ?"
            self.cur.execute(db_query, (book_id,))
            return(0)
        except sqlite3.Error:
            print("delete_book: error deleting book [book_id: 0x016xd]"
                  % (book_id))
            return(-1, None)

    #
    # shelves
    #

    def create_shelf(self, shelf_data):
        """ Insert one new shelf into "shelves" table.
            Input---
              shelf_data - list of shelf data to insert
            Output---
              status: success = 0
                      failure = -1
        """
        print("add shelf to db:", shelf_data)
        try:
            db_query = "INSERT INTO shelves VALUES (?,?,?,?,?,?)"
            self.cur.execute(db_query, shelf_data)
            self.con.commit()
            return(0)
        except sqlite3.Error:
            print("create_shelf: error inserting new shelf")
            return(-1)

    def get_shelf(self, shelf_handle):
        """ Retrieve one shelf from "shelves" table.
            Input---
              shelf_handle - id of shelf to retrieve
            Output---
              status: success = 0
                      failure = -1
              shelf_data: success = list of shelf data on success
                          failure = None
        """
        print("get shelf from db:", shelf_handle)
        try:
            db_query = "SELECT * FROM shelves WHERE shelf_handle = ?"
            self.cur.execute(db_query, (shelf_handle,))
            shelf_data = self.cur.fetchone()
            return(0, shelf_data)
        except sqlite3.Error:
            print("get_shelf: error retrieving shelf [shelf_handle: 0x016xd]"
                  % (shelf_handle))
            return(-1, None)

    def get_shelf_all(self):
        """ Retrieve all shelves from "shelves" table.
            Input---
              None
            Output---
              status: success = 0
                      failure = -1
              shelf_data: success = list of shelf data
                          failure = None
        """
        print("get all shelves from db")
        try:
            db_query = "SELECT * FROM shelves"
            self.cur.execute(db_query)
            shelf_data = self.cur.fetchall()
            return(0, shelf_data)
        except sqlite3.Error:
            print("get_shelf_all: error retrieving all shelf")
            return(-1, None)

    def modify_shelf(self, shelf_data):
        """ Modify shelf data in "shelves" table.
            Input---
              shelf_data - list of new shelf data
            Output---
              status: success = 0
                      failure = -1
        """
        print("modify shelf in db:", shelf_data)
        shelf_handle, size_bytes, book_count, open_count, \
            owner_id, creation_date = (shelf_data)
        try:
            db_query = """
                UPDATE shelves SET
                size_bytes = ?,
                book_count = ?,
                open_count = ?,
                owner_id = ?,
                creation_date = ?
                WHERE shelf_handle = ?
                """
            self.cur.execute(db_query, (size_bytes, book_count, open_count,
                             owner_id, creation_date, shelf_handle))
            self.con.commit()
            return(0)
        except sqlite3.Error:
            print("modify_shelf: error modifying existing shelf data")
            return(-1)

    def delete_shelf(self, shelf_handle):
        """ Delete one shelf from "shelves" table.
            Input---
              shelf_handle - id of shelf to delete
            Output---
              status: success = 0
                      failure = -1
        """
        print("delete shelf from db:", shelf_handle)
        try:
            db_query = "DELETE FROM shelves WHERE shelf_handle = ?"
            self.cur.execute(db_query, (shelf_handle,))
            return(0)
        except sqlite3.Error:
            print("delete_shelf: error deleting shelf \
                  [shelf_handle: 0x016xd]" % (shelf_handle))
            return(-1, None)

    #
    # shelf_reservations
    #

    def create_reservation(self, reservation_data):
        """ Insert one new reservation into "shelf_reservations" table.
            Input---
              reservation_data - list of reservation data to insert
            Output---
              status: success = 0
                      failure = -1
        """
        print("add reservation to db:", reservation_data)
        try:
            db_query = "INSERT INTO shelf_reservations VALUES (?,?,?)"
            self.cur.execute(db_query, reservation_data)
            self.con.commit()
            return(0)
        except sqlite3.Error:
            print("create_reservation: error inserting new reservation")
            return(-1)

    def get_reservation(self, reservation_id):
        """ Retrieve one reservation from "shelf_reservations" table.
            Input---
              reservation_id - id of reservation to retrieve
            Output---
              reservation_data: success - list of reservation data on success
                                failure - None
        """
        print("get reservation from db:", reservation_id)
        try:
            db_query = "SELECT * FROM shelf_reservations \
                        WHERE reservation_id = ?"
            self.cur.execute(db_query, (reservation_id,))
            reservation_data = self.cur.fetchone()
            return(0, reservation_data)
        except sqlite3.Error:
            print("get_reservation: error retrieving reservation \
                   [reservation_id: 0x016xd]" % (reservation_id))
            return(-1, None)

    def get_reservation_all(self):
        """ Retrieve all reservations from "shelf_reservations" table.
            Input---
              None
            Output---
              status: success = 0
                      failure = -1
              reservation_data: success = list of reservation data
                                failure = None
        """
        print("get all reservations from db")
        try:
            db_query = "SELECT * FROM shelf_reservations"
            self.cur.execute(db_query)
            reservation_data = self.cur.fetchall()
            return(0, reservation_data)
        except sqlite3.Error:
            print("get_reservation_all: error retrieving all reservations")
            return(-1, None)

    def modify_reservation(self, reservation_data):
        """ Modify reservation data in "shelf_reservations" table.
            Input---
              shelf_data - list of new reservation data
            Output---
              status: success = 0
                      failure = -1
        """
        print("modify reservation in db:", reservation_data)
        reservation_id, shelf_handle, date_reserved = (reservation_data)
        try:
            db_query = """
                UPDATE shelf_reservations SET
                shelf_handle = ?,
                date_reserved = ?
                WHERE reservation_id = ?
                """
            self.cur.execute(db_query, (shelf_handle,
                             date_reserved, reservation_id))
            self.con.commit()
            return(0)
        except sqlite3.Error:
            print("modify_reservation: error modifying \
                   existing reservation data")
            return(-1)

    def delete_reservation(self, reservation_id):
        """ Delete one reservation from "shelf_reservations" table.
            Input---
              reservation_id - id of reservation to delete
            Output---
              status: success = 0
                      failure = -1
        """
        print("delete reservation from db:", reservation_id)
        try:
            db_query = "DELETE FROM shelf_reservations \
                        WHERE reservation_id = ?"
            self.cur.execute(db_query, (reservation_id,))
            return(0)
        except sqlite3.Error:
            print("delete_reservation: error deleting reservation \
                   [reservation_id: 0x016xd]" % (reservation_id))
            return(-1, None)

    #
    # firewall_managers
    #

    def create_firewall(self, firewall_data):
        """ Insert one new firewall into "firewall_managers" table.
            Input---
              firewall_data - list of firewall data to insert
            Output---
              status: success = 0
                      failure = -1
        """
        print("add firewall to db:", firewall_data)
        try:
            db_query = "INSERT INTO firewall_managers VALUES (?,?,?,?)"
            self.cur.execute(db_query, firewall_data)
            self.con.commit()
            return(0)
        except sqlite3.Error:
            print("create_firewall: error inserting new firewall manager")
            return(-1)

    def get_firewall(self, node_id):
        """ Retrieve one firewall manager from "firewall_managers" table.
            Input---
              node_id - node id of firewall manager to retrieve
            Output---
              status: success = 0
                      failure = -1
              firewall_data: success = list of firewall data
                             failure = None
        """
        print("get firewall manager from db:", node_id)
        try:
            db_query = "SELECT * FROM firewall_managers WHERE node_id = ?"
            self.cur.execute(db_query, (node_id,))
            firewall_data = self.cur.fetchone()
            return(0, firewall_data)
        except sqlite3.Error:
            print("get_firewall: error retrieving firewall \
                  [node_id: 0x016xd]" % (node_id))
            return(-1, None)

    def get_firewall_all(self):
        """ Retrieve all firewall managers from "firewall_managers" table.
            Input---
              None
            Output---
              status: success = 0
                      failure = -1
              firewall_data: success = list of firewall data
                             failure =  None
        """
        print("get all firewall managers from db")
        try:
            db_query = "SELECT * FROM firewall_managers"
            self.cur.execute(db_query)
            firewall_data = self.cur.fetchall()
            return(0, firewall_data)
        except sqlite3.Error:
            print("get_firewall_all: error retrieving all \
                   firewall managers")
            return(-1, None)

    def modify_firewall(self, firewall_data):
        """ Modify firewall manager data in "firewall_managers" table.
            Input---
              firewall_data - list of new firewall data
            Output---
              status: success = 0
                      failure =-1
        """
        print("modify firewall manager in db:", firewall_data)
        node_id, host, port, date_registered = (firewall_data)
        try:
            db_query = """
                UPDATE firewall_managers SET
                host = ?,
                port = ?,
                date_registered = ?
                WHERE node_id = ?
                """
            self.cur.execute(db_query, (host, port, date_registered, node_id))
            self.con.commit()
            return(0)
        except sqlite3.Error:
            print("modify_firewall: error modifying existing firewall data")
            return(-1)

    def delete_firewall(self, node_id):
        """ Delete one firewall manager from "firewall_managers" table.
            Input---
              node_id - node id of firewall manager to delete
            Output---
              status: success = 0
                      failure = -1
        """
        print("delete firewall manager from db:", node_id)
        try:
            db_query = "DELETE FROM firewall_managers WHERE node_id = ?"
            self.cur.execute(db_query, (node_id,))
            return(0)
        except sqlite3.Error:
            print("delete_firewall: error deleting firewall \
                  [node_id: 0x016xd]" % (node_id))
            return(-1, None)

    #
    # Testing
    #

    def check_tables(self):
        print("check_tables()")
        total_tables = 0
        table_names = []
        tables_to_ignore = ["sqlite_sequence"]
        db_query = """
            SELECT name FROM sqlite_master WHERE type='table' ORDER BY Name
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
    # Test "book" methods
    #

    # Add a single book to the database and then retrieve it
    book_id = 0x0DEAD0000000BEEF
    book_owner = 0x1010101010101010
    book_data = (book_id, 0, 0, 0, 0, book_owner)
    status_create = db.create_book(book_data)
    status_retrieve, book_info = db.get_book(book_id)
    print("create/retrieve book:")
    print("  book_id = 0x%016x" % book_id)
    print("  book_owner = 0x%016x" % book_owner)
    print("  book_data =", book_data)
    print("  status_create = %s" % status_create)
    print("  status_retrieve = %s" % status_retrieve)
    print("  book_info =", book_info)

    # Modify a single book entry in database
    book_id = 0x0DEAD0000000BEEF
    book_owner = 0x2020202020202020
    book_data = (book_id, 1, 1, 1, 1, book_owner)
    status_modify = db.modify_book(book_data)
    status_retrieve, book_info = db.get_book(book_id)
    print("modify/retrieve book:")
    print("  book_id = 0x%016x" % book_id)
    print("  book_owner = 0x%016x" % book_owner)
    print("  book_data =", book_data)
    print("  status_modify = %s" % status_modify)
    print("  status_retrieve = %s" % status_retrieve)
    print("  book_info =", book_info)

    # Delete single book entry in database
    book_id = 0x0DEAD0000000BEEF
    status_delete = db.delete_book(book_id)
    status_retrieve, book_info = db.get_book(book_id)
    print("delete/retrieve book:")
    print("  book_id = 0x%016x" % book_id)
    print("  status_delete = %s" % status_modify)
    print("  status_retrieve = %s" % status_retrieve)
    print("  book_info =", book_info)

    # Add two books to the database and then retrieve them
    book_id1 = 0x0DEAD0000000AAAA
    book_owner1 = 0x1010101010101010
    book_data1 = (book_id1, 1, 1, 1, 1, book_owner1)
    book_id2 = 0x0DEAD0000000BBBB
    book_owner2 = 0x2020202020202020
    book_data2 = (book_id2, 2, 2, 2, 2, book_owner2)
    status_create1 = db.create_book(book_data1)
    status_create2 = db.create_book(book_data2)
    status_retrieve, book_info = db.get_book_all()
    print("create/retrieve two books:")
    print("  book_id1 = 0x%016x" % book_id1)
    print("  book_owner1 = 0x%016x" % book_owner1)
    print("  book_data1 =", book_data1)
    print("  status_create1 = %s" % status_create1)
    print("  book_id2 = 0x%016x" % book_id2)
    print("  book_owner2 = 0x%016x" % book_owner2)
    print("  book_data2 =", book_data2)
    print("  status_create2 = %s" % status_create2)
    print("  status_retrieve = %s" % status_retrieve)
    for book in book_info:
        print("  book =", book)

    #
    # Test "shelf" methods
    #

    # Add a single shelf to the database and then retrieve it
    shelf_handle = 0x0DEAD0000000BEEF
    shelf_owner = 0x1010101010101010
    shelf_time = time.time()
    shelf_data = (shelf_handle, 0, 0, 0, shelf_owner, shelf_time)
    status_create = db.create_shelf(shelf_data)
    status_retrieve, shelf_info = db.get_shelf(shelf_handle)
    print("create/retrieve shelf:")
    print("  shelf_handle = 0x%016x" % shelf_handle)
    print("  shelf_owner = 0x%016x" % shelf_owner)
    print("  shelf_time = %f (%s)" % (shelf_time, time.ctime(shelf_time)))
    print("  shelf_data =", shelf_data)
    print("  status_create = %s" % status_create)
    print("  status_retrieve = %s" % status_retrieve)
    print("  shelf_info =", shelf_info)

    # Modify a single shelf entry in database
    shelf_handle = 0x0DEAD0000000BEEF
    shelf_owner = 0x2020202020202020
    shelf_time = time.time()
    shelf_data = (shelf_handle, 1, 1, 1, shelf_owner, shelf_time)
    status_modify = db.modify_shelf(shelf_data)
    status_retrieve, shelf_info = db.get_shelf(shelf_handle)
    print("modify/retrieve shelf:")
    print("  shelf_handle = 0x%016x" % shelf_handle)
    print("  shelf_owner = 0x%016x" % shelf_owner)
    print("  shelf_time = %f (%s)" % (shelf_time, time.ctime(shelf_time)))
    print("  shelf_data =", shelf_data)
    print("  status_modify = %s" % status_modify)
    print("  status_retrieve = %s" % status_retrieve)
    print("  shelf_info =", shelf_info)

    # Delete single shelf entry in database
    shelf_handle = 0x0DEAD0000000BEEF
    status_delete = db.delete_shelf(shelf_handle)
    status_retrieve, shelf_info = db.get_shelf(shelf_handle)
    print("delete/retrieve shelf:")
    print("  shelf_handle = 0x%016x" % shelf_handle)
    print("  status_delete = %s" % status_modify)
    print("  status_retrieve = %s" % status_retrieve)
    print("  shelf_info =", shelf_info)

    # Add two shelves to the database and then retrieve them
    shelf_handle = 0x0DEAD0000000AAAA
    shelf_owner1 = 0x1010101010101010
    shelf_time1 = time.time()
    shelf_data1 = (shelf_handle, 1, 1, 1, shelf_owner1, shelf_time1)
    shelf_handle = 0x0DEAD0000000BBBB
    shelf_owner2 = 0x2020202020202020
    shelf_time2 = time.time()
    shelf_data2 = (shelf_handle, 2, 2, 2, shelf_owner2, shelf_time2)
    status_create1 = db.create_shelf(shelf_data1)
    status_create2 = db.create_shelf(shelf_data2)
    status_retrieve, shelf_info = db.get_shelf_all()
    print("create/retrieve two shelves:")
    print("  shelf_handle = 0x%016x" % shelf_handle)
    print("  shelf_owner1 = 0x%016x" % shelf_owner1)
    print("  shelf_data1 =", shelf_data1)
    print("  status_create1 = %s" % status_create1)
    print("  shelf_handle = 0x%016x" % shelf_handle)
    print("  shelf_owner2 = 0x%016x" % shelf_owner2)
    print("  shelf_data2 =", shelf_data2)
    print("  status_create2 = %s" % status_create2)
    print("  status_retrieve = %s" % status_retrieve)
    for shelf in shelf_info:
        print("  shelf =", shelf)

    #
    # Test "reservation" methods
    #

    # Add a single shelf reservation to the database and then retrieve it
    reservation_id = 0x0DEAD0000000BEEF
    shelf_handle = 0x1010101010101010
    reservation_time = time.time()
    reservation_data = (reservation_id, shelf_handle, reservation_time)
    status_create = db.create_reservation(reservation_data)
    status_retrieve, reservation_info = db.get_reservation(reservation_id)
    print("create/retrieve shelf reservation:")
    print("  reservation_id = 0x%016x" % reservation_id)
    print("  shelf_handle = 0x%016x" % shelf_handle)
    print("  reservation_time = %f (%s)" %
          (reservation_time, time.ctime(reservation_time)))
    print("  reservation_data =", reservation_data)
    print("  status_create = %s" % status_create)
    print("  status_retrieve = %s" % status_retrieve)
    print("  reservation_info =", reservation_info)

    # Modify a single shelf reservation entry in database
    reservation_id = 0x0DEAD0000000BEEF
    shelf_handle = 0x2020202020202020
    reservation_time = time.time()
    reservation_data = (reservation_id, shelf_handle, reservation_time)
    status_modify = db.modify_reservation(reservation_data)
    status_retrieve, reservation_info = db.get_reservation(reservation_id)
    print("modify/retrieve shelf reservation:")
    print("  reservation_id = 0x%016x" % reservation_id)
    print("  shelf_handle = 0x%016x" % shelf_handle)
    print("  reservation_time = %f (%s)" %
          (reservation_time, time.ctime(reservation_time)))
    print("  reservation_data =", reservation_data)
    print("  status_modify = %s" % status_modify)
    print("  status_retrieve = %s" % status_retrieve)
    print("  reservation_info =", reservation_info)

    # Delete single shelf reservation entry in database
    reservation_id = 0x0DEAD0000000BEEF
    status_delete = db.delete_reservation(reservation_id)
    status_retrieve, reservation_info = db.get_reservation(reservation_id)
    print("delete/retrieve shelf reservation:")
    print("  reservation_id = 0x%016x" % reservation_id)
    print("  status_delete = %s" % status_modify)
    print("  status_retrieve = %s" % status_retrieve)
    print("  reservation_info =", reservation_info)

    # Add two shelf reservations to the database and then retrieve them
    reservation_id1 = 0x0DEAD0000000AAAA
    shelf_handle = 0x1010101010101010
    reservation_time1 = time.time()
    reservation_data1 = (reservation_id1, shelf_handle, reservation_time1)
    reservation_id2 = 0x0DEAD0000000BBBB
    shelf_handle = 0x2020202020202020
    reservation_time2 = time.time()
    reservation_data2 = (reservation_id2, shelf_handle, reservation_time2)
    status_create1 = db.create_reservation(reservation_data1)
    status_create2 = db.create_reservation(reservation_data2)
    status_retrieve, reservation_info = db.get_reservation_all()
    print("create/retrieve two shelf reservations:")
    print("  reservation_id1 = 0x%016x" % reservation_id1)
    print("  shelf_handle = 0x%016x" % shelf_handle)
    print("  reservation_data1 =", reservation_data1)
    print("  status_create1 = %s" % status_create1)
    print("  reservation_id2 = 0x%016x" % reservation_id2)
    print("  shelf_handle = 0x%016x" % shelf_handle)
    print("  reservation_data2 =", reservation_data2)
    print("  status_create2 = %s" % status_create2)
    print("  status_retrieve = %s" % status_retrieve)
    for reservation in reservation_info:
        print("  reservation =", reservation)

    #
    # Test "firewall" methods
    #

    # Add a single firewall manager to the database and then retrieve it
    node_id = 0x0DEAD0000000BEEF
    host = "foobar.hp.com"
    port = 9090
    date_registered = time.time()
    firewall_data = (node_id, host, port, date_registered)
    status_create = db.create_firewall(firewall_data)
    status_retrieve, firewall_info = db.get_firewall(node_id)
    print("create/retrieve firewall manager:")
    print("  node_id = 0x%016x" % node_id)
    print("  host = %s" % host)
    print("  port = %d" % port)
    print("  date_registered = %f (%s)" %
          (date_registered, time.ctime(date_registered)))
    print("  firewall_data =", firewall_data)
    print("  status_create = %s" % status_create)
    print("  status_retrieve = %s" % status_retrieve)
    print("  firewall_info =", firewall_info)

    # Modify a single firewall manager entry in database
    node_id = 0x0DEAD0000000BEEF
    host = "fubar.hp.com"
    port = 9191
    date_registered = time.time()
    firewall_data = (node_id, host, port, date_registered)
    status_modify = db.modify_firewall(firewall_data)
    status_retrieve, firewall_info = db.get_firewall(node_id)
    print("modify/retrieve firewall manager:")
    print("  node_id = 0x%016x" % node_id)
    print("  host = %s" % host)
    print("  port = %d" % port)
    print("  date_registered = %f (%s)" %
          (date_registered, time.ctime(date_registered)))
    print("  firewall_data =", firewall_data)
    print("  status_modify = %s" % status_modify)
    print("  status_retrieve = %s" % status_retrieve)
    print("  firewall_info =", firewall_info)

    # Delete single firewall manager entry in database
    node_id = 0x0DEAD0000000BEEF
    status_delete = db.delete_firewall(node_id)
    status_retrieve, firewall_info = db.get_firewall(node_id)
    print("delete/retrieve firewall manager:")
    print("  node_id = 0x%016x" % node_id)
    print("  firewall_data =", firewall_data)
    print("  status_delete = %s" % status_delete)
    print("  status_retrieve = %s" % status_retrieve)
    print("  firewall_info =", firewall_info)

    # Add two firewall manager entries to the database and then retrieve them
    node_id1 = 0x0DEAD0000000AAAA
    host1 = "fubar.hp.com"
    port1 = 9090
    date_registered1 = time.time()
    firewall_data1 = (node_id1, host1, port1, date_registered1)
    node_id2 = 0x0DEAD0000000BBBB
    host2 = "foobar.hp.com"
    port2 = 9191
    date_registered2 = time.time()
    firewall_data2 = (node_id2, host2, port2, date_registered2)
    status_create1 = db.create_firewall(firewall_data1)
    status_create2 = db.create_firewall(firewall_data2)
    status_retrieve, firewall_info = db.get_firewall_all()
    print("create/retrieve two firewall manager entries:")
    print("  node_id1 = 0x%016x" % node_id1)
    print("  host1 = %s" % host1)
    print("  port1 = %d" % port1)
    print("  date_registered1 = %f (%s)" %
          (date_registered1, time.ctime(date_registered1)))
    print("  status_create1 = %s" % status_create1)
    print("  node_id2 = 0x%016x" % node_id2)
    print("  host2 = %s" % host2)
    print("  port2 = %d" % port2)
    print("  date_registered2 = %f (%s)" %
          (date_registered2, time.ctime(date_registered2)))
    print("  status_create2 = %s" % status_create2)
    print("  status_retrieve = %s" % status_retrieve)
    for firewall in firewall_info:
        print("  firewall =", firewall)

    # Cleanup
    db.close()
