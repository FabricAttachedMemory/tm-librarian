#!/usr/bin/python3
#---------------------------------------------------------------------------
# Librarian book data registration module
#---------------------------------------------------------------------------

import os
import configparser

BOOK_FILE = "./book_data.ini"
BOOK_SIZE = (1024*1024*1024*8)


def load_book_data(args, db):
    if args.book_file:
        print ("user specified book file: %s" % args.book_file)
        book_file = args.book_file
    else:
        print ("using default book file: %s" % BOOK_FILE)
        book_file = BOOK_FILE

    config = configparser.ConfigParser()
    if not config.read(os.path.expanduser(book_file)) or not config.sections():
        raise SystemExit("Missing or empty config file: %s" % book_file)

    for section in config.sections():
        print(section)
        node = dict(config.items(section))
        print(node)
        node_id = int(node["node_id"], 16)
        base_addr = int(node["base_addr"], 16)
        num_books = int(node["num_books"])
        for book in range(num_books):
            book_base_addr = (book * BOOK_SIZE) + base_addr
            print("book base addr: 0x%016x" % book_base_addr)
            book_data = (book_base_addr, 0, 0, 0, 0, 0)
            print("insert book into db:", book_data)
            db.create_book(book_data)


def book_data_args_init(parser):
    parser.add_argument("--book_file",
                        help="specify the book data file, (default = %s)"
                        % (BOOK_FILE))

if __name__ == '__main__':

    import argparse
    import database

    parser = argparse.ArgumentParser()
    database.db_args_init(parser)
    book_data_args_init(parser)
    args = parser.parse_args()

    # Initialize database and check tables
    db = database.LibrarianDB()
    db.db_init(args)
    db.check_tables()

    load_book_data(args, db)

    status, book_info = db.get_book_all()
    for book in book_info:
        print("  book =", book)

    db.close()

# todo: check book offsets to see if they are all unique
# todo: check base_addr values are unique
# todo: check num_books value is less than a max value "MAX_NUM_BOOKS"
