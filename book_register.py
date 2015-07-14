#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian book data registration module
#---------------------------------------------------------------------------

import os
import configparser

BOOK_FILE = "./book_data.ini"


def load_book_data(args, db):
    nvm_end_prev = -1
    if args.book_file:
        print ("user specified book file: %s" % args.book_file)
        book_file = args.book_file
    else:
        print ("using default book file: %s" % BOOK_FILE)
        book_file = BOOK_FILE

    config = configparser.ConfigParser()

    if not config.read(os.path.expanduser(book_file)) or not config.sections():
        raise SystemExit("Missing or empty config file: %s" % book_file)

    if not config.has_section("global"):
        raise SystemExit("Missing global section in config file: %s" %
                         book_file)

    for section in config.sections():
        print(section)
        sdata = dict(config.items(section))
        print(sdata)
        if section == "global":
            node_cnt = int(sdata["node_cnt"], 16)
            bsize = sdata["book_size"]
            if bsize.endswith("M"):
                rsize = int(bsize[:-1])
                book_size = rsize * 1024 * 1024
            elif bsize.endswith("G"):
                rsize = int(bsize[:-1])
                book_size = rsize * 1024 * 1024 * 1024
            else:
                raise SystemExit("unknown booksize suffix: %s" % bsize)
        else:
            node_id = int(sdata["node_id"], 16)
            lza_base = int(sdata["lza_base"], 16)
            nsize = sdata["nvm_size"]
            if nsize.endswith("M"):
                rsize = int(nsize[:-1])
                nvm_size = rsize * 1024 * 1024
            elif nsize.endswith("G"):
                rsize = int(nsize[:-1])
                nvm_size = rsize * 1024 * 1024 * 1024
            else:
                raise SystemExit("unknown booksize suffix: %s" % nsize)

            if nvm_size % book_size != 0:
                raise SystemExit("nvm_size not multiple of book_size")

            num_books = int(nvm_size / book_size)
            nvm_end = (lza_base + (num_books * book_size) - 1)
            if not nvm_end_prev < lza_base:
                raise SystemExit("nvm sections overlap")
            nvm_end_prev = nvm_end

            for book in range(num_books):
                book_base_addr = (book * book_size) + lza_base
                print("book base addr: 0x%016x" % book_base_addr)
                book_data = (book_base_addr, node_id, 0, 0, book_size)
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

    book_info = db.get_book_all()
    for book in book_info:
        book_id, node_id, status, attributes, book_size = (book)
        print("  book_id = 0x%016x, node_id = 0x%016x, status = 0x%x,"
              " attributes = 0x%x, book_size = 0x%x" %
              (book_id, node_id, status, attributes, book_size))

    db.close()
