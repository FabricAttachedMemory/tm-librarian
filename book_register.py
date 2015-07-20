#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian book data registration module
#---------------------------------------------------------------------------

import os
import sys
import configparser
from pdb import set_trace
from bookshelves import TMBook, TMShelf
from sqlcursors import SQLiteCursor

BOOK_FILE = "./book_data.ini"


def load_book_data(inifile):
    nvm_end_prev = -1
    if inifile:
        print ("user specified book file: %s" % inifile)
    else:
        print ("using default book file: %s" % BOOK_FILE)
        inifile = BOOK_FILE

    config = configparser.ConfigParser()

    if not config.read(os.path.expanduser(inifile)) or not config.sections():
        raise SystemExit("Missing or empty config file: %s" % inifile)

    if not config.has_section("global"):
        raise SystemExit("Missing global section in config file: %s" %
                         inifile)

    section2books = {}
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
                raise ValueError("unknown booksize suffix: %s" % bsize)
        else:
            section2books[section] = []
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
                raise ValueError("unknown booksize suffix: %s" % nsize)

            if nvm_size % book_size != 0:
                raise ValueError("nvm_size not multiple of book_size")

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
                tmp = TMBook(
                    node_id=node_id,
                    book_id=book_base_addr
                )
                section2books[section].append(tmp)

    return(book_size, section2books)

#---------------------------------------------------------------------------


def create_empty_db(cur):

    table_create = """
        CREATE TABLE globals (
        size_bytes INT
        )
        """
    cur.execute(table_create)

    table_create = """
        CREATE TABLE books (
        book_id INT PRIMARY KEY,
        node_id INT,
        allocated INT,
        attributes INT
        )
        """
    cur.execute(table_create)
    assert TMBook.schema() == cur.schema('books'), 'Bad schema for books'

    table_create = """
        CREATE TABLE shelves (
        shelf_id INT PRIMARY KEY,
        creator_id INT,
        size_bytes INT,
        book_count INT,
        open_count INT,
        c_time REAL,
        m_time REAL,
        name TEXT
        )
        """
    cur.execute(table_create)
    assert TMShelf.schema() == cur.schema('shelves'), 'Bad schema for shelves'

    table_create = """
        CREATE TABLE books_on_shelf (
        shelf_id INT,
        book_id INT,
        seq_num INT
        )
        """
    cur.execute(table_create)

    cur.commit()

    # Idiot checks

#---------------------------------------------------------------------------

if __name__ == '__main__':

    book_size, section2books = load_book_data(sys.argv[1])

    if len(sys.argv) > 2:
        fname = sys.argv[2]
        assert not os.path.isfile(fname), '%s already exists' % fname
    else:
        fname = ':memory:'
    cur = SQLiteCursor(DBfile=fname)

    create_empty_db(cur)

    cur.execute('INSERT INTO globals VALUES(?)', book_size)

    for books in section2books.values():
        for book in books:
            cur.execute('INSERT INTO books VALUES(?, ?, ?, ?)', book.tuple())

    cur.commit()
    cur.close()

    raise SystemExit(0)
