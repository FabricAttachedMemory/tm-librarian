#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian book data registration module.  Take an INI file that describes
# an instance of "The Machine" (node count, book size, NVM per node).  Use
# that to prepopulate all the books for the librararian DB.
#---------------------------------------------------------------------------

import os
import sys
import configparser
from pdb import set_trace
from bookshelves import TMBook, TMShelf
from sqlcursors import SQLiteCursor

BOOK_FILE = "./book_data.ini"

def usage(msg):
    print(msg, file=sys.stderr)
    print("""INI file format:

[global]
node_count = C
book_size = S

[node01]
node_id = I
lza_base = 0xHHHHHHHHHHHHHHHH
nvm_size = N

[node02]
:

Book_size and NVM_size can have multipliers M/G/T (binary bytes)

NVM_size can have additional multiple B (books)
""")
    raise SystemExit(msg)

def multiplier(instr, section, book_size=0):
    suffix = instr[-1].upper()
    if suffix not in 'BMGT':
        usage('Illegal size multiplier "%s" in [%s]' % (suffix, section))
    rsize = int(instr[:-1])
    if suffix == 'M':
        return rsize * 1024 * 1024
    elif suffix == 'G':
        return rsize * 1024 * 1024 * 1024
    elif suffix == 'T':
        return rsize * 1024 * 1024 * 1024 * 1024

    # Suffix is 'B' to reach this point
    if not book_size:
        usage('multiplier suffix "B" not useable in [%s]' % section)
    return rsize * book_size

def load_book_data(inifile):
    nvm_end_prev = -1
    if inifile:
        print ("user specified book file: %s" % inifile)
    else:
        print ("using default book file: %s" % BOOK_FILE)
        inifile = BOOK_FILE

    config = configparser.ConfigParser()

    if not config.read(os.path.expanduser(inifile)) or not config.sections():
        usage("Missing or empty config file: %s" % inifile)

    if not config.has_section("global"):
        usage("Missing global section in config file: %s" % inifile)

    section2books = {}
    for section in config.sections():
        print(section)
        sdata = dict(config.items(section))
        # print(sdata)
        if section == "global":
            node_count = int(sdata["node_count"], 16)
            book_size = multiplier(sdata["book_size"], section)
            if node_count != len(config.sections()) - 1:  # account for globals
                usage('Section count in INI file != [global] node_count')
        else:
            section2books[section] = []
            node_id = int(sdata["node_id"], 16)
            lza_base = int(sdata["lza_base"], 16)
            nvm_size = multiplier(sdata["nvm_size"], section, book_size)

            if nvm_size % book_size != 0:
                usage("[%s] nvm_size not multiple of book_size" % section)

            num_books = int(nvm_size / book_size)
            nvm_end = (lza_base + (num_books * book_size) - 1)
            if not nvm_end_prev < lza_base:
                usage("[%s] NVM overlap" % section)
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
