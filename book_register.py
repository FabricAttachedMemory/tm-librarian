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

from bookshelves import TMBook, TMShelf, TMBos
from sqlcursors import SQLiteCursor

#--------------------------------------------------------------------------

def usage(msg):
    print(msg, file=sys.stderr)
    print("""INI file format:

[global]
node_count = C
book_size_bytes = S

[node01]
node_id = I
lza_base = 0xHHHHHHHHHHHHHHHH
nvm_size = N

[node02]
:

book_size_bytes and nvm_size can have multipliers M/G/T (binary bytes)

nvm_size can have additional multiplier B (books)
""")
    raise SystemExit(msg)

#--------------------------------------------------------------------------

def multiplier(instr, section, book_size_bytes=0):
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
    if not book_size_bytes:
        usage('multiplier suffix "B" not useable in [%s]' % section)
    return rsize * book_size_bytes

#--------------------------------------------------------------------------

def load_book_data(inifile):

    config = configparser.ConfigParser()

    if not config.read(os.path.expanduser(inifile)) or not config.sections():
        usage("Missing or empty config file: %s" % inifile)

    if not config.has_section("global"):
        usage("Missing global section in config file: %s" % inifile)

    nvm_end_prev = -1
    section2books = {}
    for section in config.sections():
        print(section)
        sdata = dict(config.items(section))
        # print(sdata)
        if section == "global":
            node_count = int(sdata["node_count"], 16)
            book_size_bytes = multiplier(sdata["book_size_bytes"], section)
            if node_count != len(config.sections()) - 1:  # account for globals
                usage('Section count in INI file != [global] node_count')
        else:
            section2books[section] = []
            node_id = int(sdata["node_id"], 16)
            lza_base = int(sdata["lza_base"], 16)
            nvm_size = multiplier(sdata["nvm_size"], section, book_size_bytes)

            if nvm_size % book_size_bytes != 0:
                usage("[%s] NVM size not multiple of book size" % section)

            num_books = int(nvm_size / book_size_bytes)
            nvm_end = (lza_base + (num_books * book_size_bytes) - 1)
            if not nvm_end_prev < lza_base:
                usage("[%s] NVM overlap" % section)
            nvm_end_prev = nvm_end

            for book in range(num_books):
                book_base_addr = (book * book_size_bytes) + lza_base
                book_data = (book_base_addr, node_id, 0, 0)
                print("book %s @ 0x%016x" % (book_data, book_base_addr))
                tmp = TMBook(
                    node_id=node_id,
                    id=book_base_addr
                )
                section2books[section].append(tmp)

    return(book_size_bytes, section2books)

#---------------------------------------------------------------------------

def create_empty_db(cur):

    table_create = """
        CREATE TABLE globals (
        schema_version TEXT,
        book_size_bytes INT
        )
        """
    cur.execute(table_create)

    table_create = """
        CREATE TABLE books (
        id INT PRIMARY KEY,
        node_id INT,
        allocated INT,
        attributes INT
        )
        """
    cur.execute(table_create)
    cur.commit()

    table_create = """
        CREATE TABLE shelves (
        id INT PRIMARY KEY,
        creator_id INT,
        size_bytes INT,
        book_count INT,
        open_count INT,
        ctime INT,
        mtime INT,
        name TEXT
        )
        """
    cur.execute(table_create)
    cur.commit()

    cur.execute('CREATE UNIQUE INDEX IDX_shelves ON shelves (name)')
    cur.commit()

    table_create = """
        CREATE TABLE books_on_shelves (
        shelf_id INT,
        book_id INT,
        seq_num INT
        )
        """
    cur.execute(table_create)
    cur.commit()

    table_create = """
        CREATE TABLE shelf_open (
        shelf_id INT,
        node_id INT,
        process_id INT
        )
        """
    cur.execute(table_create)
    cur.commit()

    # Idiot checks
    book = TMBook()
    assert book.schema == cur.schema('books'), 'Bad schema: books'
    shelf = TMShelf()
    assert shelf.schema == cur.schema('shelves'), 'Bad schema: shelves'
    bos = TMBos()
    assert bos.schema == cur.schema('books_on_shelves'), 'Bad schema: BOS'

#---------------------------------------------------------------------------

if __name__ == '__main__':

    force = len(sys.argv) > 1 and sys.argv[1] == '-f' and bool(sys.argv.pop(1))
    if len(sys.argv) > 2:
        fname = sys.argv[2]
        if os.path.isfile(fname):
            if not force:
                raise SystemError('%s exists' % fname)
            os.unlink(fname)
    else:
        fname = ':memory:'
    cur = SQLiteCursor(DBfile=fname)

    book_size_bytes, section2books = load_book_data(sys.argv[1])
    create_empty_db(cur)

    cur.execute('INSERT INTO globals VALUES(?, ?)',
        ('LIBRARIAN 0.98', book_size_bytes))

    for books in section2books.values():
        for book in books:
            cur.execute('INSERT INTO books VALUES(?, ?, ?, ?)', book.tuple())

    cur.commit()
    cur.close()

    raise SystemExit(0)
