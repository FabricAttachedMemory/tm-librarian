#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian book data registration module.  Take an INI file that describes
# an instance of "The Machine" (node count, book size, NVM per node).  Use
# that to prepopulate all the books for the librararian DB.
#---------------------------------------------------------------------------

import os
import sys
import configparser
import argparse
from pdb import set_trace

from book_shelf_bos import TMBook, TMShelf, TMBos, TMOpenedShelves
from backend_sqlite3 import SQLite3assist

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

For autoprovisioning, only the global section is needed:

[global]
node_count = C
book_size_bytes = S
nvm_size_per_node = N

book_size_bytes and nvm_size can have multipliers K/M/G/T (binary bytes)

nvm_size and nvm_size_per_node can also have multiplier B (books)
""")
    raise SystemExit(msg)

#--------------------------------------------------------------------------
# Load and validate


def load_config(inifile):
    config = configparser.ConfigParser()
    if not config.read(os.path.expanduser(inifile)) or not config.sections():
        usage('Missing/invalid/empty config file "%s"' % inifile)

    if not config.has_section('global'):
        usage('Missing global section in config file: %s' % inifile)

    for s in config.sections():
        options = frozenset(config.options(s))
        if s == 'global':
            legal = frozenset((
                'book_size_bytes',
                'nvm_size_per_node',
                'node_count',
            ))
            required = frozenset((
                'book_size_bytes',
                'node_count',
            ))
        elif s.startswith('node'):
            legal = frozenset((
                'lza_base',
                'node_id',
                'nvm_size',
            ))
            required = frozenset((
                'lza_base',
                'node_id',
                'nvm_size',
            ))
        else:
            raise SystemExit('Illegal section "%s"' % s)
        bad = options - legal
        if not set(required).issubset(options):
            raise SystemExit(
                'Missing option(s) in [%s]: %s\nRequired options are %s' % (
                    s, ', '.join(required-options), ', '.join(required)))
        if bad:
            raise SystemExit(
                'Illegal option(s) in [%s]: %s\nLegal options are %s' % (
                    s, ', '.join(bad), ', '.join(legal)))

    return config

#--------------------------------------------------------------------------


def multiplier(instr, section, book_size_bytes=0):
    suffix = instr[-1].upper()
    if suffix not in 'BKMGT':
        usage('Illegal size multiplier "%s" in [%s]' % (suffix, section))
    rsize = int(instr[:-1])
    if suffix == 'K':
        return rsize * 1024
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

    config = load_config(inifile)

    # Get required global config items
    section = 'global'
    node_count = int(config.get(section, 'node_count'))
    book_size_bytes = multiplier(
        config.get(section, 'book_size_bytes'), section)
    K1 = 2 << 10
    M1 = 2 << 20
    if book_size_bytes < K1 or book_size_bytes > 8192 * M1:
        raise SystemExit('book_size_bytes is out of range [1K, 8G]')

    # If optional item 'nvm_size_per_node' is there, loop it out and quit.

    lqa = 0

    try:
        bytes_per_node = multiplier(
            config.get(section, 'nvm_size_per_node'), section,
            book_size_bytes)
        if bytes_per_node % book_size_bytes != 0:
            usage('[global] bytes_per_node not multiple of book size')
        books_per_node = int(bytes_per_node / book_size_bytes)
        section2books = {}
        lza = 0
        print('%d nodes, each with %d books of %d bytes == %d bytes/node' %
              (node_count, books_per_node, book_size_bytes,
               books_per_node * book_size_bytes))
        for node_id in range(1, node_count + 1):
            section = 'node%02d' % node_id
            section2books[section] = []
            print('%s @ LZA 0x%016x' % (section, lza))
            for i in range(books_per_node):
                book = TMBook(
                    id=lza,
                    lqa=lqa,
                    node_id=node_id,
                )
                section2books[section].append(book)
                lza += book_size_bytes
                lqa += 1
        return book_size_bytes, section2books

    except configparser.NoOptionError:
        pass

    # No short cuts, grind it out.
    config.remove_section(section)
    nvm_end_prev = -1
    section2books = {}
    auto_lza_base = 0
    for section in config.sections():
        print(section)
        sdata = dict(config.items(section))
        # print(sdata)
        section2books[section] = []
        node_id = int(sdata["node_id"], 16)
        nvm_size = multiplier(sdata["nvm_size"], section, book_size_bytes)
        try:
            lza_base = int(sdata["lza_base"], 16)
        except KeyError:
            lza_base = nvm_end_prev + 1

        if nvm_size % book_size_bytes != 0:
            usage("[%s] NVM size not multiple of book size" % section)

        num_books = int(nvm_size / book_size_bytes)
        nvm_end = (lza_base + (num_books * book_size_bytes) - 1)
        if nvm_end_prev >= lza_base:
            usage("[%s] NVM overlap" % section)
        nvm_end_prev = nvm_end

        for book in range(num_books):
            book_base_addr = (book * book_size_bytes) + lza_base
            tmp = TMBook(
                node_id=node_id,
                lqa=lqa,
                id=book_base_addr
            )
            section2books[section].append(tmp)
            lqa += 1

    return(book_size_bytes, section2books)

#---------------------------------------------------------------------------
# https://www.sqlite.org/lang_createtable.html#rowid (2 days later)
# INTEGER is not the same as INT in a primary key declaration.


def create_empty_db(cur):
    try:
        table_create = """
            CREATE TABLE globals (
            schema_version TEXT,
            book_size_bytes INT,
            nvm_bytes_total INT,
            books_total INT,
            nodes_total INT
            )
            """
        cur.execute(table_create)

        table_create = """
            CREATE TABLE books (
            id INTEGER PRIMARY KEY,
            lqa INT,
            node_id INT,
            allocated INT,
            attributes INT
            )
            """
        cur.execute(table_create)
        cur.commit()

        table_create = """
            CREATE TABLE shelves (
            id INTEGER PRIMARY KEY,
            creator_id INT,
            size_bytes INT,
            book_count INT,
            ctime INT,
            mtime INT,
            name TEXT
            )
            """
        cur.execute(table_create)
        cur.commit()

        # cur.execute('CREATE UNIQUE INDEX IDX_shelves ON shelves (name)')
        # cur.commit()

        table_create = """
            CREATE TABLE books_on_shelves (
            shelf_id INT,
            book_id INT,
            seq_num INT
            )
            """
        cur.execute(table_create)
        cur.commit()

        # The id will be used as the open_handle.  It's called id because
        # it matches some internal operations harcoded to that name.
        table_create = """
            CREATE TABLE opened_shelves (
            id INTEGER PRIMARY KEY,
            shelf_id INT,
            node_id INT,
            pid INT
            )
            """
        cur.execute(table_create)
        cur.commit()

        table_create = """
            CREATE TABLE shelf_xattrs (
            shelf_id INT,
            xattr TEXT,
            value TEXT
            )
            """
        cur.execute(table_create)
        cur.commit()

        cur.execute('''CREATE UNIQUE INDEX IDX_xattrs
                       ON shelf_xattrs (shelf_id, xattr)''')
        cur.commit()
    except Exception as e:
        raise SystemExit('DB operation failed at line %d: %s' % (
            sys.exc_info()[2].tb_lineno, str(e)))

    # Idiot checks
    book = TMBook()
    assert book.schema == cur.schema('books'), 'Bad schema: books'
    shelf = TMShelf()
    assert shelf.schema == cur.schema('shelves'), 'Bad schema: shelves'
    bos = TMBos()
    assert bos.schema == cur.schema('books_on_shelves'), 'Bad schema: BOS'
    opened_shelves = TMOpenedShelves()
    assert opened_shelves.schema == cur.schema(
        'opened_shelves'), 'Bad schema: opened_shelves'

#---------------------------------------------------------------------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Create database and populate books from .ini file')
    parser.add_argument(dest='ifile', help='book .ini file')
    parser.add_argument('-f', dest="force", action='store_true',
                        help='force overwrite of given database file')
    parser.add_argument('-d', dest='dfile', default=':memory:',
                        help='database file to create')
    args = parser.parse_args()

    if args.force is False and os.path.isfile(args.dfile):
        raise SystemError('database file exists: %s' % args.dfile)
    elif args.force is True and os.path.isfile(args.dfile):
        os.unlink(args.dfile)

    book_size_bytes, section2books = load_book_data(args.ifile)
    cur = SQLite3assist(db_file=args.dfile, raiseOnExecFail=True)
    create_empty_db(cur)

    nvm_bytes_total = 0
    books_total = 0
    for books in section2books.values():
        books_total += len(books)
        nvm_bytes_total += len(books) * book_size_bytes
        for book in books:
            print(book.tuple())
            cur.execute(
                'INSERT INTO books VALUES(?, ?, ?, ?, ?)', book.tuple())
        cur.commit()    # every section; about 1000 in a real TM node

    print('%d (0x%016x) total NVM bytes' % (nvm_bytes_total, nvm_bytes_total))

    cur.execute('INSERT INTO globals VALUES(?, ?, ?, ?, ?)', (
                'LIBRARIAN 0.981',
                book_size_bytes,
                nvm_bytes_total,
                books_total,
                len(section2books)))
    cur.commit()
    cur.close()

    raise SystemExit(0)
