#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian book data registration module.  Take an INI file that describes
# an instance of "The Machine" (node count, book size, NVM per node).  Use
# that to prepopulate all the books for the librararian DB.
# Designed for "Full Rack Demo" (FRD) to be launched in the summer of 2016.
#---------------------------------------------------------------------------

import os
import sys
import configparser
import argparse
from pdb import set_trace

from book_shelf_bos import TMBook, TMShelf, TMBos, TMOpenedShelves
from backend_sqlite3 import SQLite3assist
from frdnode import FRDnode

#--------------------------------------------------------------------------


def usage(msg):
    print(msg, file=sys.stderr)
    print("""INI file format:

[global]
node_count = C
book_size_bytes = S

[node01]
node_id = I
nvm_size = N
intlv_group = G

[node02]
:

For autoprovisioning, only the global section is needed
and intlv_group will be equal to the node ID:

[global]
node_count = C
book_size_bytes = S
nvm_size_per_node = N

book_size_bytes and nvm_size can have multipliers K/M/G/T (binary bytes)

nvm_size and nvm_size_per_node can also have multiplier B (books)
""")
    raise SystemExit(msg)

#--------------------------------------------------------------------------
# Load file and validate section and option names; values are checked later.


def load_config(inifile):
    config = configparser.ConfigParser()
    if not config.read(os.path.expanduser(inifile)) or not config.sections():
        usage('Missing/invalid/empty config file "%s"' % inifile)

    Gname = 'global'
    if not config.has_section(Gname):
        usage('Missing [%s] section in config file: %s' % (Gname, inifile))

    other_sections = [ ]
    for sname in config.sections(): # ignores 'DEFAULT'
        section = config[sname]
        other_sections.append(section)
        options = frozenset([ o for o in section.keys() ])
        if sname == Gname:
            G = other_sections.pop()    # hence global and "other"
            legal = frozenset((
                'book_size_bytes',
                'nvm_size_per_node',
                'node_count',
            ))
            required = frozenset((
                'book_size_bytes',
                'node_count',
            ))
        elif sname.startswith('node'):
            legal = frozenset((
                'node_id',
                'nvm_size',
                'intlv_group',
            ))
            required = frozenset((
                'node_id',
                'nvm_size',
                'intlv_group',
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

    return Gname, G, other_sections

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


def get_intlv_group(bn, node_id, ig):
    if ig is None:
        return node_id
    return ig

#--------------------------------------------------------------------------


def get_book_id(bn, node_id, ig):
    ''' Create a book id
        [0:12]  - unique book number (0..8191) for given interleave group
        [13:19] - unique interleave group number (0..127) for given system
    '''
    if ig is None:
        return bn + (node_id << 13)
    return bn + (ig << 13)

#--------------------------------------------------------------------------
# If optional item is there, calculate everything.  Assume it's the
# default June 2016 demo where every node gets its own IG.


def extrapolate(Gname, G, node_count, book_size_bytes):
    if 'nvm_size_per_node' not in G:
        return None
    bytes_per_node = multiplier(G['nvm_size_per_node'], Gname, book_size_bytes)
    if bytes_per_node % book_size_bytes != 0:
        usage('[%s] bytes_per_node not multiple of book size' % Gname)
    books_per_node = int(bytes_per_node / book_size_bytes)
    section2books = {}
    print('%d nodes, each with %d books of %d bytes == %d bytes/node' %
          (node_count, books_per_node, book_size_bytes,
           books_per_node * book_size_bytes))
    for n in range(node_count):
        node_id = n + 1 # old school
        sname = 'node%02d' % node_id
        section2books[sname] = []
        book_num = 0
        for i in range(books_per_node):
            book = TMBook(
                id=get_book_id(book_num, node_id, None),
                node_id=node_id,
                intlv_group=get_intlv_group(book_num, node_id, None)
            )
            section2books[sname].append(book)
            book_num += 1
    return section2books

#--------------------------------------------------------------------------


def load_book_data(inifile):

    Gname, G, other_sections = load_config(inifile)

    # Get required global config items
    node_count = int(G['node_count'])
    book_size_bytes = multiplier(G['book_size_bytes'], Gname)
    if not ((2 << 10) <= book_size_bytes <= (8 * (2 << 30))):
        raise SystemExit('book_size_bytes is out of range [1K, 8G]')

    section2books = extrapolate(Gname, G, node_count, book_size_bytes)
    if section2books is not None:
        return book_size_bytes, section2books

    # No short cuts, grind it out for the nodes.
    section2books = {}
    intlv_groups = {}
    for section in other_sections:
        set_trace()
        sdata = dict(section.items())
        section2books[section.name] = []
        node_id = int(sdata["node_id"], 10)
        ig = int(sdata["intlv_group"], 10)
        nvm_size = multiplier(sdata["nvm_size"], section, book_size_bytes)

        if nvm_size % book_size_bytes != 0:
            usage("[%s] NVM size not multiple of book size" % section)

        num_books = int(nvm_size / book_size_bytes)
        if num_books < 1:
            usage('num_books must be greater than zero')

        if ig in intlv_groups:
            book_num = intlv_groups[ig]
        else:
            book_num = 0
            intlv_groups.update({ig: book_num})

        for book in range(num_books):
            tmp = TMBook(
                node_id=node_id,
                id=get_book_id(book_num, node_id, ig),
                intlv_group=get_intlv_group(book_num, node_id, ig)
            )
            section2books[section].append(tmp)
            book_num += 1
            intlv_groups[ig] = book_num

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
            node_id INT,
            allocated INT,
            attributes INT,
            intlv_group INT
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
            print("(id = 0x%016x, node_id = %d, allocated = %d, "
                   "attributes = %d, intlv_group = %d)" % (book.tuple()))
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
