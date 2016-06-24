#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian book data registration module.  Take an INI file that describes
# an instance of "The Machine" (node count, book size, NVM per node).  Use
# that to prepopulate all the books for the librararian DB.
# Designed for "Full Rack Demo" (FRD) to be launched in the summer of 2016.
#---------------------------------------------------------------------------

import math
import os
import sys
import configparser
import json
import argparse
from pdb import set_trace

from book_shelf_bos import TMBook, TMShelf, TMBos, TMOpenedShelves
from backend_sqlite3 import SQLite3assist
from frdnode import FRDnode, FRDintlv_group, FRDFAModule
from descmgmt import DescriptorManagement as DescMgmt
from tmconfig import TMConfig, multiplier

verbose = 0

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
    for sname in config.sections():     # ignores 'DEFAULT'
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
            ))
            required = frozenset((
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

    return Gname, G, other_sections

#--------------------------------------------------------------------------


def get_intlv_group(bn, node_id, ig):
    if ig is None:
        return node_id
    return ig

#--------------------------------------------------------------------------


def get_book_id(bn, node_id, ig):
    ''' Create a book id/LZA
          [0:32]  - book offset (zeros)
          [33:45] - book number within interleave group (0 - 8191)
          [46:52] - interleave group (0 - 127)
          [53:63] - reserved (zeros)
    '''
    if ig is None:
        return (bn << DescMgmt._BOOK_SHIFT) + (node_id << DescMgmt._IG_SHIFT)
    return (bn << DescMgmt._BOOK_SHIFT) + (ig << DescMgmt._IG_SHIFT)

#--------------------------------------------------------------------------
# If optional item is there, calculate everything.  Assume it's the
# June 2016 full-rack demo (FRD) where every node gets its own IG.
# Then books per node are evenly distributed across 4 MCs.


def extrapolate(Gname, G, node_count, book_size_bytes):
    if 'nvm_size_per_node' not in G:
        return None, None
    bytes_per_node = multiplier(G['nvm_size_per_node'], Gname, book_size_bytes)
    if bytes_per_node % book_size_bytes != 0:
        usage('[%s] bytes_per_node not multiple of book size' % Gname)
    books_per_node = int(bytes_per_node / book_size_bytes)
    module_size_books = books_per_node // 4
    if module_size_books * 4 != books_per_node:
        usage('Books per node is not divisible by 4')

    FRDnodes = [ FRDnode(node_id, module_size_books=module_size_books)
                 for node_id in range(1, node_count + 1) ]
    IGs = [ FRDintlv_group(i, node.MCs) for i, node in enumerate(FRDnodes) ]

    return FRDnodes, IGs

#--------------------------------------------------------------------------


def load_book_data_ini(inifile):

    Gname, G, other_sections = load_config(inifile)

    # Get required global config items
    node_count = int(G['node_count'])
    book_size_bytes = multiplier(G['book_size_bytes'], Gname)
    if not ((2 << 10) <= book_size_bytes <= (8 * (2 << 30))):
        raise SystemExit('book_size_bytes is out of range [1K, 8G]')
    # Python 3 bin() prints a string in binary: '0b000101...'
    if sum([ int(c) for c in bin(book_size_bytes)[2:] ]) != 1:
        raise SystemExit('book_size_bytes must be a power of 2')

    FRDnodes, IGs = extrapolate(Gname, G, node_count, book_size_bytes)
    if FRDnodes is None:
        # No short cuts, grind it out for the nodes.
        FRDnodes = []
        for section in other_sections:
            sdata = dict(section.items())
            node_id = int(sdata["node_id"], 10)
            nvm_size = multiplier(sdata["nvm_size"], section, book_size_bytes)

            if nvm_size % book_size_bytes != 0:
                usage("[%s] NVM size not multiple of book size" % section)

            num_books = int(nvm_size / book_size_bytes)

            if num_books < 1:
                usage('num_books must be greater than zero')

            module_size_books = num_books // 4
            if module_size_books * 4 != num_books:
                usage('Books per node is not divisible by 4')

            newNode = FRDnode(node_id, module_size_books=module_size_books)
            FRDnodes.append(newNode)

    IGs = [ FRDintlv_group(i, node.MCs) for i, node in enumerate(FRDnodes) ]

    # Real machine hardware only has 13 bits of book number in an LZA
    for ig in IGs:
        assert ig.total_books < 8192, 'Illegal IG book count'

    books_total = 0
    for node in FRDnodes:
        books_total += sum(mc.module_size_books for mc in node.MCs)
    nvm_bytes_total = books_total * book_size_bytes
    if verbose > 0:
        print('book size = %s (%d)' % (G['book_size_bytes'], book_size_bytes))
        print('%d books == %d (0x%016x) total NVM bytes' % (
            books_total, nvm_bytes_total, nvm_bytes_total))
    cur = SQLite3assist(db_file=args.dfile, raiseOnExecFail=True)
    create_empty_db(cur)
    cur.execute('INSERT INTO globals VALUES(?, ?, ?, ?, ?)', (
                SQLite3assist.SCHEMA_VERSION,
                book_size_bytes,
                nvm_bytes_total,
                books_total,
                len(FRDnodes)))
    cur.commit()

    # Now the other tables, keep it clear..  Some of these will be used
    # "verbatim" in the Librarian, and maybe pickling is simpler.  Later :-)

    for node in FRDnodes:
        cur.execute(
            'INSERT INTO FRDnodes VALUES(?, ?, ?, ?, ?, ?)',
            (node.node_id, node.rack, node.enc, node.node, 'None', 'None'))

        cur.execute(
            'INSERT INTO SOCs VALUES(?, ?, ?, ?, ?, ?)',
            (node.node_id, 'None', FRDnode.SOC_STATUS_OFFLINE, 'None', 'None', 0))

    cur.commit()

    # That was easy.  Here's another one.
    for ig in IGs:
        if verbose > 1:
            print("InterleaveGroup: %d" % ig.num)
            for mc in ig.MCs:
                print("  %s, rawCID = %d" % (mc, mc.value))
        for mc in ig.MCs:
            cur.execute(
                'INSERT INTO FAModules VALUES(?, ?, ?, ?, ?, ?, ?)',
                (mc.node_id, ig.num, mc.module_size_books, mc.value,
                FRDFAModule.MC_STATUS_OFFLINE, 'None', (mc.module_size_books * book_size_bytes)))
    cur.commit()

    # Books are allocated behind IGs, not nodes. An LZA is a set of bit
    # fields: IG (7) | book num (13) | book offset (33) for real hardware
    # (8G books). The "id" field is now more than a simple index, it's
    # the LZA, layout:
    #   [0:32]  - book offset
    #   [33:45] - book number within interleave group (0 - 8191)
    #   [46:52] - interleave group (0 - 127)
    #   [53:63] - reserved (zeros)
    for ig in IGs:
        for igoffset in range(ig.total_books):
            lza = (ig.num << DescMgmt._IG_SHIFT) + (igoffset << DescMgmt._BOOK_SHIFT)
            if verbose > 2:
                print("lza 0x%016x, IG = %s, igoffset = %d" % (lza, ig.num, igoffset))
            cur.execute(
                'INSERT INTO books VALUES(?, ?, ?, ?, ?)',
                (lza, ig.num, igoffset, 0, 0))
        cur.commit()    # every IG

    cur.commit()
    cur.close()

#--------------------------------------------------------------------------


def load_book_data_json(jsonfile):

    config = TMConfig(jsonfile, verbose=False)

    if config.FTFY:     # or could raise SystemExit()
        print('\nAdded missing attribute(s):\n%s\n' % '\n'.join(config.FTFY))
    if config.unused_mediaControllers:
        print('MC(s) not assigned to an IG:\n%s' % '\n'.join(config.unused_mediaControllers))
        raise SystemExit('Inconsistent configuration cannot be used')

    racks = config.racks
    encs = config.enclosures
    nodes = config.nodes
    MCs = config.mediaControllers
    IGs = config.interleaveGroups

    if not ((2 << 10) <= config.bookSize <= (8 * (2 << 30))):
        raise SystemExit('book size is out of range [1K, 8G]')
    # Python 3 bin() prints a string in binary: '0b000101...'
    if sum([ int(c) for c in bin(config.bookSize)[2:] ]) != 1:
        raise SystemExit('book size must be a power of 2')

    books_total = 0
    nvm_bytes_total = 0
    for ig in IGs:
        for mc in ig.mediaControllers:
            mco = MCs[mc.coordinate]
            for item in mco:
                mem_size = multiplier(item.memorySize, 'MCs')
                ig_total_books = mem_size / config.bookSize
                # Real machine hardware only has 13 bits of book number in an LZA
                assert ig_total_books < 8192, 'Illegal IG book count'
                books_total += ig_total_books
                nvm_bytes_total += mem_size

    if verbose > 0:
        print('%d rack(s), %d enclosure(s), %d node(s), %d media controller(s), %d IG(s)' %
            (len(racks), len(encs), len(nodes), len(MCs), len(IGs)))
        print('book size = %s (%d)' % (config.bookSize, config.bookSize))
        print('%d books * %d bytes per book == %d (0x%016x) total NVM bytes' % (
            books_total, config.bookSize, nvm_bytes_total, nvm_bytes_total))

    cur = SQLite3assist(db_file=args.dfile, raiseOnExecFail=True)
    create_empty_db(cur)
    cur.execute('INSERT INTO globals VALUES(?, ?, ?, ?, ?)', (
                SQLite3assist.SCHEMA_VERSION,
                config.bookSize,
                nvm_bytes_total,
                books_total,
                len(nodes)))
    cur.commit()

    for rack in racks:
        for enc in rack.enclosures:
            for node in enc.nodes:
                if verbose > 1:
                    print("node_id: %s (mac: %s) - rack = %s / enc = %s / node = %s" %
                        (node.node_id, node.soc.macAddress, node.rack, node.enc, node.node))
                cur.execute(
                    'INSERT INTO FRDnodes VALUES(?, ?, ?, ?, ?, ?)',
                    (node.node_id, node.rack, node.enc, node.node, node.coordinate, node.serialNumber))

                cur.execute(
                    'INSERT INTO SOCs VALUES(?, ?, ?, ?, ?, ?)',
                    (node.node_id, node.soc.macAddress, FRDnode.SOC_STATUS_OFFLINE,
                    node.soc.coordinate, node.soc.tlsPublicCertificate, 0))

    cur.commit()

    for ig in IGs:
        if verbose > 1:
            print("InterleaveGroup: %d" % ig.groupId)
        for mc in ig.mediaControllers:
            mco = MCs[mc.coordinate]
            for item in mco:
                mem_size = multiplier(item.memorySize, 'mediaControllers')
                module_size_books = mem_size / config.bookSize
                if verbose > 1:
                    print("  node_id = %s, IG = %s, books = %d, rawCID = %d" %
                        (item.node_id, ig.groupId, module_size_books, item.rawCID))
                cur.execute(
                    'INSERT INTO FAModules VALUES(?, ?, ?, ?, ?, ?, ?)',
                    (item.node_id, ig.groupId, module_size_books, item.rawCID,
                    FRDFAModule.MC_STATUS_OFFLINE, item.coordinate, mem_size))
    cur.commit()

    # Books are allocated behind IGs, not nodes. An LZA is a set of bit
    # fields: IG (7) | book num (13) | book offset (33) for real hardware
    # (8G books). The "id" field is now more than a simple index, it's
    # the LZA, layout:
    #   [0:32]  - book offset
    #   [33:45] - book number within interleave group (0 - 8191)
    #   [46:52] - interleave group (0 - 127)
    #   [53:63] - reserved (zeros)
    for ig in IGs:
        total_books = 0
        mem_size = 0
        for mc in ig.mediaControllers:
            mco = MCs[mc.coordinate]
            for item in mco:
                mem_size += multiplier(item.memorySize, 'mediaControllers')
        total_books = int(mem_size / config.bookSize)
        for igoffset in range(total_books):
            lza = (ig.groupId << DescMgmt._IG_SHIFT) + (igoffset << DescMgmt._BOOK_SHIFT)
            if verbose > 2:
                print("lza 0x%016x, IG = %s, igoffset = %d" % (lza, ig.groupId, igoffset))
            cur.execute(
                'INSERT INTO books VALUES(?, ?, ?, ?, ?)',
                (lza, ig.groupId, igoffset, 0, 0))
        cur.commit()    # every IG

    cur.commit()
    cur.close()

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

        # FRD: max 80.
        # index == quadruple.
        table_create = """
            CREATE TABLE FRDnodes (
            node_id,
            rack INT,
            enc INT,
            node INT,
            coordinate TEXT,
            serialNumber TEXT
            )
            """
        cur.execute(table_create)

        # SOC
        table_create = """
            CREATE TABLE SOCs (
            node_id,
            MAC TEXT,
            status INT,
            coordinate TEXT,
            tlsPublicCertificate TEXT,
            heartbeat INT
            )
            """
        cur.execute(table_create)

        # FRD: 4 per node. CID is raw descriptor table encoded value
        table_create = """
            CREATE TABLE FAModules (
            node_id INT,
            IG INT,
            module_size_books INT,
            rawCID INT,
            status INT,
            coordinate TEXT,
            memorySize INT
            )
            """
        cur.execute(table_create)

        # Book numbers are now relative to an interleave group.
        # FIXME: rename id to lza when there's a dull moment.
        table_create = """
            CREATE TABLE books (
            id INTEGER PRIMARY KEY,
            intlv_group INT,
            book_num INT,
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
            name TEXT,
            mode INT
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
    parser.add_argument(dest='cfile', help='book configuration file (.ini or .json format')
    parser.add_argument('-f', dest="force", action='store_true',
                        help='force overwrite of given database file')
    parser.add_argument('-d', dest='dfile', default=':memory:',
                        help='database file to create')
    parser.add_argument('-v', dest='verbose', default='0', type=int,
                        help='verbose output (0..n)')
    args = parser.parse_args()

    verbose = args.verbose

    if os.path.isfile(args.dfile):
        if not args.force:
            raise SystemError('database file exists: %s' % args.dfile)
        os.unlink(args.dfile)

    if not os.path.isfile(args.cfile):
        raise SystemExit('"%s" not found' % args.cfile)

    # Determine format of config file
    with open(args.cfile, 'r') as f:
        try:
            tmp_cfile = json.loads(f.read())
            load_book_data_json(args.cfile)
        except ValueError as e:
            load_book_data_ini(args.cfile)

    raise SystemExit(0)
