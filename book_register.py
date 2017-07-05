#!/usr/bin/python3 -tt

# Copyright 2017 Hewlett Packard Enterprise Development LP

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2 as
# published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along
# with this program.  If not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

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
import time
import stat

from collections import OrderedDict
from pdb import set_trace

from book_shelf_bos import TMBook, TMShelf, TMBos, TMOpenedShelves
from backend_sqlite3 import SQLite3assist
from frdnode import FRDnode, FRDintlv_group, FRDFAModule
from tmconfig import TMConfig, multiplier

verbose = 0

_BOOK_SHIFT = 33   # Bits of offset for 20 bit book number
_IG_SHIFT = 46     # Bits of offset for 7 bit IG

#--------------------------------------------------------------------------


def usage(msg):
    print(msg, file=sys.stderr)
    print("""INI file format:

[global]
node_count = C
book_size_bytes = S
datacenter = stringwithoutslashes
rack = stringwithoutslashes
domain = have.it.your.way

[hostname1]
node_id = I
nvm_size = N

[hostname2]
:

hostnameX should usually be nodeYY where YY matches node_id.
For autoprovisioning, only the global section is needed
with one extra parameter "nvm_size_per_node":

[global]
nvm_size_per_node = N
node_count = C
book_size_bytes = S
datacenter = stringwithoutslashes
rack = stringwithoutslashes
domain = have.it.your.way

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
                'datacenter',
                'rack',
                'domain'
            ))
            required = frozenset((
                'book_size_bytes',
                'node_count',
            ))
        elif sname.startswith('enclosure'):
            legal = frozenset(('u',))
            required = frozenset(('u',))
        else:   # treat sname as a node hostname
            legal = frozenset(('node_id', 'nvm_size',))
            required = frozenset(('node_id', 'nvm_size',))
        bad = options - legal
        if not set(required).issubset(options):
            raise SystemExit(
                'Missing option(s) in [%s]: %s\nRequired options are %s' % (
                    sname, ', '.join(required-options), ', '.join(required)))
        if bad:
            raise SystemExit(
                'Illegal option(s) in [%s]: %s\nLegal options are %s' % (
                    sname, ', '.join(bad), ', '.join(legal)))

    return Gname, G, other_sections

#--------------------------------------------------------------------------


def get_book_id(bn, node_id, ig):
    ''' Create a book id/LZA
          [0:32]  - book offset (zeros)
          [33:45] - book number within interleave group (0 - 8191)
          [46:52] - interleave group (0 - 127)
          [53:63] - reserved (zeros)
    '''
    if ig is None:
        return (bn << _BOOK_SHIFT) + (node_id << _IG_SHIFT)
    return (bn << _BOOK_SHIFT) + (ig << _IG_SHIFT)

#--------------------------------------------------------------------------
# If optional item is there, calculate everything.  Assume it's the
# June 2016 full-rack demo (FRD) where every node gets its own IG.
# Then books per node are evenly distributed across 4 media controllers.


def extrapolate(Gname, G, node_count, book_size_bytes):
    if 'nvm_size_per_node' not in G:
        return None
    bytes_per_node = multiplier(G['nvm_size_per_node'], Gname, book_size_bytes)
    if bytes_per_node % book_size_bytes != 0:
        usage('[%s] bytes_per_node not multiple of book size' % Gname)
    books_per_node = int(bytes_per_node / book_size_bytes)
    module_size_books = books_per_node // 4
    if module_size_books * 4 != books_per_node:
        usage('Books per node is not divisible by 4')

    FRDnodes = [ FRDnode(node_id, module_size_books=module_size_books)
                 for node_id in range(1, node_count + 1) ]
    IGs = [ FRDintlv_group(i, node.mediaControllers) for
            i, node in enumerate(FRDnodes) ]

    return FRDnodes

#--------------------------------------------------------------------------


def createDB(book_size_bytes, nvm_bytes_total, nodes, IGs):

    _MODE_DEFAULT_DIR= stat.S_IFDIR + 0o777

    books_total = nvm_bytes_total // book_size_bytes
    cur = SQLite3assist(db_file=args.dfile, raiseOnExecFail=True)
    create_empty_db(cur)
    cur.execute('INSERT INTO globals VALUES(?, ?, ?, ?, ?)', (
                SQLite3assist.SCHEMA_VERSION,
                book_size_bytes,
                nvm_bytes_total,
                books_total,
                len(nodes)))
    cur.commit()

    # Now the other tables, keep it clear.  Some of these will be used
    # "verbatim" in the Librarian, and maybe pickling is simpler.  Later :-)
    # In loops, print as you go so crashes give you a clue.

    for node in nodes:
        if verbose > 1:
            print('node_id: %s (mac: %s) - rack = %s / enc = %s / node = %s' %
                  (node.node_id,      # 1-80
                   node.soc.macAddress,
                   node.rack,
                   node.enc,
                   node.node))

        cur.execute(
            'INSERT INTO FRDnodes VALUES(?, ?, ?, ?, ?, ?)',
            (node.node_id,
             node.rack,
             node.enc,
             node.node,
             node.coordinate,
             node.serialNumber))

        cur.execute(
            'INSERT INTO SOCs VALUES(?, ?, ?, ?, ?, ?)',
            (node.node_id,
             node.soc.macAddress,
             FRDnode.SOC_STATUS_OFFLINE,
             node.soc.coordinate,
             node.soc.tlsPublicCertificate,
             0))  # heartbeat
    cur.commit()

    # Interleave Groups and MCs.  The FRDnode MC structures are too "isolated"
    # and can't compute mc.memorySize without passing in book_size, and that
    # seems too clunky.  Compute it here (composition pattern).
    # FIXME: do we actually use mc.memorySize anywhere (probably LMP) or
    # was it just a good idea at the time?
    # INI:  type(mc) == frdnode.FRDFAModule
    # JSON: type(mc) == tmconfig.mediaControllers
    for ig in IGs:
        if verbose > 1:
            print("InterleaveGroup: %d" % ig.groupId)
        for mc in ig.mediaControllers:
            if isinstance(mc, FRDFAModule):
                assert not hasattr(mc, 'memorySize'), 'Must have fixed it'
                mc.memorySize = mc.module_size_books * book_size_bytes

            if verbose > 1:
                print('  node_id %2d: %d books, rawCID = %d' %
                      (mc.node_id, mc.module_size_books, mc.rawCID))
            cur.execute(
                'INSERT INTO FAModules VALUES(?, ?, ?, ?, ?, ?, ?)',
                (mc.node_id,
                 ig.groupId,
                 mc.module_size_books,
                 mc.rawCID,
                 FRDFAModule.MC_STATUS_OFFLINE,
                 mc.coordinate,
                 mc.memorySize))
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
            lza = ((ig.groupId << _IG_SHIFT) +
                   (igoffset << _BOOK_SHIFT))
            if verbose > 2:
                print("lza 0x%016x, IG = %s, igoffset = %d" % (
                    lza, ig.groupId, igoffset))
            cur.execute(
                'INSERT INTO books VALUES(?, ?, ?, ?, ?)',
                (lza, ig.groupId, igoffset, 0, 0))
        cur.commit()    # every IG

    # add the initial directory shelves
    tmp = int(time.time())
    # first a garbage shelf to make root id = 2
    cur.execute(
        'INSERT INTO shelves VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (1, 0, 0, 0, tmp, tmp, "garbage", _MODE_DEFAULT_DIR)) # name cant be empty string

    cur.commit()

    # and then root directory
    cur.execute(
        'INSERT INTO shelves VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (2, 0, 0, 0, tmp, tmp, ".", _MODE_DEFAULT_DIR))


    cur.commit()
    cur.close()
    return True

#--------------------------------------------------------------------------


def INI_to_JSON(G, book_size_bytes, FRDnodes, IGs, enc2U):
    # Reconstruct top coordinate from an MC which has full thing.  Since IGs
    # are always autogenerated this could be useless paranoia...but I like it.
    if not IGs:
        print('No IGs, no master coordinate, no JSON for you')
        return
    datacenter = G.get('datacenter', os.uname()[1])
    rack = G.get('rack', 'FAME')
    domain = G.get('domain', 'have.it.your.way')
    torms = 'torms.%s' % domain

    datacenter = '/MachineVersion/1/Datacenter/%s' % datacenter
    rackcoord = 'Rack/%s' % rack

    # The book size is specified in the Librarian service.
    bigun = OrderedDict([
        ('_comment', 'transmogrified from file "%s" on %s' % (
            args.cfile, time.ctime())),
        ('coordinate', datacenter),
        ('domains',
            OrderedDict([
                ('_comment', 'See Manifesting README for FAME extensions'),
                ('localData', 'N/A for FAME'),
                ('publicData', domain),
                ('management', 'N/A for FAME')
            ])
        ),
        ('servers', [
            OrderedDict([
                ('coordinate', rackcoord),  # why not
                ('ipv4Address', torms),
                ('services', [
                    {
                        'service': 'librarian',
                        'bookSize': book_size_bytes,
                        'port': 9093,
                        'tlsPublicCertificate': 'nada',
                        'restUri': 'http://%s:31179/lmp' % torms,
                    },
                    {
                        'service': 'osManifesting',
                        'port': 31178,
                        'tlsPublicCertificate': 'nada',
                        'restUri': 'http://%s:31178/manifesting/api' % torms,
                    },
                    {
                        'service': 'assemblyAgent',
                        'tlsPublicCertificate': 'nada',
                        'restUri': 'http://%s:9097/dma' % torms,
                    },
                    {
                        'service': 'monitoring',
                        'tlsPublicCertificate': 'nada',
                        'restUri': 'http://%s:5050/rest' % torms,
                    },
                ]),
            ]),
        ]),
    ])

    # Racks are simple, it's a list of 1 item in FRD
    bigun['racks'] = [
        OrderedDict([
            ('coordinate', rackcoord),
            ('enclosures', [])
        ]),
    ]
    theRack = bigun['racks'][0]

    # Implicitly iterate over enclosures by watching "enclosure" field while
    # explicitly iterating over nodes
    thisenc = None
    thisencnum = -1
    for node in FRDnodes:   # Assuming sorted by increasing node_id
        if node.enc != thisencnum:
            thisencnum = node.enc
            if thisenc is not None:
                theRack['enclosures'].append(thisenc)
            thisenc = OrderedDict([
                ('coordinate', 'Enclosure/%s/EncNum/%d' % (
                    enc2U[thisencnum], thisencnum)),
                ('iZoneBoards',[    # start of a list comprehension
                    OrderedDict([
                        ('coordinate', 'IZone/1/IZoneBoard/%d' % izzy),
                        ('izBoardMp', dict((
                            ('coordinate', 'IZoneBoardMp/1'),
                            ('msCollector', 'switchmp'),
                            ('ipv4Address', '10.254.%d.%d' %
                                (thisencnum, 100 + izzy)),
                            ('mfwApiUri', 'http://10.254.%d.%d:8081/redfish/v1' %
                                (thisencnum, 100 + izzy)),
                        ))),
                    ])
                    for izzy in (1, 2)
                ]),
                ('nodes', [])
            ])
        thisenc['nodes'].append(OrderedDict([
            ('coordinate', 'Node/%d' % node.node),
            ('serialNumber', 'nada'),
            ('nodeMp', {
                'coordinate': 'SocBoard/1/MpDaughterCard/1/NodeMp/1',
                'msCollector': 'nodemp',
                'ipv4Address': '10.254.%d.%d' % (thisencnum, 200 + node.node),
                'mfwApiUri': 'http://10.254.%d.%d:8081/redfish/v1' % (
                    thisencnum, 200 + node.node),
            }),
            ('soc', {
                'coordinate': 'SocBoard/1/Soc/1',
                'hostname': node.hostname,
                'tlsPublicCertificate': 'nada',
            }),
            ('mediaControllers', [  # start of a list comprehension
                OrderedDict([
                    ('coordinate',
                     'MemoryBoard/1/MediaController/%d' % (n + 1)),
                    ('memorySize', mc.memorySize)
                ])
                for n, mc in enumerate(node.mediaControllers)
            ])
        ]))

    theRack['enclosures'].append(thisenc)    # No enclosure left behind

    # Finish the JSON.  IGs is an array of frdnode.py::FRDFAModules.
    # Expand constituent coordinates into an absolute coordinate cuz that's
    # how it's defined.  Unroll previous attempts at list comprehension to
    # get more inner values.
    rackprefix = '%s/%s' % (datacenter, rackcoord)
    bigun['interleaveGroups'] = []
    for ig in IGs:
        mclist = []
        for mc in ig.mediaControllers:
            abscoord = '%s/Enclosure/%s/EncNum/%d/Node/%d/%s' % (
                rackprefix, enc2U[mc.enc], mc.enc, mc.node, mc.coordinate)
            mclist.append(abscoord)
        bigun['interleaveGroups'].append(
            OrderedDict([
                ('groupId', ig.groupId),
                ('mediaControllers', mclist)
            ])
        )
    print(json.dumps(bigun, indent=4))

#--------------------------------------------------------------------------


def load_book_data_ini(inifile):

    try:
        Gname, G, other_sections = load_config(inifile)
    except Exception as e:
        if verbose:
            print('Not an INI file:', str(e), file=sys.stderr)
        return False

    # Get and validate the required global config items
    node_count = int(G['node_count'])
    book_size_bytes = multiplier(G['book_size_bytes'], Gname)
    if not ((2 << 10) <= book_size_bytes <= (8 * (2 << 30))):
        raise SystemExit('book_size_bytes is out of range [1K, 8G]')
    # Python 3 bin() prints a string in binary: '0b000101...'
    if sum([ int(c) for c in bin(book_size_bytes)[2:] ]) != 1:
        raise SystemExit('book_size_bytes must be a power of 2')

    # The enclosure "U" mounting location in the rack.  Defaults will be
    # supplied later if there are no "[enclosureX]" sections.  The use
    # is split across blocks because not all code paths maintain an
    # explicit enclosure object.

    enc2U = {}

    FRDnodes = extrapolate(Gname, G, node_count, book_size_bytes)
    if FRDnodes is None:    # Grind it out section by section.
        FRDnodes = []
        for section in other_sections:
            if section.name.startswith('enclosure'):
                assert not FRDnodes, \
                    '[%s] must appear before any [node] section' % section.name
                try:
                    encnum = int(section.name[-1])
                    assert encnum not in enc2U, \
                        'Duplicate section [%s]' % section.name
                except ValueError as e:
                    raise RuntimeError('Bad section [%s]' % section.name)
                Uvalue = dict(section.items())['u']
                assert ' ' not in Uvalue and '/' not in Uvalue, \
                    'Illegal character [%s] U-value' % section.name
                enc2U[encnum] = Uvalue
                continue

            hostname = section.name
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
            if hostname.startswith('node'):     # force the default
                assert newNode.hostname == hostname, \
                    'Bad "nodeXX" format: %s' % hostname
            else:
                newNode.hostname = hostname
            FRDnodes.append(newNode)

    # Add default enclosure U-values if needed, regardless of INI form.
    for node in FRDnodes:
        if node.enc in enc2U:
            continue
        enc2U[node.enc] = 'UV'  # Virtual, like FAME

    IGs = [ FRDintlv_group(node.node_id - 1, node.mediaControllers) for
            node in FRDnodes ]

    books_total = 0
    for node in FRDnodes:
        books_total += sum(
            mc.module_size_books for mc in node.mediaControllers)
    nvm_bytes_total = books_total * book_size_bytes
    if verbose:
        print('book size = %d' % (book_size_bytes))
        print('%d books == %d (0x%016x) total NVM bytes' % (
            books_total, nvm_bytes_total, nvm_bytes_total))

    if not createDB(book_size_bytes, nvm_bytes_total, FRDnodes, IGs):
        return False

    if args.json:
        INI_to_JSON(G, book_size_bytes, FRDnodes, IGs, enc2U)

    return True

#--------------------------------------------------------------------------


def load_book_data_json(jsonfile):

    with open(args.cfile, 'r') as f:
        try:
            tmp_cfile = json.loads(f.read())    # Is it JSON?
        except ValueError as e:
            if verbose:
                print('Not a JSON file:', str(e), file=sys.stderr)
            return False
    try:
        config = TMConfig(jsonfile, verbose=False)
    except Exception as e:
        print('Not a JSON TMCF file:', str(e), file=sys.stderr)
        return False

    if config.errors:
        print('Errors:\n%s' % '\n'.join(config.errors))
        raise SystemExit('Illegal configuration cannot be used')
    if config.unused_mediaControllers:
        print('MC(s) not assigned to an IG:\n%s' %
              '\n'.join(config.unused_mediaControllers))
        raise SystemExit('Inconsistent configuration cannot be used')
    if config.FTFY:     # or could raise SystemExit()
        print('\nAdded missing attribute(s):\n%s\n' % '\n'.join(config.FTFY))

    # config.allXXXX are shortcut summaries that do not account for
    # physical or logical slot placement.   Walk the structure
    # config.racks[X].enclosures.populated or
    # config.racks[X].enclosures[Y].nodes.populated for that.
    allRacks = config.allRacks
    allEnclosures = config.allEnclosures
    allNodes = config.allNodes
    allMCs = config.allMediaControllers
    IGs = config.interleaveGroups

    if not ((2 << 10) <= config.bookSize <= (8 * (2 << 30))):
        raise SystemExit('book size is out of range [1K, 8G]')
    # Python 3 bin() prints a string in binary: '0b000101...'
    if sum([ int(c) for c in bin(config.bookSize)[2:] ]) != 1:
        raise SystemExit('book size must be a power of 2')

    if verbose:
        print('%d rack(s), %d enclosure(s), %d node(s), %d MC(s), %d IG(s)' %
              (len(allRacks), len(allEnclosures), len(allNodes),
               len(allMCs), len(IGs)))
        print('book size = %s (%d)' % (config.bookSize, config.bookSize))
        books_total = config.totalNVM / config.bookSize
        print('%d books * %d bytes/book == %d (0x%016x) total NVM bytes' % (
            books_total, config.bookSize, config.totalNVM, config.totalNVM))

    return createDB(config.bookSize, config.totalNVM, allNodes, IGs)

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
    parser.add_argument(dest='cfile',
                        help='book configuration file (.ini or .json format')
    parser.add_argument('-f', dest="force", action='store_true',
                        help='force overwrite of given database file')
    parser.add_argument('-d', dest='dfile', default=':memory:',
                        help='database file to create')
    parser.add_argument('-j', dest="json", action='store_true',
                        help='for INI files, emit JSON equivalent to stdout')
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
    if not (load_book_data_json(args.cfile) or load_book_data_ini(args.cfile)):
        usage('unrecognized file format')
        raise SystemExit('Bogus source file, dude')

    raise SystemExit(0)
