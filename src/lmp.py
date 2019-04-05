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

# Dependencies:
# - python3-flask
# - librarian DB must be created from json configuration file

import errno
import logging
import os
import stat
import sys
import time

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from flask import Flask, render_template, jsonify, request, g
from pdb import set_trace

from backend_sqlite3 import SQLite3assist
from book_shelf_bos import TMBook, TMShelf, TMBos
from frdnode import FRDnode, FRDFAModule

os.chdir(os.path.dirname(os.path.realpath(__file__)))

mainapp = Flask('tm_lmp', static_url_path='/static')
mainapp.config.from_object('lmp_config')
mainapp.config['API_VERSION'] = 1.0
mainapp.cur = None

###########################################################################
# Format an error code


def _response_bad(errmsg, status_code=418):
    response = jsonify({'error': errmsg})
    response.status_code = status_code
    return response

###########################################################################
# Execute before each request to check version


@mainapp.before_request
def check_version(*args, **kwargs):
    if mainapp.cur is None:
        mainapp.cur = SQLite3assist(
            db_file=mainapp.db_file, raiseOnExecFail=True, ro=True)
    if not requestor_wants_json(request):  # Ignore versioning for HTML
        return None
    hdr_accept = request.headers['Accept']
    version = -1.0
    for elem in hdr_accept.split(';'):
        if 'version' in elem:
            try:
                version = float(elem.split('=')[-1])
                break
            except Exception as e:
                pass
    if version < 0:
        return _response_bad('No version sent')
    want = mainapp.config['API_VERSION']
    if version != want:
        return _response_bad('Bad version: %s != %s' % (version, want))

###########################################################################
# Execute after every request to fix up headers


@mainapp.after_request
def version(response):
    response.headers['Content-Type'] += ';charset=utf-8'
    response.headers['Content-Type'] += ';version=%s' % \
        mainapp.config['API_VERSION']
    return response

###########################################################################
# Check if requestor wants json formatted reply.  Now that grids updater
# calls without context, return True if it's not a "real" request.


def requestor_wants_json(request):
    try:
        return 'application/json' in request.headers['Accept']
    except Exception as e:
        return True

###########################################################################
# Convert Librarian books status to LMP equivalent


def convert_book_status(status):
    if status == TMBook.ALLOC_FREE:
        return 'available'
    elif status == TMBook.ALLOC_INUSE:
        return 'allocated'
    elif status == TMBook.ALLOC_ZOMBIE:
        return 'notready'
    elif status == TMBook.ALLOC_OFFLINE:
        return 'offline'
    else:
        return 'unknown'


###########################################################################
# View: /lmp - (root) list views available


@mainapp.route('/lmp/')
def show_views():
    if requestor_wants_json(request):
        return _response_bad("Root view not implemented", 404)

    return render_template(
        'show_views.html',
        url=request.url,
        api_version=mainapp.config['API_VERSION'])


###########################################################################
# View: /global - Global Memory Information
# Desc: list global memory usage information


@mainapp.route('/lmp/global/')
def show_global():
    try:
        cur = mainapp.cur
        cur.execute('SELECT book_size_bytes FROM globals')
        cur.iterclass = None
        b_size = cur.fetchone()[0]
        cur.execute('SELECT books_total FROM globals')
        m_total = cur.fetchone()[0] * b_size
        cur.execute('''
            SELECT COUNT(*) FROM books
            WHERE allocated=?''', TMBook.ALLOC_FREE)
        m_free = cur.fetchone()[0] * b_size
        cur.execute('''
            SELECT COUNT(*) FROM books
            WHERE allocated=?''', TMBook.ALLOC_INUSE)
        m_inuse = cur.fetchone()[0] * b_size
        cur.execute('''
            SELECT COUNT(*) FROM books
            WHERE allocated=?''', TMBook.ALLOC_ZOMBIE)
        m_zombie = cur.fetchone()[0] * b_size
        cur.execute('''
            SELECT COUNT(*) FROM books
            WHERE allocated=?''', TMBook.ALLOC_OFFLINE)
        m_offline = cur.fetchone()[0] * b_size

        d_memory = {
            'total': m_total,
            'available': m_free,
            'allocated': m_inuse,
            'notready': m_zombie,
            'offline': m_offline }

        cur.execute('SELECT COUNT(*) FROM FRDnodes')
        s_total = cur.fetchone()[0]

        ts = int(time.time() - FRDnode.SOC_HEARTBEAT_SECS)

        cur.execute('''
            SELECT COUNT(*) FROM SOCs
            WHERE status=? and heartbeat>=?''',
                    (FRDnode.SOC_STATUS_ACTIVE, ts))
        s_active = cur.fetchone()[0]

        cur.execute('''
            SELECT COUNT(*) FROM SOCs
            WHERE status=? AND heartbeat<?''',
                    (FRDnode.SOC_STATUS_ACTIVE, ts))
        s_indeterminate = cur.fetchone()[0]

        cur.execute('''
            SELECT COUNT(*) FROM SOCs
            WHERE status=?''',
                    FRDnode.SOC_STATUS_OFFLINE)
        s_offline = cur.fetchone()[0]

        cur.execute('SELECT AVG(cpu_percent) FROM SOCs')
        s_cpu_percent = cur.fetchone()[0]

        d_socs = {
            'total': s_total,
            'active': s_active,
            'offline': s_offline,
            'indeterminate': s_indeterminate,
            'cpu_percent': s_cpu_percent
        }

        cur.execute('SELECT COUNT(*) FROM FAModules')
        p_total = cur.fetchone()[0]
        cur.execute('''
            SELECT COUNT(*) FROM FAModules
            WHERE status=?''',
                    FRDFAModule.MC_STATUS_ACTIVE)
        p_active = cur.fetchone()[0]
        cur.execute('''
            SELECT COUNT(*) FROM FAModules
            WHERE status=?''',
                    FRDFAModule.MC_STATUS_OFFLINE)
        p_offline = cur.fetchone()[0]

        d_pools = {
            'total': p_total,
            'active': p_active,
            'offline': p_offline }

        a_shelves = 0
        a_books = 0
        cur.execute('''
            SELECT * FROM shelves JOIN opened_shelves
            ON shelves.id = opened_shelves.shelf_id''')
        cur.iterclass = 'default'
        o_shelves = [ r for r in cur ]
        for s in o_shelves:
            a_shelves += 1
            a_books += s.book_count

        d_active = {
            'shelves': a_shelves,
            'books': a_books }

    except Exception as e:
        return _response_bad('%s' % (e), 400)

    if requestor_wants_json(request):
        return jsonify(
            memory=d_memory,
            socs=d_socs,
            pools=d_pools,
            active=d_active)

    return render_template(
        'show_global.html',
        memory=d_memory,
        socs=d_socs,
        pools=d_pools,
        active=d_active,
        api_version=mainapp.config['API_VERSION'])

###########################################################################
# View: /nodes - List of Nodes
# Desc: list nodes in The Machine instance managed by the Librarian


@mainapp.route('/lmp/nodes/')
def show_nodes():
    try:
        cur = mainapp.cur
        l_nodes = []
        cur.execute('SELECT * FROM FRDnodes')
        cur.iterclass = 'default'
        nodes = [ r for r in cur ]
        for n in nodes:
            cur.execute('SELECT * FROM SOCs WHERE node_id=?', n.node_id)
            cur.iterclass = 'default'
            socs = [ r for r in cur ]
            assert len(socs) == 1, 'Only 1 SOC/node can be handled'
            s = socs[0]
            # Only partial coords are returned, it's up to the caller to
            # keep track of upstack.  Add the n.data to assist matryoshka
            # which for some reason (in 2019) is expecting full coord string.
            d_soc = {
                'coordinate': s.coordinate,     # Legacy values start here
                'tlsPublicCertificate': s.tlsPublicCertificate,
                'macAddress': s.MAC,
                'cpu_percent': s.cpu_percent,
                'rootfs_percent': s.rootfs_percent,
                'network_in': s.network_in,
                'network_out': s.network_out,
                'mem_percent': s.mem_percent
            }

            cur.execute('SELECT * FROM FAModules WHERE node_id=?', n.node_id)
            cur.iterclass = 'default'
            mcs = [ r for r in cur ]
            l_mcs = []
            for m in mcs:
                d_mc = {}
                d_mc['coordinate'] = m.coordinate
                d_mc['memorySize'] = m.memorySize
                l_mcs.append(d_mc)

            # node_id added in 2017 for books_allocated_demo; others added
            # in 2019 for matryoshka.
            d_node = {
                'coordinate': n.coordinate,
                'serialNumber': n.serialNumber,
                'soc': d_soc,
                'mediaControllers': l_mcs,
                'node_id': n.node_id,       # 1-40, throughout entire rack
                'rack_num': n.rack,         # 1     MFT; will SDflex grow?
                'enc_num': n.enc,           # 1-4,  within this rack_num
                'node_num': n.node,         # 1-10, within this enc_num
            }
            l_nodes.append(d_node)

    except Exception as e:
        return _response_bad('%s' % (e), 400)

    if requestor_wants_json(request):
        return jsonify(nodes=l_nodes)

    return render_template(
        'show_nodes.html',
        nodes=l_nodes,
        api_version=mainapp.config['API_VERSION'])

###########################################################################
# View: /interleaveGroups - Memory Configuration
# Desc: list interleave groups in The Machine instance managed by the Librarian


@mainapp.route('/lmp/interleaveGroups/')
def show_interleaveGroups():
    try:
        cur = mainapp.cur
        cur.execute('SELECT * FROM FAModules')
        cur.iterclass = 'default'
        mcs = [ r for r in cur ]
        d_ig = {}
        for m in mcs:
            if m.IG in d_ig:
                d_ig[m.IG]['size'] += m.memorySize
                d_ig[m.IG]['mediaControllers'].append(m.coordinate)
            else:
                d_data = {}
                d_data['groupId'] = m.IG
                d_data['baseAddress'] = (m.IG << 46)  # lowest LZA in IG
                d_data['size'] = m.memorySize
                d_data['mediaControllers'] = [m.coordinate, ]
                d_ig[m.IG] = d_data

        l_ig = []
        for ig in d_ig:
            l_ig.append(d_ig[ig])

    except Exception as e:
        return _response_bad('%s' % (e), 400)

    if requestor_wants_json(request):
        return jsonify(interleaveGroups=l_ig)

    return render_template(
        'show_interleaveGroups.html',
        interleaveGroups=l_ig,
        api_version=mainapp.config['API_VERSION'])

###########################################################################
# View: /allocated/{coordinate} - Memory Allocation
# Desc: list allocations across The Machine
# Input: coordinate - the coordinate of a datacenter, rack, enclosure,
#                     node, memory_board or media_controller


@mainapp.route('/lmp/allocated/<path:coordinate>')
def show_allocated(coordinate):
    try:
        cur = mainapp.cur
        cur.execute('SELECT book_size_bytes FROM globals')
        cur.iterclass = None
        b_size = cur.fetchone()[0]

        # Obtain the number of MCs in each IG
        cur.execute('''SELECT * FROM FAModules''')
        cur.iterclass = 'default'
        mcs = [ r for r in cur ]
        d_ig_cnt = {}
        for m in mcs:
            if m.IG in d_ig_cnt:
                d_ig_cnt[m.IG] += 1
            else:
                d_ig_cnt[m.IG] = 1

        # One row for each MC a book is spread across
        c_filter = '/' + coordinate + '%'
        cur.execute('''SELECT * FROM books
            JOIN FAModules ON books.intlv_group = FAModules.IG
            WHERE FAModules.coordinate LIKE ?
            ''', c_filter)

        cur.iterclass = 'default'
        books = [ r for r in cur ]
        d_memory = {}

        b_total = float(0)
        b_allocated = float(0)
        b_available = float(0)
        b_notready = float(0)
        b_offline = float(0)

        for b in books:
            b_total += (b_size / d_ig_cnt[b.intlv_group])
            if b.allocated == TMBook.ALLOC_INUSE:
                b_allocated += (b_size / d_ig_cnt[b.intlv_group])
            if b.allocated == TMBook.ALLOC_FREE:
                b_available += (b_size / d_ig_cnt[b.intlv_group])
            if b.allocated == TMBook.ALLOC_ZOMBIE:
                b_notready += (b_size / d_ig_cnt[b.intlv_group])
            if b.allocated == TMBook.ALLOC_OFFLINE:
                b_offline += (b_size / d_ig_cnt[b.intlv_group])

        d_memory['total'] = int(b_total)
        d_memory['allocated'] = int(b_allocated)
        d_memory['available'] = int(b_available)
        d_memory['notready'] = int(b_notready)
        d_memory['offline'] = int(b_offline)

        if requestor_wants_json(request):
            return jsonify(
                memory=d_memory)
        return render_template(
            'show_allocated.html',
            memory=d_memory,
            api_version=mainapp.config['API_VERSION'])

    except Exception as e:
        return _response_bad('%s' % (e), 404)

###########################################################################
# View: /active/{coordinate} - Memory Activity
# Desc: list memory access by SOCs across The Machine
# Input: coordinate - the coordinate of a specfic SOC


@mainapp.route('/lmp/active/<path:coordinate>')
def show_active(coordinate):
    try:
        c_type = coordinate.split('/')[-2]
        coordinate = '/' + coordinate

        cur = mainapp.cur

        if c_type == 'Datacenter':
            cur.execute('''
                SELECT DISTINCT opened_shelves.shelf_id, shelves.book_count
                FROM opened_shelves
                JOIN shelves ON opened_shelves.shelf_id = shelves.id
                JOIN SOCs ON opened_shelves.node_id = SOCs.node_id''')
        elif c_type == 'Soc':
            cur.execute('''
                SELECT DISTINCT opened_shelves.shelf_id, shelves.book_count
                FROM opened_shelves
                JOIN shelves ON opened_shelves.shelf_id = shelves.id
                JOIN SOCs ON opened_shelves.node_id = SOCs.node_id
                WHERE SOCs.coordinate = ?''', coordinate)
        else:
            return _response_bad("Bad coordinate", 404)

        cur.iterclass = 'default'
        socs = [ r for r in cur ]
        s_shelves = 0
        s_books = 0
        d_active = {}
        for s in socs:
            s_shelves += 1
            s_books += s.book_count

        d_active['shelves'] = s_shelves
        d_active['books'] = s_books

        if requestor_wants_json(request):
            return jsonify(
                active=d_active)
        return render_template(
            'show_active.html',
            active=d_active,
            api_version=mainapp.config['API_VERSION'])

    except Exception as e:
        return _response_bad('%s' % (e), 404)

###########################################################################
# View: /shelf/{pathname} - Directory and Shelf Information
# Desc: list directory/shelf information (pathname = directory or shelf)
# Input: pathname - full path of the directory or shelf within the librarian
#                   file system, not including the /lfs prefix


@mainapp.route('/lmp/shelf/')
@mainapp.route('/lmp/shelf/<pathname>')
def show_shelf(pathname=None):
    try:
        cur = mainapp.cur

        # Root directory (for FRD this is the ONLY directory)
        if not pathname:
            d_owner = 42  # FIXME
            d_group = 42  # FIXME
            d_mode = '0777'  # FIXME

            cur.execute('SELECT * FROM shelves')
            cur.iterclass = 'default'
            shelves = [ r for r in cur ]

            l_entries = []
            for s in shelves:
                d_attr = {}
                d_attr['name'] = s.name
                d_attr['type'] = 'file'
                d_attr['owner'] = 42  # FIXME
                d_attr['group'] = 42  # FIXME
                d_attr['mode'] = s.mode
                d_attr['size'] = s.size_bytes
                d_attr['id'] = s.id     # Extra for flatgrids demo

                xattr_policy = 'user.LFS.AllocationPolicy'
                cur.execute('''
                SELECT value FROM shelf_xattrs
                    WHERE shelf_id=? AND xattr=?''', (s.id, xattr_policy))
                xattr_value = cur.fetchone()
                s_policy = 'unknown' if xattr_value is None else xattr_value[0]
                d_attr['policy'] = s_policy

                l_entries.append(d_attr)

            if requestor_wants_json(request):
                return jsonify(
                    owner=d_owner,
                    group=d_group,
                    mode=d_mode,
                    entries=l_entries)

            return render_template(
                'show_directory.html',
                owner=d_owner,
                group=d_group,
                mode=d_mode,
                entries=l_entries,
                api_version=mainapp.config['API_VERSION'])

        else:
            cur.execute('SELECT book_size_bytes FROM globals')
            cur.iterclass = None
            s_booksize = cur.fetchone()[0]

            cur.execute('SELECT * FROM shelves WHERE name=?', pathname)
            cur.iterclass = 'default'
            shelf = [ r for r in cur ]
            if len(shelf) == 0:
                return _response_bad("Shelf does not exist", 404)
            for s in shelf:
                s_owner = 42  # FIXME
                s_group = 42  # FIXME
                s_mode = s.mode
                s_size = s.size_bytes

            xattr_policy = 'user.LFS.AllocationPolicy'
            cur.execute('''
                SELECT value FROM shelf_xattrs
                WHERE shelf_id=? AND xattr=?''', (s.id, xattr_policy))
            xattr_value = cur.fetchone()
            s_policy = 'unknown' if xattr_value is None else xattr_value[0]

            cur.execute('''
                SELECT * FROM opened_shelves
                JOIN SOCs
                ON opened_shelves.node_id = SOCs.node_id
                WHERE opened_shelves.shelf_id = ?''', s.id)
            cur.iterclass = 'default'
            open = [ r for r in cur ]
            l_active = []
            for o in open:
                if o.coordinate not in l_active:
                    l_active.append(o.coordinate)

            cur.execute('''
                SELECT * FROM books_on_shelves
                WHERE shelf_id = ?''', s.id)
            cur.iterclass = 'default'
            books = [ r for r in cur ]
            l_books = []
            for b in books:
                l_books.append(b.book_id)

            if requestor_wants_json(request):
                return jsonify(
                    owner=s_owner,
                    group=s_group,
                    mode=s_mode,
                    size=s_size,
                    booksize=s_booksize,
                    policy=s_policy,
                    active=l_active,
                    books=l_books)

            return render_template(
                'show_shelf.html',
                owner=s_owner,
                group=s_group,
                mode=s_mode,
                size=s_size,
                booksize=s_booksize,
                policy=s_policy,
                active=l_active,
                books=l_books,
                api_version=mainapp.config['API_VERSION'])

    except Exception as e:
        return _response_bad('%s' % (e), 400)

###########################################################################
# View: /books/{interleaveGroup} - Book Information
# Desc: list information about all books
# Input: interleave-group - (optional) interleave group to filter books listing


@mainapp.route('/lmp/books/')
@mainapp.route('/lmp/books/<interleaveGroup>')
def show_books(interleaveGroup="all"):
    try:
        cur = mainapp.cur
        cur.execute('SELECT book_size_bytes FROM globals')
        cur.iterclass = None
        b_size = cur.fetchone()[0]

        allIGs = interleaveGroup == 'all'
        if allIGs:
            cur.execute('SELECT * FROM books')
        else:
            # On SD990 and SDFlex the IG field took on flags above 8 bits (see
            # frdnode::IG_MASK.  Filter out the IG value from the lower bits.
            cur.execute('SELECT * FROM books WHERE intlv_group & 0xff = ?',
                        int(interleaveGroup))

        cur.iterclass = 'default'
        books = [ r for r in cur ]
        if len(books) == 0:
            return _response_bad("interleaveGroup does not exist", 404)
        l_books = []
        for b in books:
            d_b = {}
            d_b['lza'] = b.id
            d_b['state'] = convert_book_status(b.allocated)
            if allIGs:
                d_b['ig'] = b.intlv_group & 0xFF

            # Do not report shelf or offset for notready books
            if d_b['state'] == 'notready':
                l_books.append(d_b)
                continue

            cur.execute('SELECT * FROM books_on_shelves WHERE book_id=?', b.id)
            cur.iterclass = 'default'
            bos = [ r for r in cur ]
            assert len(bos) <= 1, 'Matched more than one BOS'
            for s in bos:
                d_b['offset'] = s.seq_num * b_size
                cur.execute('SELECT * FROM shelves WHERE id=?', s.shelf_id)
                cur.iterclass = 'default'
                shelf = [ r for r in cur ]
                assert len(shelf) <= 1, 'Matched more than one shelf'
                for h in shelf:
                    d_b['shelf'] = h.name

            l_books.append(d_b)

    except Exception as e:
        return _response_bad('%s' % (e), 400)

    if requestor_wants_json(request):
        return jsonify(book_size=b_size, books=l_books)

    return render_template(
        'show_books.html',
        book_size=b_size,
        books=l_books,
        api_version=mainapp.config['API_VERSION'])

############################################################################
# Create a FIFO after possibly removing an old one.  Return an open, non-
# blocking file descriptor.  path can be absolute or relative (because
# of the os.chdir way up top).

inpipe = None                   # Globals set at startup, used in event loop.
logger = None


def piper(path='/tmp/lmpfifo'):
    global inpipe

    logger = logging.getLogger('werkzeug')      # For Justin.
    try:
        if os.path.exists(path):
            assert stat.S_ISFIFO(os.stat(path).st_mode), \
                '%s exists but is not a FIFO'
        else:
            os.mkfifo(path)
    except Exception as e:
        raise RuntimeError('reuse/create("%s") failed: %s' % (path, str(e)))

    try:
        inpipe = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    except Exception as e:
        raise RuntimeError('open("%s") failed: %s' % (path, str(e)))

###########################################################################
# See https://github.hpe.com/rocky-craig/FABulous for standalone version.
# Putting it here means one less program to run.  Fix up matryoshka config
# file to point here.

lastFAB = 7                     # Gotta start somewhere.


@mainapp.route('/fab/')
def fab():
    global lastFAB                          # MIGHT get updated

    try:
        new = os.read(inpipe, 1024)         # Non-blocking
        if not new:                         # No data
            new = lastFAB                   # Let it ride
        else:
            # Extract the trailing/final number.  Any errors will return
            # the previous sample.
            try:
                new = new.decode().strip()  # Bytes to string and chomp
                new = int(new.split()[-1])
                assert 0 <= new <= 100
            except Exception as e:
                new = lastFAB
    except OSError as err:
        new = lastFAB                       # Let it ride
        if err.errno != errno.EAGAIN:       # aka EWOULDBLOCK
            logger.warning('Pipe read got %s' % str(err))

    lastFAB = new
    return jsonify({ 'fabric': { 'percentage': new }})

###########################################################################
# Main.  Now that routes are done, load flatgrids which memoizes them.

if __name__ == '__main__':

    if False:
        from importlib import import_module
        grids = import_module('flatgrids')
        grids.register(mainapp)

    DB_FILE = '/var/hpetm/librarian.db'

    parser = ArgumentParser(
        formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-d',
        action='store',
        dest='db_file',
        default=DB_FILE,
        help='full path to librarian database file')

    args = parser.parse_args()

    try:
        st = os.stat(args.db_file)
        mainapp.db_file = args.db_file
    except IOError as e:
        raise SystemExit('DB file %s does not exist' % args.db_file)

    try:
        piper(mainapp.config['FABFIFO'])
    except Exception as err:
        raise SystemExit(err)

    mainapp.run(
        debug=mainapp.config['DEBUG'],
        use_reloader=bool(mainapp.config['DEBUG']),
        host=mainapp.config['HOST'],
        port=mainapp.config['PORT'],
        threaded=False)
