#!/usr/bin/python3

import os
import sys
import mmap
import binascii
import argparse

def rw_mm(m, cur_offset, length, verbose):

    failures = 0
    obuf_rand = os.urandom(length)

    m.seek(cur_offset)
    ibuf = b"\x00" * length
    ibuf = m.read(length)
    if (verbose > 1):
        print("read: %s" % (binascii.hexlify(ibuf)))

    m.seek(cur_offset)
    m.write(obuf_rand)
    m.flush()
    if (verbose > 1):
        print("write: cur_offset = %d (0x%x), size = %d" %
            (cur_offset, cur_offset, len(obuf_rand)))
        print("write: %s" % (binascii.hexlify(obuf_rand)))

    m.seek(cur_offset)
    ibuf = b"\x00" * length
    ibuf = m.read(length)
    if (verbose > 1):
        print("read: %s" % (binascii.hexlify(ibuf)))

    if obuf_rand == ibuf and len(obuf_rand) == len(ibuf):
        if (verbose > 1):
            print("verify passed")
    else:
        failures += 1
        if (verbose > 0):
            print("verify failed")

    return failures

def rw_fs(f, cur_offset, length, verbose):

    failures = 0
    obuf_rand = os.urandom(length)

    f.seek(cur_offset)
    ibuf = b"\x00" * length
    ibuf = f.read(length)
    if (verbose > 1):
        print("read: %s" % (binascii.hexlify(ibuf)))

    f.seek(cur_offset)
    f.write(obuf_rand)
    f.flush()
    if (verbose > 1):
        print("write: cur_offset = %d (0x%x), size = %d" %
            (cur_offset, cur_offset, len(obuf_rand)))
        print("write: %s" % (binascii.hexlify(obuf_rand)))

    f.seek(cur_offset)
    ibuf = b"\x00" * length
    ibuf = f.read(length)
    if (verbose > 1):
        print("read: %s" % (binascii.hexlify(ibuf)))

    if obuf_rand == ibuf and len(obuf_rand) == len(ibuf):
        if (verbose > 1):
            print("verify passed")
    else:
        failures += 1
        if (verbose > 0):
            print("verify failed")

    return failures

def rw_books(shelf_name, verbose, debug, book_max, length, book_size,
    book_start, chunk_size, chunk_cnt, access_type, max_iter,
    trans_type, mmap_offset, mmap_length):

    total_failures = 0
    offset = ((book_start - 1) * book_size)
    book_num = book_start
    book_end = book_start + book_max
    cur_iter = 1

    if (debug):
        print("offset = %d (0x%x)" % (offset, offset))
        print("book_size = %d (0x%x)" % (book_size, book_size))
        print("book_num = %d" % book_num)
        print("book_max = %d" % book_max)
        print("book_start = %d" % book_start)
        print("book_end = %d" % book_end)
        print("length = %d" % length)
        print("max_iter = %d" % max_iter)
        print("mmap_offset = %d" % mmap_offset)
        print("mmap_length = %d" % mmap_length)

    f = open(shelf_name, 'r+b', buffering=0)

    if trans_type == 'mm':
        m = mmap.mmap(f.fileno(), length=mmap_length, offset=mmap_offset)
        if (verbose > 1):
            print("mmap offset = %d, length = %d" % (mmap_offset, mmap_length))

    while cur_iter <= max_iter:

        while book_num < book_end:

            for pos in range(0, chunk_cnt):

                if access_type == 'seq':
                    book_offset = (pos * chunk_size)
                else:
                    pos2 = pos - pos // 2
                    if pos%2 == 0:
                        book_offset = (pos2 * chunk_size)
                    else:
                        book_offset = book_size - (pos2 * chunk_size)

                cur_offset = offset + book_offset

                if (verbose > 0):
                    print("[%2d/%s] book %4d: pos = %d, book_offset = 0x%012x cur_offset = 0x%012x, size = %d" %
                        (cur_iter, trans_type, book_num, pos, book_offset, cur_offset, length))

                if trans_type == 'mm':
                    failures = rw_mm(m, cur_offset, length, verbose)
                else: # trans_type == 'fs'
                    failures = rw_fs(f, cur_offset, length, verbose)

                total_failures += failures

            book_num += 1
            offset += book_size

        book_num = book_start
        offset = ((book_start - 1) * book_size)
        cur_iter += 1

    if trans_type == 'mm':
        m.close()

    f.close()

    return total_failures

if __name__ == '__main__':

    BOOK_SIZE='8G'  # 8GB
    BOOK_MAX=512
    BOOK_START=1
    LENGTH=128
    CHUNK_SIZE=4096
    CHUNK_CNT=1
    ACCESS_TYPES = [ 'seq', 'bounce' ]
    TRANS_TYPES = [ 'mm', 'fs' ]
    MAX_ITER=1
    MMAP_OFFSET=0
    MMAP_LENGTH=0
    VERBOSE=0

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        action='store',
        dest='shelf_name',
        help='shelf/file to read/write/read/verify')
    parser.add_argument(
        '-v',
        action='store',
        dest='verbose',
        default=VERBOSE,
        type=int,
        help='verbosity control (0 = no output, 1 = basic status, 2 = everything)')
    parser.add_argument(
        '-d',
        action='store_true',
        dest='debug',
        help='debug output')
    parser.add_argument(
        '-n',
        action='store',
        dest='book_max',
        default=BOOK_MAX,
        type=int,
        help='number of books to verify')
    parser.add_argument(
        '-l',
        action='store',
        dest='length',
        default=LENGTH,
        type=int,
        help='size in bytes of read/write')
    parser.add_argument(
        '-s',
        action='store',
        dest='book_start',
        default=BOOK_START,
        type=int,
        help='book number to start at')
    parser.add_argument(
        '-b',
        action='store',
        dest='book_size',
        default=BOOK_SIZE,
        help='number of bytes in a book (K, M, G suffixes supported)')
    parser.add_argument(
        '-z',
        action='store',
        dest='chunk_size',
        default=CHUNK_SIZE,
        type=int,
        help='size in bytes of book chunks')
    parser.add_argument(
        '-c',
        action='store',
        dest='chunk_cnt',
        default=CHUNK_CNT,
        type=int,
        help='number of book chunks to access')
    parser.add_argument(
        '-i',
        action='store',
        dest='max_iter',
        default=MAX_ITER,
        type=int,
        help='number of iterations to run')
    parser.add_argument(
        '-a',
        action='store',
        dest='access_type',
        choices= ACCESS_TYPES,
        default='seq',
        help='book chunk access type')
    parser.add_argument(
        '-t',
        action='store',
        dest='trans_type',
        choices= TRANS_TYPES,
        default='mm',
        help='type of transaction (mmap \'mm\' or filesystem \'fs\')')
    parser.add_argument(
        '-o',
        action='store',
        dest='mmap_offset',
        default=MMAP_OFFSET,
        type=int,
        help='mmap offset in bytes (default is beginning of file')
    parser.add_argument(
        '-x',
        action='store',
        dest='mmap_length',
        default=MMAP_LENGTH,
        type=int,
        help='mmap length in bytes (default is end of file')

    args = parser.parse_args()

    if (args.debug):
        print("args.shelf_name  = %s" % args.shelf_name)
        print("args.verbose     = %d" % args.verbose)
        print("args.book_max    = %d" % args.book_max)
        print("args.length      = %d" % args.length)
        print("args.book_size   = %s" % args.book_size)
        print("args.book_start  = %d" % args.book_start)
        print("args.chunk_size  = %d" % args.chunk_size)
        print("args.chunk_cnt   = %d" % args.chunk_cnt)
        print("args.access_type = %s" % args.access_type)
        print("args.max_iter    = %d" % args.max_iter)
        print("args.trans_type  = %s" % args.trans_type)
        print("args.mmap_offset = %s" % args.mmap_offset)
        print("args.mmap_length = %s" % args.mmap_length)

    try:
        st = os.stat(args.shelf_name)
    except IOError:
        print("Cannot open: %s" % args.shelf_name)
        sys.exit(1)

    if (args.book_size.endswith(('k', 'K', 'm', 'M', 'g', 'G'))):
        suffix = args.book_size[-1].upper()
        try:
            rsize = int(args.book_size[:-1])
        except ValueError:
            print('Illegal book_size value "%s"' % args.book_size[:-1])
            sys.exit(1)

        if suffix == 'K':
            book_size = int(rsize * 1024)
        elif suffix == 'M':
            book_size = int(rsize * 1024 * 1024)
        elif suffix == 'G':
            book_size = int(rsize * 1024 * 1024 * 1024)
    else:
        try:
            book_size = int(args.book_size)
        except ValueError:
            print("Illegal book_size: %s" % args.book_size)
            sys.exit(1)

    if ((st.st_size // book_size) == 0):
        total_books = 1
    else:
        total_books = (st.st_size // book_size)

    if (args.mmap_offset > st.st_size):
        print("offset (%d) is greater than file size (%d)" % (args.mmap_offset, st.st_size))
        sys.exit(1)

    if (args.mmap_length > st.st_size):
        print("length (%d) is greater than file size (%d)" % (args.mmap_length, st.st_size))
        sys.exit(1)

    if ((args.mmap_offset + args.mmap_length) > st.st_size):
        print("offset+length (%d) is greater than file size (%d)" %
            ((args.mmap_length+args.mmap_offset), st.st_size))
        sys.exit(1)

    if (args.mmap_length == 0):
        mmap_length = st.st_size - args.mmap_offset
    else:
        mmap_length = args.mmap_length

    if (args.verbose > 1):
        print("shelf = %s, size = %d bytes / %d book(s)" %
            (args.shelf_name, st.st_size, total_books))

    if (args.verbose > 1):
        print("  st_mode     : 0x%x" % st.st_mode)
        print("  st_ino      : 0x%x" % st.st_ino)
        print("  st_dev      : 0x%x" % st.st_dev)
        print("  st_nlink    : %d" % st.st_nlink)
        print("  st_uid      : %d" % st.st_uid)
        print("  st_gid      : %d" % st.st_gid)
        print("  st_size     : %d" % st.st_size)
        print("  st_atime    : 0x%x" % st.st_atime)
        print("  st_mtime    : 0x%x" % st.st_mtime)
        print("  st_ctime    : 0x%x" % st.st_ctime)
        print("  st_atime_ns : 0x%x" % st.st_atime_ns)
        print("  st_mtime_ns : 0x%x" % st.st_mtime_ns)
        print("  st_ctime_ns : 0x%x" % st.st_ctime_ns)

    total_failures = rw_books( args.shelf_name, args.verbose, args.debug,
        args.book_max, args.length, book_size, args.book_start, args.chunk_size,
        args.chunk_cnt, args.access_type, args.max_iter, args.trans_type,
        args.mmap_offset, mmap_length)

    if (args.verbose > 0):
        print("total_failures = %d" % total_failures)

    if (total_failures == 0):
        sys.exit(0)
    else:
        sys.exit(1)
