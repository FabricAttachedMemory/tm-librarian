#!/usr/bin/python3

import os
import sys
import mmap
import binascii
import argparse

def rw_mmap(shelf_name, verbose, debug, book_max, length, book_size, book_start, chunk_size, chunk_cnt):

    with open(shelf_name, 'r+b') as f:

        obuf_rand = os.urandom(length)
        obuf_zero = b"\x00" * length
        offset = ((book_start - 1) * book_size)
        book_num = book_start
        book_end = book_start + book_max

        if (debug):
            print("offset = %d (0x%x)" % (offset, offset))
            print("book_size = %d (0x%x)" % (book_size, book_size))
            print("book_num = %d" % book_num)
            print("book_max = %d" % book_max)
            print("book_start = %d" % book_start)
            print("book_end = %d" % book_end)
            print("length = %d" % length)

        m = mmap.mmap(f.fileno(), 0)

        while book_num < book_end:

            for pos in range(0, chunk_cnt):

                book_offset = (pos * chunk_size)
                cur_offset = offset + book_offset

                print("book %4d: pos = %d, book_offset = 0x%012x cur_offset = 0x%012x, size = %d" %
                    (book_num, pos, book_offset, cur_offset, len(obuf_rand)))

                m.seek(cur_offset)
                ibuf = b"\x00" * length
                ibuf = m.read(length)
                if (verbose > 0):
                    print("read: %s" % (binascii.hexlify(ibuf)))

                m.seek(cur_offset)
                m.write(obuf_rand)
                if (verbose > 0):
                    print("write: cur_offset = %d (0x%x), size = %d" % (cur_offset, cur_offset, len(obuf_rand)))
                    print("write: %s" % (binascii.hexlify(obuf_rand)))

                m.seek(cur_offset)
                ibuf = b"\x00" * length
                ibuf = m.read(length)
                if (verbose > 0):
                    print("read: %s" % (binascii.hexlify(ibuf)))

                if obuf_rand == ibuf and len(obuf_rand) == len(ibuf):
                    if (verbose > 0):
                        print("verify passed")
                else:
                    print("verify failed")

            book_num += 1
            offset += book_size

        m.close()
    f.close()

if __name__ == '__main__':

    BOOK_SIZE=(1024*1024*1024*8)  # 8GB
    BOOK_MAX=512
    BOOK_START=1
    LENGTH=128
    CHUNK_SIZE=4096
    CHUNK_CNT=1

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(action='store', dest='shelf_name', help='shelf/file to read/write/read/verify')
    parser.add_argument('-v', action='store_true', dest='verbose', help='verbose output')
    parser.add_argument('-d', action='store_true', dest='debug', help='debug output')
    parser.add_argument('-n', action='store', dest='book_max', default=BOOK_MAX, type=int, help='number of books to verify')
    parser.add_argument('-l', action='store', dest='length', default=LENGTH, type=int, help='size in bytes of read/write')
    parser.add_argument('-s', action='store', dest='book_start', default=BOOK_START, type=int, help='book number to start at')
    parser.add_argument('-b', action='store', dest='book_size', default=BOOK_SIZE, type=int, help='number of bytes in a book')
    parser.add_argument('-z', action='store', dest='chunk_size', default=CHUNK_SIZE, type=int, help='size in bytes of book chunks')
    parser.add_argument('-c', action='store', dest='chunk_cnt', default=CHUNK_CNT, type=int, help='number of book chunks to access')

    args = parser.parse_args()

    if (args.debug):
        print("args.shelf_name = %s" % args.shelf_name)
        print("args.verbose    = %s" % args.verbose)
        print("args.book_max   = %d" % args.book_max)
        print("args.length     = %d" % args.length)
        print("args.book_size  = %d" % args.book_size)
        print("args.book_start = %d" % args.book_start)
        print("args.chunk_size = %d" % args.chunk_size)
        print("args.chunk_cnt  = %d" % args.chunk_cnt)

    try:
        st = os.stat(args.shelf_name)
    except IOError:
        print("Cannot open: %s" % args.shelf_name)
        sys.exit(0)

    if ((st.st_size // args.book_size) == 0):
        total_books = 1
    else:
        total_books = (st.st_size // args.book_size)

    print("shelf = %s, size = %d bytes / %d book(s)" %
        (args.shelf_name, st.st_size, total_books))

    if (args.verbose > 0):
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

    rw_mmap( args.shelf_name, args.verbose, args.debug, args.book_max,
        args.length, args.book_size, args.book_start, args.chunk_size, args.chunk_cnt)

    sys.exit(0)
