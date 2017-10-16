#!/usr/bin/python3

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

import os
import sys
import mmap
import binascii
import argparse
import pickle
import hashlib
import random

# Verification header for verify-only operations.  HEADER_VERSION should be
# incremented whenever an incompatible change to the header is made.
HEADER_VERSION = b'1'
class FileHeader:
    pickled_args = ''
    hash = ''


def rw_mm(m, cur_offset, length, verbose, ops):

    failures = 0
    rand_index = random.randrange(0, len(global_rand_buf) - length);

    if 'r' in ops:
        m.seek(cur_offset)
        ibuf = m.read(length)
        if (verbose > 2):
            print("read: %s" % (binascii.hexlify(ibuf)))

    if 'w' in ops:
        m.seek(cur_offset)
        m.write(global_rand_buf[rand_index:rand_index + length])
        m.flush(cur_offset, length)
        if (verbose > 2):
            print("write: cur_offset = %d (0x%x), size = %d" %
                (cur_offset, cur_offset, length))
            print("write: %s" % (binascii.hexlify(global_rand_buf[rand_index:rand_index + length])))

    if 'v' in ops:
        m.seek(cur_offset)
        ibuf = m.read(length)
        if (verbose > 2):
            print("read: %s" % (binascii.hexlify(ibuf)))

        if global_rand_buf[rand_index:rand_index + length] == ibuf and length == len(ibuf):
            if (verbose > 2):
                print("verify passed")
        else:
            failures += 1
            if (verbose > 1):
                print("verify failed")

    return failures

def rw_fs(f, cur_offset, length, verbose, ops):

    failures = 0
    rand_index = random.randrange(0, len(global_rand_buf) - length);

    if 'r' in ops:
        f.seek(cur_offset)
        ibuf = f.read(length)
        if (verbose > 2):
            print("read: %s" % (binascii.hexlify(ibuf)))

    if 'w' in ops:
        f.seek(cur_offset)
        f.write(global_rand_buf[rand_index:rand_index + length])
        f.flush()
        if (verbose > 2):
            print("write: cur_offset = %d (0x%x), size = %d" %
                (cur_offset, cur_offset, length))
            print("write: %s" % (binascii.hexlify(global_rand_buf[rand_index:rand_index + length])))

    if 'v' in ops:
        f.seek(cur_offset)
        ibuf = f.read(length)
        if (verbose > 2):
            print("read: %s" % (binascii.hexlify(ibuf)))

        if global_rand_buf[rand_index:rand_index + length] == ibuf and length == len(ibuf):
            if (verbose > 2):
                print("verify passed")
        else:
            failures += 1
            if (verbose > 1):
                print("verify failed")

    return failures

def rw_books(shelf_name, verbose, debug, book_max, length, book_size,
    book_start, chunk_size, chunk_cnt, access_type, max_iter,
    trans_type, mmap_offset, mmap_length, header_len, file_ops):

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
        if (verbose > 2):
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

                # Skip reading/writing range occupied by header
                if (cur_offset < header_len):
                    rw_length = length - (header_len - cur_offset)
                    if rw_length <= 0 and verbose > 1:
                        print("[%2d/%s] book %4d: pos = %d, book_offset = 0x%012x cur_offset = 0x%012x, size = %d (skipped due to verify header)" %
                            (cur_iter, trans_type, book_num, pos, book_offset, cur_offset, length))
                    cur_offset = header_len
                else:
                    rw_length = length

                if rw_length > 0:
                    if (verbose > 1):
                        print("[%2d/%s] book %4d: pos = %d, book_offset = 0x%012x cur_offset = 0x%012x, size = %d" %
                            (cur_iter, trans_type, book_num, pos, book_offset, cur_offset, rw_length))

                    if trans_type == 'mm':
                        failures = rw_mm(m, cur_offset, rw_length, verbose, file_ops)
                    else: # trans_type == 'fs'
                        failures = rw_fs(f, cur_offset, rw_length, verbose, file_ops)

                    total_failures += failures

            book_num += 1
            offset += book_size

        book_num = book_start
        offset = ((book_start - 1) * book_size)

        if verbose > 0 and max_iter > 1:
            print("Iteration %d of %d complete," % (cur_iter, max_iter), end="")
            if failures:
                print(" %d failures", (failures))
            else:
                print(" passed")
        cur_iter += 1

    if trans_type == 'mm':
        m.close()

    f.close()

    return total_failures



# This function processes the verification header that may be at the beginning
# of the test file, or stored in a separate file.
# On file creation, this writes the header, and reads it when verifying the file.
# Return values:
#   args - when reading header, args are modified to match how file was written
#   header_len - header length, used for skipping header when embedded in file
#   random_seed - random seed is used when verifying to seed PRNG to match how
#                 file was written.
#   file_ops - what file operations should be done.
def process_verif_header (args, random_seed):
    header_len = 0
    file_ops = ('r', 'w', 'v')
    if args.verify:
        # We are verifying, so we need to read header, and override args
        if args.header_file:
            f = open(args.header_file, 'r+b', buffering=0)
        else:
            f = open(args.shelf_name, 'r+b', buffering=0)
        try:
            fh = pickle.load(f)
        except:
            print("Error: unable to read verification header, it is either missing (ie not written with --header option) or corrupt.")
            sys.exit(1)

        if not args.header_file:
            header_len = f.tell()
        f.close()
        hash = hashlib.new('sha256')
        hash.update(fh.pickled_args)
        # Include version in hash, so incompatible versions will not be recognized.
        hash.update(HEADER_VERSION)
        if hash.hexdigest() == fh.hash:
            # Copy required args from arguments in header
            verify_args = pickle.loads(fh.pickled_args)
            args.book_max    =  verify_args.book_max
            args.length      =  verify_args.length
            args.book_size   =  verify_args.book_size
            args.book_start  =  verify_args.book_start
            args.chunk_size  =  verify_args.chunk_size
            args.chunk_cnt   =  verify_args.chunk_cnt
            args.access_type =  verify_args.access_type
            args.mmap_offset =  verify_args.mmap_offset
            args.mmap_length =  verify_args.mmap_length
            random_seed =  verify_args.random_seed
        else:
            print("Error: Cannot load header due to checksum error, verify not supported.")
            sys.exit(1)
        if args.verbose > 0:
            print("Verifying existing file, test parameters take from file header, not command line")
        file_ops = ('v')

    elif args.header:
        fh = FileHeader()
        args.random_seed = random_seed
        fh.pickled_args = pickle.dumps(args);
        hash = hashlib.new('sha256')
        hash.update(fh.pickled_args)
        # Include version in hash, so incompatible versions will not be recognized.
        hash.update(HEADER_VERSION)
        fh.hash = hash.hexdigest()
        pickled_fh = pickle.dumps(fh)
        if args.header_file:
            f = open(args.header_file, 'w+b', buffering=0)
            f.write(pickled_fh)
        else:
            f = open(args.shelf_name, 'r+b', buffering=0)
            f.write(pickled_fh)
            header_len = f.tell()
        f.close()
    return args, header_len, random_seed, file_ops

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
    VERBOSE=1

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
        help='verbosity control (0 = none, 3 = max)')
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
    parser.add_argument(
        '--verify',
        action='store_true',
        dest='verify',
        help='verify existing file.  must have verification header written with --header.  This is a read-only operation.')
    parser.add_argument(
        '--header',
        action='store_true',
        dest='header',
        help='write test parameters to verification header, allows verification of file later')
    parser.add_argument(
        '--header_file',
        action='store',
        dest='header_file',
        default='',
        help='separate file to write verification header to, allows test file to be test data only')
    parser.add_argument(
        '--random_seed',
        action='store',
        dest='random_seed',
        default='',
        help='seed for pseudorandom pattern, use to generate reproducible patterns')

    args = parser.parse_args()

    if args.verify and args.header:
        print("Error: --verify and --header are mutually exclusive options.")
        sys.exit(1)

    if args.max_iter > 1 and (args.verify or args.header):
        print("Error: -i and --verify or --header not compatible options.")
        sys.exit(1)

    if args.length > args.chunk_size and args.header:
        print("Error: write length must be no bigger than chunk size to enable verification headers.")
        sys.exit(1)

    if args.random_seed:
        random_seed = args.random_seed
    else:
        random_seed = os.urandom(8)

    try:
        st = os.stat(args.shelf_name)
    except IOError:
        print("Cannot open: %s" % args.shelf_name)
        sys.exit(1)

    # Read or write the verification header, based on args.
    args, header_len, random_seed, file_ops = process_verif_header(args, random_seed);

    # Create a global buffer of pseudorandom data to use for test.  We use data
    # from random offsets into this buffer to get changing data faster than we
    # can generate new bytes.
    random.seed(random_seed)
    global_rand_buf = bytes(random.getrandbits(8) for _ in range(3 * args.length))

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
        print("args.mmap_offset = %s" % args.mmap_offset)
        print("args.mmap_length = %s" % args.mmap_length)
        print("args.verify = %s" % args.verify)

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

    if (args.verbose > 2):
        print("shelf = %s, size = %d bytes / %d book(s)" %
            (args.shelf_name, st.st_size, total_books))

    if (args.verbose > 2):
        print("  st_mode     : 0x%x" % st.st_mode)
        print("  st_ino      : 0x%x" % st.st_ino)
        print("  st_dev      : 0x%x" % st.st_dev)
        print("  st_nlink    : %d" % st.st_nlink)
        print("  st_uid      : %d" % st.st_uid)
        print("  st_gid      : %d" % st.st_gid)
        print("  st_size     : %d" % st.st_size)
        print("  st_atime    : %f" % st.st_atime)
        print("  st_mtime    : %f" % st.st_mtime)
        print("  st_ctime    : %f" % st.st_ctime)
        print("  st_atime_ns : 0x%x" % st.st_atime_ns)
        print("  st_mtime_ns : 0x%x" % st.st_mtime_ns)
        print("  st_ctime_ns : 0x%x" % st.st_ctime_ns)

    total_failures = rw_books( args.shelf_name, args.verbose, args.debug,
        args.book_max, args.length, book_size, args.book_start, args.chunk_size,
        args.chunk_cnt, args.access_type, args.max_iter, args.trans_type,
        args.mmap_offset, mmap_length, header_len, file_ops)


    if (args.verbose > 0):
        print("check-book test complete,", end="")
    if (total_failures == 0):
        if (args.verbose > 0):
            print(" all tests passed")
        sys.exit(0)
    else:
        if (args.verbose > 0):
            print(" total failures = %d" % total_failures)
        sys.exit(1)
