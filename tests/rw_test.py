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

""" Divide given shelf into chunks then write a random
    sized buffer of data to each chunk, read it back and
    verify data integrity. Using file and shmem operators.

    Requires:
    - librarian running
    - LFS running
    - shelf created
    - shelf truncated to non-zero size
"""

import os
import sys
import random
import mmap
from time import sleep

def read_write_test(args):

    CHUNKS = args.chunks
    MAX_RW = 1024 * 1024
    END_OFFSET = 1024
    DELAY = 0

    obuf = [b'' for _ in range(CHUNKS)]
    ibuf = [b'' for _ in range(CHUNKS)]

    c_start = [0 for _ in range(CHUNKS)]
    c_offset = [0 for _ in range(CHUNKS)]
    c_length = [0 for _ in range(CHUNKS)]

    fn = args.shelf
    max_iter = args.iteration

    with open(fn, 'r+b', buffering=0) as f:

        st = os.fstat(f.fileno())
        file_size = st.st_size
        chunk_size = file_size // CHUNKS

        print("file = %s, size = %d, max_iter = %d" % (fn, file_size, max_iter))
        print("CHUNKS = %d, chunk_size = %d" % (CHUNKS, chunk_size))

        cur_cs = 0
        for x in range(CHUNKS):
            c_start[x] = cur_cs
            print("chunk = %d, c_start = %d, cend = %d, chunk_size = %d" %
                  (x, c_start[x], (c_start[x] + chunk_size - 1), chunk_size))
            cur_cs += chunk_size

        iter = 1

        while True:

            if args.type in ['fs', 'both']:

                print("filesystem read/write testing -----")

                for x in range(CHUNKS):

                    obuf[x] = b''

                    c_offset[x] = random.randrange(c_start[x], (c_start[x] + chunk_size - END_OFFSET), 1)
                    end_of_chunk = min((chunk_size - (c_offset[x] % chunk_size)), MAX_RW)
                    c_length[x] = random.randrange(1, end_of_chunk, 1)

                    print("[%d] r/w file (%d): offset = %d, length = %d, last = %d" %
                          (iter, x, c_offset[x], c_length[x], (c_offset[x] + c_length[x]-1)))

                    obuf[x] = os.urandom(c_length[x])

                    f.seek(c_offset[x])
                    f.write(obuf[x])
                    f.flush()

                    sleep(DELAY)

                for x in range(CHUNKS):

                    ibuf[x] = b''

                    f.seek(c_offset[x])
                    ibuf[x] = f.read(c_length[x])

                    if (obuf[x] != ibuf[x]):
                        print("verify file (%d) - fail " % (x))
                        exit(-1)

            if args.type in ['mm', 'both']:

                print("mmap read/write testing -----")

                m = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ | mmap.PROT_WRITE)

                for x in range(CHUNKS):

                    obuf[x] = b''

                    c_offset[x] = random.randrange(c_start[x], (c_start[x] + chunk_size - END_OFFSET), 1)
                    end_of_chunk = min((chunk_size - (c_offset[x] % chunk_size)), MAX_RW)
                    c_length[x] = random.randrange(1, end_of_chunk, 1)

                    print("[%d] r/w mmap (%d): offset = %d, length = %d, last = %d" %
                          (iter, x, c_offset[x], c_length[x], (c_offset[x] + c_length[x]-1)))

                    obuf[x] = os.urandom(c_length[x])

                    m.seek(c_offset[x])
                    m.write(obuf[x])
                    m.flush()

                    sleep(DELAY)

                for x in range(CHUNKS):

                    ibuf[x] = b''

                    m.seek(c_offset[x])
                    ibuf[x] = m.read(c_length[x])

                    if (obuf[x] != ibuf[x]):
                        print("verify mmap (%d) - fail " % (x))
                        exit(-1)

                m.close()

            if iter == max_iter:
                return

            iter += 1

if __name__ == '__main__':

    import argparse
    import os
    import sys

    test_types = ['fs', 'mm', 'both']
    parser = argparse.ArgumentParser(
        description='Librarian read/write/verify regression test')
    parser.add_argument(
        'shelf',
        help='name of shelf to perform test on',
        type=str)
    parser.add_argument(
        'type',
        choices=test_types,
        help='type of testing to perform; fs, mm or both')
    parser.add_argument(
        'iteration',
        help='number of iterations to perform (0=forever)',
        type=int)
    parser.add_argument(
        '--chunks',
        default=5,
        help='number of chunks to divide shelf into (default=5)',
        type=int)
    args = parser.parse_args()

    read_write_test(args)

    exit(0)
