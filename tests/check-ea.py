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

# Simple test to write different types of data to extended attribute

import errno
import os
import sys
import mmap
import binascii
import argparse
import pickle
import hashlib
import random
import base64

from pdb import set_trace

# Verification data for verify-only operations.  VERIF_DATA_VERSION should be
# incremented whenever an incompatible change to the data format is made.
VERIF_DATA_VERSION = b'2'
class VerifData:
    pickled_args = ''
    hash = ''


if __name__ == '__main__':

    VERBOSE=1
    VERIF_EA_NAME = "user.check-ea.test-attribute"
    total_failures = 0

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
        help='verbosity control (0 = none, 1 = normal)')

    args = parser.parse_args()

    try:
        assert args.shelf_name.startswith('/lfs/'), 'Not an LFS file'
        assert not os.path.isfile(args.shelf_name), '%s exists' % args.shelf_name
        APs = os.getxattr('/lfs', 'user.LFS.AllocationPolicyList').decode().split(',')
    except Exception as e:
        raise SystemExit('Bad initial conditions: %s' % str(e))
    for ap in APs:
        try:
            os.setxattr('/lfs', 'user.LFS.AllocationPolicyDefault', ap.encode())
            with open(args.shelf_name, 'w') as f:
            	os.ftruncate(f.fileno(), 1)
            thisap = os.getxattr(args.shelf_name, 'user.LFS.AllocationPolicy')
            assert ap == thisap.decode(), 'Policy mismatch: %s' % ap
            os.unlink(args.shelf_name)
        except Exception as e:
            if isinstance(e, OSError) and e.errno == errno.EINVAL:
                if ap in ('RequestIG', ):
                    continue
            raise SystemExit('Error during policy walkthrough: %s' % str(e))

    try:
        with open(args.shelf_name, 'w') as f:	# need a test file
            os.ftruncate(f.fileno(), 1)
        os.setxattr(args.shelf_name, VERIF_EA_NAME, b'asdfasdfasdfkajsd;flijasd;fiads;fui')
    except Exception as e:
        total_failures += 1
        if (args.verbose > 0):
            print("Error setting EA with string value: %s" % str(e))

    fh = VerifData()
    args.random_seed = os.urandom(8)
    fh.pickled_args = pickle.dumps(args)
    hash = hashlib.new('sha256')
    hash.update(fh.pickled_args)

    # Include version in hash, so incompatible versions will not be recognized.
    hash.update(VERIF_DATA_VERSION)
    fh.hash = hash.hexdigest()
    pickled_fh = pickle.dumps(fh)

    try:
        os.setxattr(args.shelf_name, VERIF_EA_NAME, pickled_fh)
    except:
        total_failures += 1
        if args.verbose:
            print("Error setting EA with default pickle protocol")


    pickled_fh = pickle.dumps(fh, protocol=0)

    try:
        os.setxattr(args.shelf_name, VERIF_EA_NAME, pickled_fh)
    except:
        total_failures += 1
        if args.verbose:
            print("Error setting EA with pickle protocol 0 ")


    try:
        os.setxattr(args.shelf_name, VERIF_EA_NAME, os.urandom(300))
    except:
        total_failures += 1
        if args.verbose:
            print("Error setting EA random data")


    try:
        os.setxattr(args.shelf_name, VERIF_EA_NAME, base64.b64encode(os.urandom(300)))
    except Exception as e:
        total_failures += 1
        if args.verbose:
            print("Error setting EA base64 encoded random data")

    if args.verbose:
        print("%s complete, total failures = %d" % (sys.argv[0], total_failures))
    raise SystemExit(total_failures)
