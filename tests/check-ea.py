#!/usr/bin/python3
# Simple test to write different types of data to extended attribute
import os
import sys
import mmap
import binascii
import argparse
import pickle
import hashlib
import random
import base64

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
        os.setxattr(args.shelf_name, VERIF_EA_NAME, b'asdfasdfasdfkajsd;flijasd;fiads;fui')
    except:
        total_failures += 1
        if (args.verbose > 0):
            print("Error setting EA with string value")


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
        if (args.verbose > 0):
            print("Error setting EA with default pickle protocol")


    pickled_fh = pickle.dumps(fh, protocol=0)

    try:
        os.setxattr(args.shelf_name, VERIF_EA_NAME, pickled_fh)
    except:
        total_failures += 1
        if (args.verbose > 0):
            print("Error setting EA with pickle protocol 0 ")


    try:
        os.setxattr(args.shelf_name, VERIF_EA_NAME, os.urandom(300))
    except:
        total_failures += 1
        if (args.verbose > 0):
            print("Error setting EA random data")


    try:
        os.setxattr(args.shelf_name, VERIF_EA_NAME, base64.b64encode(os.urandom(300)))
    except:
        total_failures += 1
        if (args.verbose > 0):
            print("Error setting EA base64 encoded random data")

    if (args.verbose > 0):
        print("check-ea.py test complete,", end="")
    if (total_failures == 0):
        if (args.verbose > 0):
            print(" all tests passed")
        sys.exit(0)
    else:
        if (args.verbose > 0):
            print(" total failures = %d" % total_failures)
        sys.exit(1)
