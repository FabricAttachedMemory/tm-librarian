#!/usr/bin/python3 -tt
""" Librarian server main module
"""
import argparse

from pdb import set_trace

from backend_sqlite3 import LibrarianDBackendSQLite3 as LBE
from engine import LibrarianCommandEngine as LCE
from socket_handling import Server

# Initialize argparse for local stuff, then have each module add
# its arguments, then go.
parser = argparse.ArgumentParser(description='The Machine Librarian')
parser.add_argument(
    '--verbose',
    help='level of runtime output, larger == more',
    type=int,
    default=0)
parser.add_argument(
    '--perf',
    help='suppress output and some functions for perf runs, larger= more',
    type=int,
    default=0)


def main():
    """ Librarian main """
    for obj in (LBE, LCE, Server):
        obj.argparse_extend(parser)
    parseargs = parser.parse_args()

    backend = LBE(parseargs)
    lce = LCE(backend, parseargs)
    server = Server(parseargs, perf=parseargs.perf)

    try:
        server.serv(lce)
    except Exception as e:
        print(str(e))

    backend.close()

if __name__ == '__main__':
    main()
