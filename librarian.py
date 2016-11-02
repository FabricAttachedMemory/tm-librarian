#!/usr/bin/python3 -tt
""" Librarian server main module
"""
import argparse

from pdb import set_trace

from backend_sqlite3 import LibrarianDBackendSQLite3 as LBE
from engine import LibrarianCommandEngine as LCE
from socket_handling import Server, lfsLogger

# Initialize argparse for local stuff, then have each module add
# its arguments, then go.
parser = argparse.ArgumentParser(description='The Machine Librarian')
parser.add_argument(
    '--verbose',
    help='level of runtime output (0=ERROR, 1=PERF, 2=NOTICE, 3=INFO, 4=DEBUG, 5=OOB)',
    type=int,
    default=0)
parser.add_argument(
    '--daemon',
    help='run in the background, logging to /var/log/librarian',
    action='store_true')

def main():
    """ Librarian main """
    for obj in (LBE, LCE, Server):
        obj.argparse_extend(parser)
    parseargs = parser.parse_args()

    if parseargs.daemon:
        raise NotImplementedError('Steal daemonize from books_allocated_demo')
        lfsLogger('Librarian', parseargs.verbose, 'librarian')
    else:
        lfsLogger('Librarian', parseargs.verbose)   # stderr

    backend = LBE(parseargs)
    lce = LCE(backend, parseargs)
    server = Server(parseargs)

    try:
        server.serv(lce)
    except Exception as e:
        print(str(e))

    backend.close()

if __name__ == '__main__':
    main()
