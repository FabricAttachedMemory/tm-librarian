#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian server main module
#---------------------------------------------------------------------------

import argparse
import sys

from pdb import set_trace

from backend_sqlite3 import LibrarianDBackendSQLite3 as LBE
from engine import LibrarianCommandEngine as LCE
from socket_handling import Server
from librarian_chain import LibrarianChain

# Initialize argparse for local stuff, then have each module add
# its arguments, then go.
parser = argparse.ArgumentParser(description='The Machine Librarian')
parser.add_argument('--verbose',
                     help='level of runtime output, larger == more',
                     type=int,
                     default=0)

for obj in (LBE, LCE, Server, LibrarianChain ):
    obj.argparse_extend(parser)
args = parser.parse_args()

backend = LBE(args)
lce = LCE(backend, args)
server = Server(args)
chain = LibrarianChain(args)

try:
    server.serv(lce, chain)
except Exception as e:
    print(str(e))

backend.close()

raise SystemExit(0)
