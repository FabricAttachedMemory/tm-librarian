#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian server main module
#---------------------------------------------------------------------------

import argparse
import sys

from pdb import set_trace

from database import LibrarianDBackendSQL as LBE
from engine import LibrarianCommandEngine as LCE
from socket_handling import Server
from librarian_chain import Librarian_Chain

# Initialize argparse for local stuff, then have each module add
# its arguments, then go.
parser = argparse.ArgumentParser(description='The Machine Librarian')

for obj in (LBE, LCE, Server, Librarian_Chain ):
    obj.argparse_extend(parser)
args = parser.parse_args()

backend = LBE(args)
lce = LCE(backend, args)
lce.check_tables()
server = Server(args)
chain = Librarian_Chain(args)

try:
    server.serv(lce, chain)
except Exception as e:
    print(str(e))

backed.close()

raise SystemExit(0)
