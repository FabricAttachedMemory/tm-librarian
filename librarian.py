#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian server main module
#---------------------------------------------------------------------------

import argparse
import database
import book_register
from engine import LibrarianCommandEngine
import socket_handling
from librarian_chain import Librarian_Chain

LIBRARIAN_VERSION="Librarian v0.01"


def librarian_args_init(parser):
    pass

if __name__ == '__main__':

    # Initialize argparse and module argument additions
    parser = argparse.ArgumentParser()
    database.db_args_init(parser)
    book_register.book_data_args_init(parser)
    librarian_args_init(parser)
    args = parser.parse_args()

    # Initialize database and check tables
    db = database.LibrarianDB()
    db.db_init(args)
    db.check_tables()

    # Populate books database
    book_register.load_book_data(args, db)

    # create server object
    server = socket_handling.Server()

    # add handler to server object
    chain = Librarian_Chain()

    lce = LibrarianCommandEngine(DBfile=args.something)
    server.serv(lce, chain)

    db.close()
