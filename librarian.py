#!/usr/bin/python3
#---------------------------------------------------------------------------
# Librarian server main module
#---------------------------------------------------------------------------

import argparse
import database
import book_register


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

    db.close()
