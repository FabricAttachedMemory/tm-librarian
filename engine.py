#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian engine module
#---------------------------------------------------------------------------

import uuid
import time

# version will fail if you don't have a top level definition
LIBRARIAN_VERSION = "Librarian v0.01"

def cmd_version(cmd_data):
    """" Return Librarian version number
         Input---
           None
         Output---
           status
           version
    """
    out_data = {}
    out_data.update({'status': 0})
    out_data.update({'version': LIBRARIAN_VERSION})
    return(out_data)


def cmd_shelf_create(cmd_data):
    """" Return Librarian version number
         Input---
           shelf_owner
         Output---
           status
           version
    """
    out_data = {}
    shelf_handle = int(uuid.uuid1().int >> 65)
    shelf_owner = cmd_data["shelf_owner"]
    shelf_time = time.time()
    shelf_data = (shelf_handle, 0, 0, 0, shelf_owner, shelf_time)
    print("shelf_data:", shelf_data)
    status = db.create_shelf(shelf_data)
    if status == 0:
        out_data.update({'status': status})
        out_data.update({'shelf_handle': shelf_handle})
    else:
        out_data.update({'status': -1})
        out_data.update({'shelf_handle': 0})
    return(out_data)


def cmd_shelf_open(cmd_data):
    out_data = {}
    out_data.update({'status': 0})
    return(out_data)


def cmd_shelf_close(cmd_data):
    out_data = {}
    out_data.update({'status': 0})
    return(out_data)


def cmd_shelf_destroy(cmd_data):
    out_data = {}
    out_data.update({'status': 0})
    return(out_data)


def cmd_shelf_resize(cmd_data):
    out_data = {}
    out_data.update({'status': 0})
    return(out_data)


def cmd_shelf_zaddr(cmd_data):
    out_data = {}
    out_data.update({'status': 0})
    return(out_data)


def cmd_book_list(cmd_data):
    out_data = {}
    out_data.update({'status': 0})
    return(out_data)


def cmd_shelf_list(cmd_data):
    out_data = {}
    out_data.update({'status': 0})
    return(out_data)


def cmd_shelf_reservation_list(cmd_data):
    out_data = {}
    out_data.update({'status': 0})
    return(out_data)


def execute_command(cmd_data):
    cmd = cmd_data["command"]
    return(command_handlers[cmd](cmd_data))


def engine_args_init(parser):
    pass

def unknown_command_handler():
    return lambda x : json_error_creator(x)

command_handlers = {
    "version": cmd_version,
    "shelf_create": cmd_shelf_create,
    "shelf_open": cmd_shelf_open,
    "shelf_close": cmd_shelf_close,
    "shelf_destroy": cmd_shelf_destroy,
    "shelf_resize": cmd_shelf_resize,
    "shelf_zaddr": cmd_shelf_zaddr,
    "book_list": cmd_book_list,
    "shelf_list": cmd_shelf_list,
    "shelf_reservation_list": cmd_shelf_reservation_list
    }

if __name__ == '__main__':

    import argparse
    import database

    LIBRARIAN_VERSION = "Librarian v0.01"

    # cmd_version
    data_in = {"command": "version"}
    data_out = execute_command(data_in)
    print ("data_in  =",  data_in)
    print ("data_out =",  data_out)

    # setup database and acquire handle
    parser = argparse.ArgumentParser()
    database.db_args_init(parser)
    args = parser.parse_args()

    # Initialize database and check tables
    db = database.LibrarianDB()
    db.db_init(args)
    db.check_tables()

    # cmd_create_shelf
    shelf_owner = 0x1010101010101010
    data_in = {"command": "shelf_create", "shelf_owner": shelf_owner}
    data_out = execute_command(data_in)
    print ("data_in  =",  data_in)
    print ("data_out =",  data_out)
