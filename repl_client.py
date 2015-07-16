#!/usr/bin/python3 -tt

from collections import defaultdict
import socket_handling
import json
import json_handler

def repl_init():
    socket_handling.client_init()
    sys

def command_quit(var):
    print("bye")
    return None

def command_help(var):
    print("help - list supported commands")
    print("version - query Librarian for current version")
    print("book_list <rowid> - list book details by book rowid")
    print("book_listall - list book details for all books in database")
    print("shelf_list - list shelf details by shelf name")
    print("shelf_listall - list shelf details for all shelves in database")
    print("shelf_create <shelf_name> <shelf_owner> - create new shelf")
    print("shelf_resize <shelf_name> <size_in_bytes> - resize shelf to given size in bytes, add/remove books")
    print("shelf_destroy <shelf_name> - destroy shelf and free reserved books")
    print("shelf_open <shelf_name> - open shelf and setup node access")
    print("shelf_close <shelf_name> - close shelf and tear down node access")
    return ""

def command_version(command):
    command_dict = {}
    command_dict["command"] = "version"
    return(command_dict)

def command_book_list(command):
    command_dict['command'] = "book_list"
    command_dict['rowid'] = command[1]
    return(command_dict)

def command_shelf_list(command):
    command_dict = {}
    command_dict['command'] = "shelf_list"
    command_dict['shelf_name'] = command[1]
    return(command_dict)

def command_shelf_listall(command):
    command_dict = {}
    command_dict['command'] = "shelf_reservation_list"
    return(command_dict)

def command_shelf_create(command):
    command_dict = {}
    command_dict['command'] = "shelf_create"
    command_dict['shelf_name'] = command[1]
    command_dict['shelf_owner'] = command[2]
    return(command_dict)

def command_shelf_resize(command):
    command_dict = {}
    command_dict['command'] = "shelf_resize"
    command_dict['shelf_name'] = command[1]
    command_dict['size_bytes'] = command[2]
    return(command_dict)

def command_shelf_destroy(command):
    command_dict = {}
    command_dict['command'] = "shelf_destroy"
    command_dict['shelf_name'] = command[1]
    return(command_dict)

def command_shelf_open(command):
    command_dict = {}
    command_dict['command'] = "shelf_open"
    command_dict['shelf_name'] = command[1]
    command_dict['res_owner'] = command[2]
    return(command_dict)

def command_shelf_close(command):
    command_dict = {}
    command_dict['command'] = "shelf_close"
    command_dict['shelf_name'] = command[1]
    command_dict['res_owner'] = command[2]
    return(command_dict)

def command_shelf_reservation_list(command):
    command_dict = {}
    command_dict['command'] = "shelf_reservation_list"
    return command_dict

# This is to stop the send currently this will be improved at a later date
def json_error_creator(val):
    print("Invlaid Command")
    return ""

def unknown_command_handler():
    return lambda x : json_error_creator(x)


command_handlers = defaultdict(unknown_command_handler)

command_handlers.update({
        "version":command_version,
        "shelf_create": command_shelf_create,
        "shelf_open": command_shelf_open,
        "shelf_close": command_shelf_close,
        "shelf_destroy": command_shelf_destroy,
        "shelf_resize": command_shelf_resize,
        "book_list": command_book_list,
        "shelf_list": command_shelf_list,
        "shelf_reservation_list": command_shelf_reservation_list,
        "help" : command_help,
        "quit" : quit
        })

command_line_helpers = defaultdict(unknown_command_handler)

def main():

    client = socket_handling.Client()
    client.connect()

    processor = json_handler.Processor()
    processor.add_processor(json.dumps)

    while True:
        user_input_list = input("command> ").split(' ')
        command = user_input_list[0]

        msg = command_handlers[command](user_input_list)
        if msg is None:
            break
        if msg is "":
            continue

        out_string = processor.process(msg)
        print(client.send_recv(out_string))

if __name__ == '__main__':
    main()
