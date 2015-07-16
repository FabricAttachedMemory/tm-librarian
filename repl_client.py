#!/usr/bin/python3 -tt

from collections import defaultdict
import socket_handling
import json
import json_handler

def repl_init():
    socket_handling.client_init()
    sys

def cmd_quit(var):
    print("bye")
    return None

def cmd_help(var):
    print("help - list supported commands")
    print("version - query Librarian for current version")
    print("listbook <rowid> - list book details by book rowid")
    print("listbookall - list book details for all books in database")
    print("listshelf - list shelf details by shelf name")
    print("listshelfall - list shelf details for all shelves in database")
    print("createshelf <shelf_name> <shelf_owner> - create new shelf")
    print("resizeshelf <shelf_name> <size_in_bytes> - resize shelf to given size in bytes, add/remove books")
    print("destroyshelf <shelf_name> - destroy shelf and free reserved books")
    print("openshelf <shelf_name> - open shelf and setup node access")
    print("closeshelf <shelf_name> - close shelf and tear down node access")
    return ""

def cmd_version(cmd):
    cmd_dict = {}
    cmd_dict["cmd"] = "version"
    return(cmd_dict)

def cmd_listbook(cmd):
    cmd_dict['cmd'] = "listbook"
    cmd_dict['rowid'] = cmd[1]
    return(cmd_dict)

def cmd_listbookall(cmd):
    cmd_dict = {}
    cmd_dict['cmd'] = 'listbookall'
    return(cmd_dict)

def cmd_listshelf(cmd):
    cmd_dict = {}
    cmd_dict['cmd'] = "listshelf"
    cmd_dict['shelf_name'] = cmd[1]
    return(cmd_dict)

def cmd_listshelfall(cmd):
    cmd_dict = {}
    cmd_dict['cmd'] = "listshelfall"
    return(cmd_dict)

def cmd_createshelf(cmd):
    cmd_dict = {}
    cmd_dict['cmd'] = "createshelf"
    cmd_dict['shelf_name'] = cmd[1]
    cmd_dict['shelf_owner'] = cmd[2]
    return(cmd_dict)

def cmd_resizeshelf(cmd):
    cmd_dict = {}
    cmd_dict['cmd'] = "resizeshelf"
    cmd_dict['shelf_name'] = cmd[1]
    cmd_dict['size_bytes'] = cmd[2]
    return(cmd_dict)

def cmd_destroyshelf(cmd):
    cmd_dict = {}
    cmd_dict['cmd'] = "destroyshelf"
    cmd_dict['shelf_name'] = cmd[1]
    return(cmd_dict)

def cmd_openshelf(cmd):
    cmd_dict = {}
    cmd_dict['cmd']
    cmd_dict['shelf_name'] = cmd[1]
    cmd_dict['res_owner'] = cmd[2]
    return(cmd_dict)

def cmd_closeshelf(cmd):
    cmd_dict = {}
    cmd_dict['cmd'] = "closeshelf"
    cmd_dict['shelf_name'] = cmd[1]
    cmd_dict['res_owner'] = cmd[2]
    return(cmd_dict)

def cmd_listresall(cmd):
    cmd_dict = {}
    cmd_dict['cmd'] = "listresall"
    return(cmd_dict)

# This is to stop the send currently this will be improved at a later date
def json_error_creator(val):
    print("Invlaid Command")
    return ""

def unknown_command_handler():
    return lambda x : json_error_creator(x)


command_handlers = defaultdict(unknown_command_handler)

command_handlers.update({
        "version":cmd_version,
        "listbook": cmd_listbook,
        "listbookall": cmd_listbookall,
        "listshelf": cmd_listshelf,
        "listshelfall": cmd_listshelfall,
        "createshelf": cmd_createshelf,
        "resizeshelf": cmd_resizeshelf,
        "destroyshelf": cmd_destroyshelf,
        "openshelf": cmd_openshelf,
        "closeshelf": cmd_closeshelf,
        "listresall": cmd_listresall,
        "help" : cmd_help,
        "quit" : quit
        })

command_line_helpers = defaultdict(unknown_command_handler)

def main():

    client = socket_handling.Client()
    client.connect()

    processor = json_handler.Processor()
    processor.add_processor(json.dumps)

    while True:
        user_input_list = input("cmd> ").split(' ')
        cmd = user_input_list[0]

        msg = command_handlers[cmd](user_input_list)
        if msg is None:
            break
        if msg is "":
            continue

        out_string = processor.process(msg)
        client.send_recv(out_string)

if __name__ == '__main__':
    main()
