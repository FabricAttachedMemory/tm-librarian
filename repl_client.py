#!/usr/bin/python3 -tt

import socket_handling


def repl_init():
    socket_handling.client_init()

def cmd_Help():
    print("help - list supported commands")
    print("version - query Librarian for current version")
    print("listbook <rowid> - list book details by book rowid")
    print("listbookall - list book details for all books in database")
    print("listshelf - list shelf details by shelf name")
    print("listshelfall - list shelf details for all shelves in database")
    print("createshelf <shelf_name> <shelf_ownner> - create new shelf")
    print("resizeshelf <shelf_name> <size_in_bytes> - resize shelf to given size in bytes, add/remove books")
    print("destroyshelf <shelf_name> - destroy shelf and free reserved books")
    print("openshelf <shelf_name> - open shelf and setup node access")
    print("closeshelf <shelf_name> - close shelf and teardown node access")
    return()

def cmd_Version(cmd):
    print("cmd_Version()")
    jcmd = '{"cmd":"version"}'
    return(jcmd)

def cmd_ListBook(cmd):
    print("cmd_ListBook()")
    jcmd = '{"cmd":"listbook","rowid":"' + cmd[1] + '"}'
    return(jcmd)

def cmd_ListBookAll(cmd):
    print("cmd_ListBookAll()")
    jcmd = '{"cmd":"listbookall"}'
    return(jcmd)

def cmd_ListShelf(cmd):
    print("cmd_ListShelf()")
    jcmd = '{"cmd":"listshelf","shelf_name":"' + cmd[1] + '"}'
    return(jcmd)

def cmd_ListShelfAll(cmd):
    print("cmd_ListShelfAll()")
    jcmd = '{"cmd":"listshelfall"}'
    return(jcmd)

def cmd_CreateShelf(cmd):
    print("cmd_CreateShelf()")
    jcmd = '{"cmd":"createshelf","shelf_name":"' + cmd[1] + '","shelf_owner":"' + cmd[2] + '"}'
    return(jcmd)

def cmd_ResizeShelf(cmd):
    print("cmd_ResizeShelf()")
    jcmd = '{"cmd":"resizeshelf","shelf_name":"' + cmd[1] + '","size_bytes":"' + cmd[2] + '"}'
    return(jcmd)

def cmd_DestroyShelf(cmd):
    print("cmd_DestroyShelf()")
    jcmd = '{"cmd":"destroyshelf","shelf_name":"' + cmd[1] + '"}'
    return(jcmd)

def cmd_OpenShelf(cmd):
    print("cmd_OpenShelf()")
    jcmd = '{"cmd":"openshelf","shelf_name":"' + cmd[1] + '","res_owner":"' + cmd[2] + '"}'
    return(jcmd)

def cmd_CloseShelf(cmd):
    print("cmd_CloseShelf()")
    jcmd = '{"cmd":"closeshelf","shelf_name":"' + cmd[1] + '","res_owner":"' + cmd[2] + '"}'
    return(jcmd)

def cmd_ListResAll(cmd):
    print("cmd_ListResAll()")
    jcmd = '{"cmd":"listresall"}'
    return(jcmd)


command_handlers = {
        "version":cmd_Version,
        "listbook": cmd_ListBook,
        "listbookall": cmd_ListBookAll,
        "listshelf": cmd_ListShelf,
        "listshelfall": cmd_ListShelfAll,
        "createshelf": cmd_CreateShelf,
        "resizeshelf": cmd_ResizeShelf,
        "destroyshelf": cmd_DestroyShelf,
        "openshelf": cmd_OpenShelf,
        "closeshelf": cmd_CloseShelf,
        "listresall": cmd_ListResAll
        }

def main():
    socket_handling.client_init()

    while True:
        user_input_list = input("cmd> ").split(' ')
        cmd = user_input_list[0]

        if cmd == "quit":
            print("bye")
            break
        elif cmd == "help":
            cmd_Help()
            continue
        else:
            try:
                msg = command_handlers[cmd](user_input_list)
            except KeyError:
                print("Unrecognized command")
                continue

            socket_handling.client_send_recv(msg)

if __name__ == '__main__':
    main()
