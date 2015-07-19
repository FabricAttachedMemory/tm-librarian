#!/usr/bin/python3 -tt

from collections import defaultdict
import socket_handling
import json
import json_handler

from pdb import set_trace
from pprint import pprint

def keyvals2dict(command, values, keywords=None):
    """keywords is an ordered list that aligns with values"""
    assert isinstance(values, list), 'Didn\'t see that coming'
    command_dict = { 'command': command }
    if keywords is not None:
        assert len(values) == len(keywords), 'Keywords<->values mismatch'
        command_dict.update(dict(zip(keywords, values)))
    return command_dict

def command_version(command, values):
    """- query Librarian for current version"""
    return keyvals2dict(command, values)

def command_book_list(command, values):
    """<rowid> - list book details by book rowid"""
    return keyvals2dict(command, values, keywords=('rowid', ) )

def command_shelf_list(command, *args):
    """- list shelf details by shelf name"""
    return keyvals2dict(command, *args, keywords=('shelf_name', ) )

def command_shelf_listall(command, *args):
    """- list shelf details for all shelves in database"""
    return keyvals2dict('shelf_reservation_list', *args)

def command_shelf_create(command, *args):
    """shelf_create <shelf_name> <shelf_owner> - create new shelf"""
    return keyvals2dict(command, *args, keywords=('shelf_name', 'shelf_owner') )

def command_shelf_resize(command, *args):
    """<shelf_name> <size_in_bytes> - resize shelf to given size in bytes, add/remove books"""
    return keyvals2dict(command, *args, keywords=('shelf_name', 'size_bytes') )

def command_shelf_destroy(command, *args):
    """<shelf_name> - destroy shelf and free reserved books"""
    return keyvals2dict(command, *args, keywords=('shelf_name', ) )

def command_shelf_open(command, *args):
    """<shelf_name>  <res_owner> - open shelf and setup node access"""
    return keyvals2dict(command, *args, keywords=('shelf_name', 'res_owner') )

def command_shelf_close(command, *args):
    """<shelf_name> <res_owner> - close shelf and tear down node access"""
    return keyvals2dict(command, *args, keywords=('shelf_name', 'res_owner') )

def command_shelf_reservation_list(command, *args):
    """- show all shelves"""
    return keyvals2dict(command, *args)

# This is to stop the send currently this will be improved at a later date
def json_error_creator(val):
    print("Invlaid Command")
    return ""

def main():

    command_handlers = dict(
        [ (name[8:], func) for (name, func) in globals().items() if
            name.startswith('command_')
        ]
    )
    docstrings = [ name + ' ' + func.__doc__ for (name, func) in
        list(command_handlers.items()) ]
    assert None not in docstrings, 'Missing one or more command_ docstrings'
    docstrings = '\n'.join(sorted(docstrings))

    # client = socket_handling.Client()
    # client.connect()

    # processor = json_handler.Processor()
    # processor.add_processor(json.dumps)

    while True:
        user_input_list = input("command> ").split(' ')
        try:
            assert user_input_list, 'Missing command'
            command = user_input_list.pop(0)
            if not command:
                continue
            if command == 'help':
                print(docstrings)
                continue
            if command in ('exit', 'quit', 'q'):
                break
            handler = command_handlers[command]
        except KeyError as e:
            print('Unknown command "%s"' % command)
            continue

        try:
            # user_input_list may be an empty list but is never None
            cmdict = ''
            cmdict = handler(command, user_input_list)
        except AssertionError as e:
            print(e)
        except Exception as e:
            print('Internal error:', str(e))

        if not cmdict:
            continue

        pprint(cmdict)

        # out_string = processor.process(cmdict)
        # print(client.send_recv(out_string))

if __name__ == '__main__':
    main()
