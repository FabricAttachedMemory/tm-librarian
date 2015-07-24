#!/usr/bin/python3 -tt

import socket_handling
from librarian_chain import Librarian_Chain

from pdb import set_trace
from pprint import pprint

import cmdproto

#--------------------------------------------------------------------------

def main():
    lcp = cmdproto.LibrarianCommandProtocol()

    chain = Librarian_Chain()
    client = socket_handling.Client()   # socket_handling.py
    try:
        client.connect()
        print('Connected')
    except Exception as e:
        print(str(e), '; falling back to local print')
        client = None

    # Right now it's just identification, not auth[entication|orization]
    auth = {
        'uid': os.geteuid(),
        'gid': os.getegid(),
        'pid': os.getpid(),
        'node_id': 999
    }

    while True:
        user_input_list = input("command> ").split(' ')
        try:
            assert user_input_list, 'Missing command'
            command = user_input_list.pop(0)
            if not command:
                continue
            if command == 'help':
                print(lcp.help)
                print('q or quit to end the session')
                continue
            if command in ('adios', 'bye', 'exit', 'quit', 'q'):
                break

            # Two forms
            cmdict = lcp(command, *user_input_list)
            pprint(cmdict)
            print()

            cmdict = lcp(command, *user_input_list, auth=auth)
            pprint(cmdict)
            print()

        except Exception as e:
            print('Bad command:', str(e))
            continue

        out_string = chain.forward_traverse(cmdict)

        if client is None:
            print('LOCAL:', out_string)
        else:
            print(client.send_recv(out_string))
        print()

if __name__ == '__main__':
    import os

    main()
