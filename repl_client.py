#!/usr/bin/python3 -tt
""" Main module  for the REPL client for the librarian """

import time
from pdb import set_trace
from pprint import pprint

import socket_handling
from librarian_chain import LibrarianChain

import cmdproto

#--------------------------------------------------------------------------


def main(serverhost='localhost'):
    """ Main function for the REPL client this functions connects to the
    server waits for user input and then sends/recvs commands from ther server
    """
    # Right now it's just identification, not auth[entication|orization]
    auth = {
        'uid': os.geteuid(),
        'gid': os.getegid(),
        'pid': os.getpid(),
        'node_id': 999
    }

    lcp = cmdproto.LibrarianCommandProtocol(auth)

    chain = LibrarianChain()
    client = socket_handling.Client(selectable=False)
    try:
        client.connect(host=serverhost)
        print('Connected')
    except Exception as e:
        print(str(e), '; falling back to local print')
        client = None

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

            if command == 'death_by_OOB':
                cmdict = lcp('send_OOB', 'DEATH BY OOB')
                set_trace()
                while True:
                    rspdict = client.send_recv(cmdict, chain)
                    pprint(rspdict)
                    time.sleep(0.5)

            # Two forms
            cmdict = lcp(command, *user_input_list)
            pprint(cmdict)
            print()

            #cmdict = lcp(command, *user_input_list, auth=auth)
            #pprint(cmdict)
            #print()

        except Exception as e:
            print('Bad command:', str(e))
            continue


        if client is None:
            out_string = chain.forward_traverse(cmdict)
            print('LOCAL:', out_string)
        else:
            rspdict = client.send_recv(cmdict, chain)
            pprint(rspdict)
        print()

if __name__ == '__main__':
    import os, sys

    main(sys.argv[1])
