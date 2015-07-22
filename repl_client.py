#!/usr/bin/python3 -tt

import socket_handling
from librarian_chain import Librarian_Chain

from pprint import pprint

import cmdproto

#--------------------------------------------------------------------------

def main():

    lcp = cmdproto.LibrarianCommandProtocol()

    chain = Librarian_Chain()
    client = socket_handling.Client()
    client.connect()

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
            cmdict = lcp(command, user_input_list)
        except Exception as e:
            print(str(e))
            continue

        out_string = chain.forward_traverse(cmdict)

        print(client.send_recv(out_string))

if __name__ == '__main__':
    main()
