#!/usr/bin/python3 -tt

import socket_handling
import json
import json_handler

from pprint import pprint

import cmdproto

#--------------------------------------------------------------------------

def main():

    lcp = cmdproto.LibrarianCommandProtocol()

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
                print(lcp.help)
                print('q or quit to end the session')
                continue
            if command in ('adios', 'bye', 'exit', 'quit', 'q'):
                break
            cmdict = lcp(command, user_input_list)
        except Exception as e:
            print(str(e))
            continue

        pprint(cmdict)

        # out_string = processor.process(cmdict)
        # print(client.send_recv(out_string))

if __name__ == '__main__':
    main()
