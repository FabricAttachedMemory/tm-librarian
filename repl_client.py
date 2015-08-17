#!/usr/bin/python3 -tt
""" Main module  for the REPL client for the librarian """

import time
from pdb import set_trace
from pprint import pprint

import socket_handling

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

    client = socket_handling.Client(selectable=False)
    try:
        client.connect(host=serverhost)
        print('Connected')
    except Exception as e:
        print(str(e), '; falling back to local print')
        client = None

    while True:
        loop = 1
        delay = 0.0
        verbose = True
        scripting = False
        try:
            user_input_list = input("command> ").split(' ')
            assert user_input_list, 'Missing command'
            command = user_input_list.pop(0)
            if not command:
                continue
            if command == 'help':
                print(lcp.help)
                print('\nAux commands:')
                print('loop count|"forever" delay_secs real_command ...')
                print('script filename execute commands from filename')
                print('quiet|verbose control printing of response')
                print('q or quit to end the session')
                continue

            if command in ('adios', 'bye', 'exit', 'quit', 'q'):
                break
            if command == 'quiet':
                verbose = False
                continue
            if command == 'verbose':
                verbose = True
                continue
            if command == 'loop':
                loop = user_input_list.pop(0)
                if loop != 'forever':
                    loop = int(loop)
                delay = float(user_input_list.pop(0))
                command = user_input_list.pop(0)

            # Two forms
            cmdict = lcp(command, *user_input_list)
            if verbose:
                print()
                pprint(cmdict)

            #cmdict = lcp(command, *user_input_list, auth=auth)
            #pprint(cmdict)
            #print()

        except KeyboardInterrupt:
            break
        except Exception as e:
            print('Bad command:', str(e))
            continue

        if client is None:
            pprint('LOCAL:', cmdict)
        else:
            try:
                while loop == 'forever' or loop > 0:
                    print(loop) # need to see small indication of progress
                    client.send_all(cmdict)
                    rspdict = client.recv_chunk(selectable=False)
                    if verbose:
                        pprint(rspdict['value'])
                    time.sleep(delay)
                    if isinstance(loop, int):
                        loop -= 1
            except Exception as e:
                print('Oops: ', str(e), '\n')
            except KeyboardInterrupt:
                pass
        print()

if __name__ == '__main__':
    import os, sys

    main(sys.argv[1])
