#!/usr/bin/python3 -tt
""" Main module  for the REPL client for the librarian """

import copy
import time
from pdb import set_trace
from pprint import pprint

import socket_handling

import cmdproto

verbose = True

#--------------------------------------------------------------------------

def setup_command_generator(uil_repeat=None, script=None):
    if script is None: # never raise StopIteration.  Caller watches loop counter
        while True:
            if uil_repeat:
                yield copy.copy(uil_repeat)
            else:
                yield input("command> ").strip().split(' ')

    # Let end-of-script raise StopIteration
    with open(script, 'r') as f:
        script = f.readlines()
    for line in script:
        line = line.strip() # gets EOL newline, too
        if line and line[0] != '#':
             yield line.split()

#--------------------------------------------------------------------------

def main(serverhost='localhost'):
    """Optionally connect to the server, then wait for user input to
       send to the server, printing out the response."""

    # Right now it's just identification, not auth[entication|orization]
    context = {
        'uid': os.geteuid(),
        'gid': os.getegid(),
        'pid': os.getpid(),
        'node_id': 999
    }
    lcp = cmdproto.LibrarianCommandProtocol(context)

    client = socket_handling.Client(selectable=False)
    try:
        client.connect(host=serverhost)
        print('Connected')
    except Exception as e:
        print(str(e), '; falling back to local print')
        client = None

    script = None
    loop = 1
    delay = 0.0
    verbose = True
    uigen = setup_command_generator()
    while True:
        try:
            user_input_list = next(uigen)
            command = user_input_list[0]

            # Directives
            if command in ('adios', 'bye', 'exit', 'quit', 'q'):
                break
            if command == 'help':
                print(lcp.help)
                print('\nAux commands:')
                print('loop count delay_secs real_command ...')
                print('script filename')
                print('verbose on|off')
                print('q or quit')
                continue
            if command == 'verbose':
                verbose = user_input_list[0].lower() == 'on'
                continue
            if command == 'loop':
                loop = int(user_input_list[1])
                assert loop > 1, 'Invalid loop count'
                delay = float(user_input_list[2])
                assert delay >= 0.0, 'Invalid delay'
                uigen = setup_command_generator(
                    uil_repeat=user_input_list[3:])
                continue
            if command == 'script':
                script = user_input_list[1]
                uigen = setup_command_generator(script=script)
                continue

            # Construct and execute the command
            cmdict = lcp(command, *user_input_list[1:])
            if verbose:
                print()
                pprint(cmdict)
            if client:
                client.send_all(cmdict)
                rspdict = client.recv_chunk()
                if verbose:
                    pprint(rspdict['value'])
            print()
            time.sleep(delay)

            if not script:
                loop -= 1
                if loop < 1:
                    raise StopIteration

        except StopIteration:       # loop or script finished
            if script:
                loop -= 1
                if loop > 0:
                    uigen = setup_command_generator(script=script)
                    continue
                script = None
            loop = 1
            delay = 0.0
            uigen = setup_command_generator()
        except EOFError:            # Ctrl-D
            break
        except KeyboardInterrupt:   # Ctrl-C
            uigen = setup_command_generator()
        except Exception as e:
            set_trace()
            print('Line %d: bad command? %s' % (
                sys.exc_info()[2].tb_lineno, str(e)))


if __name__ == '__main__':
    import os, sys

    main(sys.argv[1])
