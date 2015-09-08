#!/usr/bin/python3 -tt
""" Main module  for the REPL client for the librarian """

import copy
import time
from pdb import set_trace
from pprint import pprint

import socket_handling

import cmdproto

verbose = 1

#--------------------------------------------------------------------------


def setup_command_generator(uil_repeat=None, script=None):
    # Never raise StopIteration, caller watches loop counter
    if script is None:
        while True:
            if uil_repeat:
                yield copy.copy(uil_repeat)
            else:
                yield input("command> ").strip().split(' ')

    # Let end-of-script raise StopIteration
    with open(script, 'r') as f:
        script = f.readlines()
    for line in script:
        line = line.strip()  # gets EOL newline, too
        if line and line[0] != '#':
            yield line.split()

#--------------------------------------------------------------------------


def main(node_id, serverhost, onetime=None):
    """Optionally connect to the server, then wait for user input to
       send to the server, printing out the response."""

    global verbose

    # Right now it's just identification, not auth[entication|orization]
    context = {
        'uid': os.geteuid(),
        'gid': os.getegid(),
        'pid': os.getpid(),
        'node_id': node_id
    }
    lcp = cmdproto.LibrarianCommandProtocol(context)

    client = socket_handling.Client(selectable=False, verbose=verbose)
    try:
        client.connect(host=serverhost)
        if verbose > 1:
            print('Connected')
    except Exception as e:
        print(str(e), '; falling back to local print')
        client = None

    def reset():    # local helper
        nonlocal delay, loop, script, uigen
        script = None
        loop = 1
        delay = 0.0
        uigen = setup_command_generator()

    def substitute_vars(uil, node_id, prevrsp):
        for i in range(1, len(uil)):
            arg = uil[i]
            if arg[0] == '$':
                var = arg[1:]
                uil[i] = 'NONE'
                if 'value' in prevrsp:
                    uil[i] = str(prevrsp['value'].get(var, 'NONE'))
            elif arg.endswith('$NODE'):
                uil[i] = arg.replace('$NODE', '%03d' % node_id)

    reset()
    rspdict = None
    while True:
        try:
            if onetime is None or not onetime:
                user_input_list = next(uigen)
            else:
                user_input_list = copy.copy(onetime)
            substitute_vars(user_input_list, node_id, rspdict)
            command = user_input_list[0]

            # Directives
            if command in ('adios', 'bye', 'exit', 'quit', 'q'):
                break
            if command == 'help':
                print(lcp.help)
                print('\nAux commands:')
                print('loop count delay_secs real_command ...')
                print('script filename')
                print('verbose <level> (cur = %d)' % verbose)
                print('q or quit')
                continue
            if command == 'verbose':
                verbose = int(user_input_list[1])
                continue
            if command == 'loop':
                loop = int(user_input_list[1])
                assert loop > 1, 'Invalid loop count'
                delay = float(user_input_list[2])
                assert delay >= 0.0, 'Invalid delay'
                uigen = setup_command_generator(
                    uil_repeat=user_input_list[3:])
                continue
            if command == 'response':
                pprint(rspdict)
                continue
            if command == 'script':
                script = user_input_list[1]
                uigen = setup_command_generator(script=script)
                continue

            # Construct and execute the command
            cmdict = lcp(command, *user_input_list[1:])
            if verbose == 2:
                print(cmdict)
            elif verbose > 2:
                pprint(cmdict)
            if client:
                client.send_all(cmdict)
                rspdict = client.recv_all()
                if 'errmsg' in rspdict:
                    pprint(rspdict)
                if verbose == 1 or verbose == 2:
                    print(rspdict['value'])
                elif verbose > 2:
                    pprint(rspdict['value'])
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
            reset()
        except KeyError as e:
            print('Unknown command: %s' % ' '.join(user_input_list))
            reset()
        except EOFError:            # Ctrl-D
            break
        except KeyboardInterrupt:   # Ctrl-C
            reset()
        except Exception as e:
            print('Line %d: "%s" bad command? %s' % (
                sys.exc_info()[2].tb_lineno, command, str(e)))
            reset()
            pass

        if onetime:
            raise SystemExit(0)


if __name__ == '__main__':

    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(
        description='Librarian command-line client')
    parser.add_argument(
        '--verbose',
        help='level of runtime output, larger == more',
        type=int,
        default=1)
    parser.add_argument(
        'node_id',
        help='Numeric node id',
        type=int)
    parser.add_argument(
        'hostname',
        help='ToRMS host running the Librarian',
        type=str)
    parser.add_argument(
        'onetime',
        help='ONe-time command (then exit)',
        nargs='*',
        default=None)
    args = parser.parse_args(sys.argv[1:])

    # argparse "choices" expands this in help message, less than helpful
    assert 1 <= args.node_id < 1000, 'Node ID must be from 1 - 999'
    verbose = args.verbose
    main(args.node_id, args.hostname, args.onetime)
