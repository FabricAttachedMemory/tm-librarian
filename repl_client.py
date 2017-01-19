#!/usr/bin/python3 -tt
""" Main module  for the REPL client for the librarian """

# Copyright 2017 Hewlett Packard Enterprise Development LP

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2 as
# published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along
# with this program.  If not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

import copy
import time
from pdb import set_trace
from pprint import pprint

import socket_handling

import cmdproto

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


def main(physloc, serverhost, verbose, onetime=None):
    """Optionally connect to the server, then wait for user input to
       send to the server, printing out the response."""

    # Right now it's just identification, not auth[entication|orization]
    context = {
        'uid': os.geteuid(),
        'gid': os.getegid(),
        'pid': os.getpid(),
        'node_id': physloc.split(':')[2]
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

    def substitute_vars(uil, physloc, prevrsp):
        for i in range(1, len(uil)):
            arg = uil[i]
            if arg[0] == '$':
                var = arg[1:]
                uil[i] = 'NONE'
                if 'value' in prevrsp:
                    uil[i] = str(prevrsp['value'].get(var, 'NONE'))
            elif arg.endswith('$NODE'):
                uil[i] = arg.replace('$NODE', '%s' % physloc)

    reset()
    rspdict = None
    while True:
        try:
            if onetime is None or not onetime:
                user_input_list = next(uigen)
            else:
                user_input_list = copy.copy(onetime)
            substitute_vars(user_input_list, physloc, rspdict)
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
        'physloc',
        help='Node physical location "rack:enc:node"',
        type=str)
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

    verbose = args.verbose
    main(args.physloc, args.hostname, args.verbose, args.onetime)
