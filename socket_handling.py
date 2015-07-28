#!/usr/bin/python3 -tt
""" Module to handle socket communication for Librarian and Clients """
import errno
import socket
import select

from pdb import set_trace

class SocketReadWrite():
    """ Object that will read and write from a socket
    used primarily as a base class for the Client and Server
    objects """

    _sock = None

    def __init__(self):
        self._sock = socket.socket()

    def __del__(self):
        self._sock.close()

    # should be static helper in base class called by child class
    def send(self, string, sock=None):
        """ Encode and send end string along the socket.

        Args:
            string: the python3 string to be sent
            sock: The socket to send the string on.

        Returns:
                Nothing.
        """

        if sock is None:
            sock = self._sock

        sock.send(str.encode(string))

    # should be static helper in base class called by child class
    def recv_all(self, sock=None):
        """ Receive the whole message and decode it to a python3 string.

        Args:
            sock: the socket to receive data from.

        Returns:
            The python3 string that was recieved from the socket.
        """

        if sock is None:
            sock = self._sock

        in_json = ""
        in_delta = sock.recv(1024).decode("utf-8").strip()
        in_json += in_delta

        while not len(in_delta) != 1024:
            in_delta = sock.recv(1024).decode("utf-8").strip()
            in_json += in_delta

        # should never have to worry about this
        # python will set the descriptor to -1 when it closes stopping us from
        # having to read 0. But this is select/socket behavior so we need to
        # account for it.
        if len(in_json) == 0:
            sock.close()
            return None

        return in_json

    # should be static helper in base class called by child class
    def send_recv(self, string, sock=None):
        """ Send and receive all data.

        Args:
            string: String to be sent.
            sock: The socket to send, then receive from.

        Returns:
            The string received as a response to what was sent.
        """

        if sock is None:
            sock = self._sock

        if len(string) == 0:
            sock.close()
            return None

        # This kills the socket
        if string == "":
            return None

        # maybe do a proper error but w/e all of this will be replaced
        # by a proper event loop eventually
        if sock == None:
            return None

        self.send(string, sock)
        return self.recv_all(sock)


class Client(SocketReadWrite):
    """ A simple synchronous client for the Librarian """
    _sock = None

    def __init__(self):
        super().__init__()

    def __del__(self):
        super().__del__()

    def connect(self, host='', port=9093):
        """ Connect socket to port on host

        Args:
            host: the host to connect to
            port: the port to connect to

        Returns:
            Nothing
        """

        self._sock.connect((host, port))


class Server(SocketReadWrite):
    """ A simple asynchronous server for the Librarian """

    @staticmethod
    def argparse_extend(parser):
        """ Method to add the arguments server expects to the parser

        Args:
            parser: the parse to add the arguments too.

        Returns:
            Nothing.
        """

        # group = parser.add_mutually_exclusive_group()
        parser.add_argument('--port',
                            help='TCP listening port',
                            type=int,
                            default=9093)

    _sock = None

    def __init__(self, args):
        super().__init__()
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._port = args.port
        self.verbose = args.verbose

    def __del__(self):
        super().__del__()

    # the default processor is an identity processor so it's probably
    # a requirement to have this, it could just initialize and use a brand
    # new processor which is exactly what somone who didn't want processing
    # would provide

    # "Weird" event loop exit behavior is explained in Python source:
    # https://github.com/python/cpython/blob/Master/modules/socketmodule.c#L2530
    # The remote side of a socket may get closed at any time, whether gracefuli
    # or not.  Linux sees it first, before the Python socket module (duh).  A
    # system-call read on a dead socket yields EBADF.  Python tries to do the
    # same thing.  In pure C, The right thing to do is close the socket;
    # however, the kernel may release the fd for reuse BEFORE the syscall
    # returns.   The Python socket module still has the old fd, so on the
    # first EBADF, "socketmodule" first puts -1 in the internal Python
    # fd which will force EBADF.  Finally it closes the real socket fd.
    # Hence the explicit searches for fileno == -1 below.

    def serv(self, handler, chain, interface = ''):
    # would provid
    def serv(self, handler, chain, interface=''):
        """ "event-loop" for the server this is where the server starts
        listening and serving requests.

        Args:
            handler: The handler for commands received by the server.
            chain: The chain to convert to strings from the socket into usable
                ojects.
        Returns:
            Nothing.
        """

        self._sock.bind((interface, self._port))
        self._sock.listen(10)

        to_read = [self._sock]

        while True:

            # There are few enough connections to where this should
            # not be a performance problem.
            to_read = [sock for sock in to_read if sock.fileno() != -1]

            if self.verbose:
                print('Waiting for request(s)')
                timeout = 5.0
            else:
                timeout = 20.0 # hits the cleanup loop

            readable, _, _ = select.select(to_read, [], [], timeout)

            # Is it a new connection?
            try:
                if self._sock in readable:
                    if self.verbose: print('New connection')
                    (conn, _) = self._sock.accept()
                    to_read.append(conn)
                    readable.remove(self._sock) # and fall through for others
            except ValueError:
                if verbose:
                    print('SELECT: socket closed from afar, maybe no FIN/RST')
                continue
            except Exception as e:
                print('SELECT: ', str(e))
                set_trace()
                continue

            for s in readable:
                try:
                    in_string = self.recv_all(s)
                    cmdict = chain.reverse_traverse(in_string)
                    if self.verbose:
                        if self.verbose == 1:
                            print('Processing ',
                                s.getpeername(), cmdict['command'])
                        else:
                            print('Processing ',
                                s.getpeername(), str(cmdict))
                    result = handler(cmdict)
                except RuntimeError as e:   # handler self-detected fault
                    print('HANDLER: ', str(e))
                    result = None
                except TypeError as e:
                    print('SOCKET CLIENT: socket death, maybe bad JSON')
                    result = None
                except Exception as e:
                    print('SOCKET CLIENT: ', str(e))
                    result = None
                    set_trace()
                    pass
                finally:
                    try:    # socket may have died by now
                        self.send(chain.forward_traverse(result), s)
                    except OSError as e:
                        if e.errno != errno.EBADF:
                            print('SOCKET CLIENT: final reply: ', str(e))
                            set_trace()
                            pass
                    except Exception as e:
                        print('SOCKET CLIENT: final reply: ', str(e))
                        set_trace()
                        pass


def main():
    """ Run simple echo server to exercise the module """

    import json
    import time

    def echo_handler(string):
        """ Echo handler for use with testing server.

        Args:
            string: a JSON string.

        Returns:
            Dictionary representing the json string.
        """
        # Does not handle ctrl characters well.  Needs to be modified
        # for dictionary IO
        print(string)
        return json.dumps({'status': 'Processed @ %s' % time.ctime()})

    from function_chain import IdentityChain
    chain = IdentityChain()
    server = Server(None)
    print('Waiting for connection...')
    server.serv(echo_handler, chain)

if __name__ == "__main__":
    main()

