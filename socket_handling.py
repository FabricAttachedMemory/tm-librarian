#!/usr/bin/python3 -tt
""" Module to handle socket communication for Librarian and Clients """
import errno
import socket
import select
import time

from pdb import set_trace


class SocketReadWrite(object):
    """ Object that will read and write from a socket
    used primarily as a base class for the Client and Server
    objects """

    _sock = None

    def __init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setblocking(False)


    def __del__(self):
        self._sock.shutdown(socket.SHUT_RDWR)
        self._sock.close()

    @staticmethod
    def send_all(string, sock):
        """ Encode and send end string along the socket.

        Args:
            string: the python3 string to be sent
            sock: The socket to send the string on.

        Returns:
                Nothing.
        """

        return sock.sendall(str.encode(string))

    _bufsz = 4096

    @classmethod
    def recv_all(cls, sock):
        """ Receive the whole message and decode it to a python3 string.
            The hope is that a message does not end on a bufsz boundary
            or this will hang.

        Args:
            sock: the socket to receive data from.

        Returns:
            The python3 string that was recieved from the socket.
        """

        in_delta = sock.recv(cls._bufsz).decode("utf-8").strip()
        in_json = in_delta

        while len(in_delta) == cls._bufsz:
            in_delta = sock.recv(cls._bufsz).decode("utf-8").strip()
            in_json += in_delta

        return in_json

    @classmethod
    def send_recv(cls, outstring, sock):
        """ Send and receive all data.

        Args:
            outstring: String to be sent.
            sock: The socket to send, then receive from.

        Returns:
            The string received as a response to what was sent.
        """

        if not outstring:
            return None

        cls.send_all(string, sock)
        return cls.recv_all(sock)


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

    # The default processor is an identity processor so it's probably
    # a requirement to have this, it could just initialize and use a brand
    # new processor which is exactly what someone who didn't want processing
    # would provide.

    # "Weird" event loop exit behavior is explained in Python source:
    # https://github.com/python/cpython/blob/
    #     Master/modules/socketmodule.c#L2530
    # The remote side of a socket may get closed at any time, whether graceful
    # or not.  Linux sees it first, before the Python socket module (duh).  A
    # system-call read on a dead socket yields EBADF.  Python tries to do the
    # same thing.  In pure C, The right thing to do is close the socket;
    # however, the kernel may release the fd for reuse BEFORE the syscall
    # returns.  The Python socket module still has the old fd, so on the
    # first EBADF, "socketmodule" first puts -1 in the internal Python
    # fd which will force EBADF.  Finally it closes the real socket fd.
    # Hence the explicit searches for fileno == -1 below.

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

        # When a socket is closed, getpeername() fails.  Save the peername
        # ASAP, and do it here because sockets have __slots__
        sock2peer = { }
        to_read = [self._sock]
        t0 = time.time()
        xlimit = 2000
        transactions = 0
        while True:

            if self.verbose:
                print('Waiting for request...')
            try:
                readable, _, _ = select.select(to_read, [], [], 10.0)
            except Exception as e:
                # Usually ValueError on a negative fd from a remote close
                dead = [sock for sock in to_read if sock.fileno() == -1]
                assert self._sock not in dead, 'Server socket has died'
                for d in dead:
                    if d in to_read:    # avoid error
                        to_read.remove(d)
                    del sock2peer[d]
                continue

            if not readable:    # timeout; reset counters
                transactions = 0
                t0 = time.time()
                continue

            for s in readable:
                transactions += 1

                if transactions > xlimit:
                    deltat = time.time() - t0
                    tps = int(float(transactions) / deltat)
                    print('%d transactions/second' % tps)
                    transactions = 0
                    t0 = time.time()

                if s is self._sock: # New connection, save name now
                    try:
                        (conn, peername) = self._sock.accept()
                        to_read.append(conn)
                        sock2peer[conn] = '{0}:{1}'.format(*peername)
                        print('%s: new connection' % sock2peer[conn])
                    except Exception as e:
                        pass
                    continue

                try:
                    in_string = self.recv_all(s)
                    assert in_string, 'null command'
                    cmdict = chain.reverse_traverse(in_string)
                    if self.verbose:
                        if self.verbose == 1:
                            print('Processing ', sock2peer[s], cmdict['command'])
                        else:
                            print('Processing ', sock2peer[s], str(cmdict))
                    result = handler(cmdict)
                except Exception as e:   # self-detected
                    result = None
                    if e.__class__ in (AssertionError, RuntimeError):
                        msg = str(e) # self-detected at lower levels
                    elif isinstance(e, TypeError):
                        msg = 'socket death'
                    elif isinstance(e, ValueError):
                        msg = 'unparseable command >>> %s <<<' % in_str
                    else:
                        msg = 'UNEXPECTED %s' % str(e)
                    print('%s: %s' % (sock2peer[s], msg))
                finally:
                    try:    # socket may have died by now
                        self.send_all(chain.forward_traverse(result), s)
                    except OSError as e:
                        if e.errno == errno.EPIPE: print(
                            '%s: closed by client' % sock2peer[s])
                        elif e.errno != errno.EBADF:
                            print('%s: closed earlier' % sock2peer[s])
                        s.close()
                    except Exception as e:
                        print('SEND to %s failed: %s' % (sock2peer[s], str(e)))

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
