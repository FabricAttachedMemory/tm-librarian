#!/usr/bin/python3 -tt
""" Module to handle socket communication for Librarian and Clients """
import errno
import socket
import select
import sys
import time

from pdb import set_trace

from genericobj import GenericObject
from librarian_chain import BadChainUnapply


class SocketReadWrite(object):
    """ Object that will read and write from a socket
    used primarily as a base class for the Client and Server
    objects """

    _sock = None

    def __init__(self, sock=None):
        if sock is None:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self._sock = sock
        self._sock.setblocking(False)   # always?

    def __del__(self):
        self._sock.shutdown(socket.SHUT_RDWR)
        self._sock.close()

    def fileno():
        '''Allows this object to be used in select'''
        return self._sock.fileno()

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
    def recv_chunk(cls, sock):
        """ Receive the next part of a message and decode it to a
            python3 string. FIXME what about chain() stuff?

        Args:
            sock: the socket to receive data from.

        Returns:
            The python3 string that was recieved from the socket.
        """

        return sock.recv(cls._bufsz).decode("utf-8").strip()

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

        cls.send_all(outstring, sock)
        return cls.recv_chunk(sock)


class Client(SocketReadWrite):
    """ A simple synchronous client for the Librarian """

    def __init__(self):
        super().__init__()

    def __del__(self):
        super().__del__()

    def connect(self, host='localhost', port=9093):
        """ Connect socket to port on host

        Args:
            host: the host to connect to
            port: the port to connect to

        Returns:
            Nothing
        """

        try:
            self._sock.connect((host, port))
        except OSError as e:
            if e.errno == errno.EINPROGRESS:
                # socket is non-blocking but connection is not complete.
                # So far only happens with repl client.  Weird.
                if host == 'localhost':
                    raise


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

    def __init__(self, args):
        super().__init__()
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._port = args.port
        self.verbose = args.verbose

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

        def send_result(s, peer, result):
            try:    # socket may have died by now
                bytesout = chain.forward_traverse(result)
                if self.verbose:
                    print('%s: sending %s' % (peer.name,
                        'NULL' if result is None
                               else '%d bytes' % len(bytesout)))
                self.send_all(bytesout, s)
            except OSError as e:
                if e.errno == errno.EPIPE:
                    msg = 'closed by client'
                elif e.errno != errno.EBADF:
                    msg = 'closed earlier'
                print ('%s: %s' % (peer.name, msg), file=sys.stderr)
                to_read.remove(s)
                del sock2peer[s]
                s.close()
            except Exception as e:
                print('%s: SEND failed: %s' % (peer.name, str(e)),
                      file=sys.stderr)

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

            if not readable:    # timeout: respond to partials, reset counters
                for s in to_read:
                    peer = sock2peer.get(s, False)
                    if peer and peer.inbuf:    # No more is coming
                        print('%s: reset inbuf' % peer.name)
                        peer.inbuf = ''
                        send_result(None, peer, s)
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
                        sock2peer[conn] = GenericObject(
                            name='{0}:{1}'.format(*peername),
                            inbuf=''
                        )
                        print('%s: new connection' % sock2peer[conn].name)
                    except Exception as e:
                        pass
                    continue

                try:
                    # Accumulate partial messages until a parse works.
                    # FIXME: chain should return tuple of (cmdict, leftovers)
                    peer = sock2peer[s]
                    in_string = ''      # in case the recv bombs
                    in_string = self.recv_chunk(s)
                    peer.inbuf += in_string
                    cmdict = chain.reverse_traverse(peer.inbuf)
                    # Since it parsed, the message is complete.
                    peer.inbuf = ''
                    if self.verbose:
                        if self.verbose == 1:
                            print('%s: %s' % (peer.name, cmdict['command']))
                        else:
                            print('%s: %s' % ( peer.name, str(cmdict)))
                    result = handler(cmdict)
                except Exception as e:
                    result = None
                    if isinstance(e, BadChainUnapply):
                        if in_string:
                            print('%s: appended %d bytes to inbuf' % (
                                peer.name, len(in_string)))
                            continue
                        msg = 'null command'
                    elif e.__class__ in (AssertionError, RuntimeError):
                        msg = str(e) # self-detected at lower levels
                    elif isinstance(e, TypeError):
                        msg = 'socket death'
                    else:
                        msg = 'UNEXPECTED %s' % str(e)
                    print('%s: %s' % (peer.name, msg))

                # NO "finally": it circumvents "continue" in error clause(s)
                send_result(s, peer, result)

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
