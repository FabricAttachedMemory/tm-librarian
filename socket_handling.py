#!/usr/bin/python3 -tt
""" Module to handle socket communication for Librarian and Clients """
import errno
import socket
import select
import sys
import time

from pdb import set_trace

from librarian_chain import BadChainUnapply


class SocketReadWrite(object):
    """ Object that will read and write from a socket
    used primarily as a base class for the Client and Server
    objects """

    def __init__(self, **kwargs):
        peertuple = kwargs.get('peertuple', None)
        self._perf = kwargs.get('perf', 0)
        selectable = kwargs.get('selectable', True)
        sock = kwargs.get('sock', None)

        if sock is None:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self._sock = sock
        self._sock.setblocking(not selectable)

        if peertuple is None:
            self._peer = ''
            self._port = 0
            self._str = ''
        else:
            self._peer, self._port = peertuple
            self._str = '{0}:{1}'.format(*peertuple)
        self.inbuf = ''
        self.appended = 0

    def __str__(self):
        return self._str

    def close(self):
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except Exception as e:
            pass
        self._sock.close()

    def fileno(self):
        '''Allows this object to be used in select'''
        return self._sock.fileno()

    def send_all(self, outstr):
        """ Encode and send outstr along the socket.

        Args:
            outstr: the python3 string to be sent
            sock: The socket to send the string on.

        Returns:
                Nothing.
        """
        # FIXME: accept a "chain" in init
        return self._sock.sendall(str.encode(outstr))

    _bufsz = 4096

    def recv_chunk(self):
        """ Receive the next part of a message and decode it to a
            python3 string. FIXME what about chain() stuff?
        Args:
        Returns:
        """

        # FIXME: accept a "chain" in init, then I can do the
        # chain decode/error loop in here.
        last = len(self.inbuf)
        self.inbuf += self._sock.recv(self._bufsz).decode("utf-8").strip()
        self.appended = len(self.inbuf) - last
        if not self._perf:
            print('%s: received %d bytes' % (self._str, self.appended))

    def clear(self):
        self.inbuf = ''
        self.appended = 0

    def send_recv(self, outstring):
        """ Send and receive all data.

        Args:
            outstring: String to be sent.
            sock: The socket to send, then receive from.

        Returns:
            The string received as a response to what was sent.
        """

        if not outstring:
            return None

        self.send_all(outstring)
        self.recv_chunk()


class Client(SocketReadWrite):
    """ A simple synchronous client for the Librarian """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def connect(self, host='localhost', port=9093, retry=True):
        """ Connect socket to port on host

        Args:
            host: the host to connect to
            port: the port to connect to

        Returns:
            Nothing
        """

        while True:
            try:
                self._sock.connect((host, port))
            except Exception as e:
                if retry:
                    print('Retrying connection...')
                    time.sleep(2)
                    continue
                raise
            try:
                peertuple = self._sock.getpeername()
            except Exception as e:
                if retry:
                    print('Retrying getpeername...')
                    time.sleep(2)
                    continue
                raise

            self._peer, self._port = peertuple
            self._str = '{0}:{1}'.format(*peertuple)
            return

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
        self._sock.bind(('', self._port))
        self._sock.listen(20)
        self.verbose = args.verbose

    def accept(self):
        return self._sock.accept()

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

    def serv(self, handler, chain):
        """ "event-loop" for the server this is where the server starts
        listening and serving requests.

        Args:
            handler: The handler for commands received by the server.
            chain: The chain to convert to strings from the socket into usable
                ojects.
        Returns:
            Nothing.
        """

        to_read = [self]
        t0 = time.time()
        xlimit = 2000
        transactions = 0

        # FIXME: put "chain" in SocketRW, move this routine into that class
        def send_result(s, result):
            try:    # socket may have died by now
                bytesout = chain.forward_traverse(result)
                if self.verbose:
                    print('%s: sending %s' % (s,
                        'NULL' if result is None
                               else '%d bytes' % len(bytesout)))
                s.send_all(bytesout)
            except OSError as e:
                if e.errno == errno.EPIPE:
                    msg = 'closed by client'
                elif e.errno != errno.EBADF:
                    msg = 'closed earlier'
                print ('%s: %s' % (s, msg), file=sys.stderr)
                to_read.remove(s)
                s.close()
            except Exception as e:
                print('%s: SEND failed: %s' % (s, str(e)),
                      file=sys.stderr)

        while True:

            if self.verbose:
                print('Waiting for request...')
            try:
                readable, _, _ = select.select(to_read, [], [], 10.0)
            except Exception as e:
                # Usually ValueError on a negative fd from a remote close
                dead = [sock for sock in to_read if sock.fileno() == -1]
                assert self not in dead, 'Server socket has died'
                for d in dead:
                    if d in to_read:    # avoid error
                        to_read.remove(d)
                continue

            if not readable: # timeout: respond to partials, reset counters
                for s in to_read:
                    if s.inbuf:    # No more is coming FIXME: too harsh?
                        print('%s: reset inbuf' % s)
                        s.inbuf = ''
                        send_result(s, None)
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

                if s is self: # New connection
                    try:
                        (sock, peertuple) = self.accept()
                        newsock = SocketReadWrite(
                            sock=sock, peertuple=peertuple)
                        to_read.append(newsock)
                        print('%s: new connection' % newsock)
                    except Exception as e:
                        pass
                    continue

                try:
                    # Accumulate partial messages until a parse works.
                    # FIXME: chain should return tuple (cmdict, leftovers)

                    s.recv_chunk()
                    cmdict = chain.reverse_traverse(s.inbuf)
                    # Since it parsed, the message is complete.
                    # FIXME: give "chain" to socket class, do this there
                    s.clear()
                    if self.verbose:
                        if self.verbose == 1:
                            print('%s: %s' % (s, cmdict['command']))
                        else:
                            print('%s: %s' % (s, str(cmdict)))
                    result = handler(cmdict)
                except Exception as e:
                    result = None
                    if isinstance(e, BadChainUnapply):
                        if s.appended:  # got more this last pass
                            continue
                        msg = 'null command'
                    elif e.__class__ in (AssertionError, RuntimeError):
                        msg = str(e) # self-detected at lower levels
                    elif isinstance(e, TypeError):
                        msg = 'socket death'
                    else:
                        msg = 'UNEXPECTED %s' % str(e)
                    print('%s: %s' % (s, msg))

                # NO "finally": it circumvents "continue" in error clause(s)
                send_result(s, result)

def main():
    """ Run simple echo server to exercise the module """

    import json

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
