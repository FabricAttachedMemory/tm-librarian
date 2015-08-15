#!/usr/bin/python3 -tt
""" Module to handle socket communication for Librarian and Clients """

import errno
import re
import socket
import select
import sys
import time

from pdb import set_trace

from function_chain import BadChainReverse


class SocketReadWrite(object):
    """ Object that will read and write from a socket
    used primarily as a base class for the Client and Server
    objects """

    _OOBprefix = '<@<'
    _OOBsuffix = '>@>'
    _OOBformat = _OOBprefix + '%s' + _OOBsuffix
    _OOBmatch  = re.compile(_OOBprefix + '\(.*\)' + _OOBsuffix)

    def __init__(self, **kwargs):
        self._perf = kwargs.get('perf', 0)
        self.verbose = kwargs.get('verbose', 0)
        peertuple = kwargs.get('peertuple', None)
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
        else:   # A dead socket can't getpeername so cache it now
            self._peer, self._port = peertuple
            self._str = '{0}:{1}'.format(*peertuple)
        self.inbytes = bytes()
        if self._perf:
            self.verbose = 0

    def __str__(self):
        return self._str

    def close(self):
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except Exception as e:
            pass
        self._sock.close()
        self._sock = self._peer = self._port = self._str = None

    def fileno(self):
        '''Allows this object to be used in select'''
        return self._sock.fileno()

    def send_all(self, obj, chain=None):
        """ Send an object after optional transformation

        Args:
            obj: the object to be sent
            chain: the transformations from object to bytes

        Returns:
               Number of bytes sent.
        """

        if chain is None:
            outbytes = obj
        else:
            outbytes = chain.forward_traverse(obj)
        if self.verbose:
            print('%s: sending %s' % (s,
                  'NULL' if result is None
                   else '%d bytes' % len(outbytes)))
        try:
            return self._sock.sendall(outbytes)
        except OSError as e:
            if e.errno == errno.EPIPE:
                msg = 'closed by client'
            elif e.errno != errno.EBADF:
                msg = 'closed earlier'
            msg = '%s: %s' % (s, msg)
            raise OSError(errno.ECONNABORTED, msg)
        except Exception as e:
            print('%s: send_all failed: %s' % (self, str(e)),
                    file=sys.stderr)
            set_trace()
            pass
            raise

    _bufsz = 4096

    def recv_chunk(self, chain=None):
        """ Receive the next part of a message and decode it to a
            python3 string.
        Args:
        Returns:
        """

        while True:
            last = len(self.inbytes)
            try:
                self.inbytes += self._sock.recv(self._bufsz)
            except BlockingIOError as e:
                # Not ready.  get back on the select train
                return None
            except Exception as e:
                set_trace() # socket death
                raise

            appended = len(self.inbytes) - last
            if not self._perf:
                print('%s: received %d bytes' % (self._str, appended))

            if not appended:   # Far side is gone
                msg = '%s: closed by remote' % str(self)
                self.close()
                raise OSError(errno.ECONNABORTED, msg)

            if chain is None:
                return self.inbytes[last:]

            try:    # If it parses, I can be done
                result = chain.reverse_traverse(self.inbytes)
                self.clear()
                return result
            except BadChainReverse:
                if not appended:  # did NOT get more, check OOB
                    set_trace()
                    raise


    def clear(self):
        self.inbytes = bytes()

    def send_recv(self, obj, chain=None):
        """ Send and receive all data.

        Args:
            obj: String to be sent.
            chain: the forward/reverse chains of translation

        Returns:
            The string received as a response to what was sent.
        """

        self.send_all(bytes, chain)
        return self.recv_chunk(chain)

    def send_OOB(self, OOBmsg):
        OOBytes = (self.__OOBfmt % OOBmsg).encode()
        set_trace()
        self.send_all(OOBytes)

    def recv_OOB(self):
        '''Check for an unsolicited inbound message when using
           blocking sockets in a cmd/rsp pairing.'''
        try:
            self._sock.settimeout(0.001)  # non-blocking
            OOB = self._sock.recv(self._bufsz)
        except socket.timeout:
            OOB = ''
        finally:
            self._sock.settimeout(None)   # blocking
        return OOB

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

    def __init__(self, parseargs, **kwargs):
        self.verbose = parseargs.verbose
        super().__init__(**kwargs)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._port = parseargs.port
        self._sock.bind(('', self._port))
        self._sock.listen(20)

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

        # Consolidate error handling.  Closure on "chain" and "clients"
        def send_result(s, result):
            try:
                s.send_all(result, chain)
            except Exception as e:
                print(str(e), file=sys.stderr)
                clients.remove(s)
                s.close()

        clients = []
        XLO = 50
        XHI = 2000
        xlimit = XLO
        transactions = 0
        t0 = time.time()

        while True:

            if self.verbose:
                print('Waiting for request...')
            try:
                readable, _, _ = select.select(
                    [ self ] + clients, [], [], 10.0)
            except Exception as e:
                # Usually ValueError on a negative fd from a remote close
                assert self.fileno() != -1, 'Server socket has died'
                dead = [sock for sock in clients if sock.fileno() == -1]
                for d in dead:
                    if d in clients:    # avoid error
                        clients.remove(d)
                continue

            if not readable: # timeout: respond to partials, reset counters
                for s in clients:
                    if s.inbytes:    # No more is coming FIXME: too harsh?
                        print('%s: reset inbuf' % s)
                        s.clear()
                        send_result(s, None)
                transactions = 0
                t0 = time.time()
                xlimit = XLO
                continue

            for s in readable:
                transactions += 1

                if self._perf and transactions > xlimit:
                    deltat = time.time() - t0
                    tps = int(float(transactions) / deltat)
                    print('%d transactions/second' % tps)
                    if xlimit < tps < XHI:
                        xlimit *= 2
                    elif XLO < tps < xlimit:
                        xlimit /= 2
                    transactions = 0
                    t0 = time.time()

                if s is self: # New connection
                    try:
                        (sock, peertuple) = self.accept()
                        newsock = SocketReadWrite(
                            sock=sock,
                            peertuple=peertuple,
                            perf=self._perf,
                            verbose=self.verbose)
                        clients.append(newsock)
                        print('%s: new connection' % newsock)
                    except Exception as e:
                        pass
                    continue

                try:
                    cmdict = s.recv_chunk(chain)
                    if cmdict is None:  # need more, not available now
                        continue

                    if self.verbose:
                        if self.verbose == 1:
                            print('%s: %s' % (s, cmdict['command']))
                        else:
                            print('%s: %s' % (s, str(cmdict)))
                    result = OOBmsg = None
                    result, OOBmsg = handler(cmdict)
                except OSError as e:    # Socket has been closed
                    print(str(e))
                    clients.remove(s)
                    continue
                except Exception as e:  # Internal, lower level stuff
                    if e.__class__ in (AssertionError, RuntimeError):
                        msg = str(e)
                    elif isinstance(e, TypeError):
                        set_trace() # might have gone away w/refactor
                        msg = 'socket death'
                    else:
                        msg = 'UNEXPECTED SOCKET ERROR: %s' % str(e)
                    print('%s: %s' % (s, msg))

                # NO "finally": it circumvents "continue" in error clause(s)
                send_result(s, result)

                if OOBmsg:
                    print('-' * 20, 'OOB:', OOBmsg)
                    for c in clients:
                        if str(c) != str(s):
                            print(str(c))
                            c.send_OOB( OOBmsg)


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
