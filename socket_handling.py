#!/usr/bin/python3 -tt
""" Module to handle socket communication for Librarian and Clients """

import errno
import socket
import select
import sys
import time

from pdb import set_trace
from json import dumps, loads, JSONDecoder


class SocketReadWrite(object):
    """ Object that will read and write from a socket
    used primarily as a base class for the Client and Server
    objects """

    def __init__(self, **kwargs):
        self._perf = kwargs.get('perf', 0)
        self.verbose = kwargs.get('verbose', 0)
        peertuple = kwargs.get('peertuple', None)
        selectable = kwargs.get('selectable', True)
        sock = kwargs.get('sock', None)
        self.jsond = JSONDecoder(strict=False)  # allow chars such as CRLF

        if sock is None:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self._sock = sock
        self._created_blocking = not selectable
        self.safe_setblocking()

        if peertuple is None:
            self._peer = ''
            self._port = 0
            self._str = ''
        else:  # A dead socket can't getpeername so cache it now
            self._peer, self._port = peertuple
            self._str = '{0}:{1}'.format(*peertuple)
        self.clear()
        self.inOOB = []
        self.outbytes = bytes()
        if self._perf:
            self.verbose = 0

    def __str__(self):
        return self._str

    def safe_setblocking(self, value=None):
        '''If None, restore to created value, else use specified value'''
        if self._sock is not None:
            if value is None:
                value = self._created_blocking
            self._sock.setblocking(bool(value))

    def close(self):
        if self._sock is None:
            return
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except Exception as e:  # already closed, whatever
            pass
        self._sock.close()
        self._sock = None  # Force AttributeError on methods
        self._str += ' (closed)'

    def fileno(self):
        '''Allows this object to be used in select'''
        return -1 if self._sock is None else self._sock.fileno()

    #----------------------------------------------------------------------
    # Send stuff

    def send_all(self, obj, JSON=True):
        """ Send an object after optional transformation

        Args:
            obj: the object to be sent, None means work off backlog

        Returns:
               True or raised error.
        """

        if JSON:
            outbytes = dumps(obj).encode()
        else:
            outbytes = obj.encode()
        if self.verbose > 1:
            print('%s: sending %s' % (self, 'NULL' if obj
                  is None else '%d bytes' % len(outbytes)))

        # socket.sendall will do so and return None on success.  If not,
        # an error is raised with no clue on byte count.  Do it myself.
        self.outbytes += outbytes
        try:
            while len(self.outbytes):
                # This seems to throw errno 11 on its own, but just in case
                # do it myself.  BTW, EAGAIN == EWOULDBLOCK
                n = self._sock.send(self.outbytes)
                if not n:
                    raise OSError(errno.EWOULDBLOCK, 'full')
                self.outbytes = self.outbytes[n:]
            return True
        except BlockingIOError as e:
            # Far side is full
            raise
        except OSError as e:
            if e.errno == errno.EPIPE:
                msg = 'closed by client'
            elif e.errno != errno.EBADF:
                msg = 'closed earlier'
            msg = '%s: %s' % (self, msg)
            raise OSError(errno.ECONNABORTED, msg)
        except AttributeError as e:
            # During retry of a closed socket.  FIXME: delay the close?
            raise OSError(errno.ECONNABORTED, 'Socket closed on prior error')
        except Exception as e:
            print('%s: send_all failed: %s' % (self, str(e)),
                  file=sys.stderr)
            set_trace()
            pass
            raise

    def send_result(self, result):
        try:
            return self.send_all(result)
        except Exception as e:  # could be blocking IO
            print('%s: %s' % (self, str(e)), file=sys.stderr)
            return False

    #----------------------------------------------------------------------
    # Receive stuff

    _bufsz = 8192           # max recv.  FIXME: preallocate this?
    _bufhi = 2 * _bufsz     # Not sure I'll keep this
    _OOBlimit = 20          # when to dump a flood

    def recv_all(self):
        """ Receive the next part of a message and decode it to a
            python3 string.
        Args:
        Returns:
        """

        needmore = False  # Should be entered with an empty buffer
        while True:
            if self.inOOB:  # make caller deal with OOB first
                return None
            last = len(self.instr)
            if last and not self._perf:
                if last > 60:
                    print('INSTR: %d bytes' % last)
                else:
                    print('INSTR: %s' % self.instr)
            appended = 0

            # First time through OR go-around with partial buffer?
            if not last or needmore:
                try:
                    if last:  # maybe trying to finish off a fragment
                        self._sock.settimeout(0.5)  # akin to blocking mode
                    self.instr += self._sock.recv(self._bufsz).decode('utf-8')

                    appended = len(self.instr) - last

                    if self.verbose > 1:
                        print('%s: received %d bytes' % (self._str, appended))

                    if not appended:  # Far side is gone without timeout
                        msg = '%s: closed by remote' % str(self)
                        self.close()  # May lead to AttributeError below
                        raise OSError(errno.ECONNABORTED, msg)

                except ConnectionAbortedError as e:  # see two lines up
                    raise
                except BlockingIOError as e:
                    # Not ready; only happens on a fresh read with non-blocking
                    # mode (ie, can't happen in timeout mode).  Get back to
                    # select, or just re-recv?
                    set_trace()
                    if not self._sock._created_blocking:
                        return None
                    continue
                except socket.timeout as e:
                    pass
                except Exception as e:
                    # AttributeError is ops on closed socket.  Other stuff
                    # can just go through.
                    if self._sock is None:
                        raise OSError(errno.ECONNABORTED, str(self))
                    self.safe_setblocking()  # just in case it's live
                    raise
                self.safe_setblocking()  # undo timeout (idempotent)

            # Only the JSON decode can be allowed to throw an error.
            while self.instr:
                try:
                    if len(self.inOOB) > self._OOBlimit and \
                       len(self.instr) < self._bufhi:
                        return None  # Deal with this part of the flood
                    result, nextjson = self.jsond.raw_decode(self.instr)
                    self.instr = self.instr[nextjson:]
                    OOBmsg = result.get('OOBmsg', False)
                    if OOBmsg:  # and that's the whole result
                        self.inOOB.append(OOBmsg)
                        continue
                    return result
                except ValueError as e:
                    # Bad JSON conversion.  Since I'm using raw_decode
                    # multiple messages are no longer a problem; just walk
                    # through them with nextjson above.  xattrs can go past
                    # _bufsiz, or perhaps an OOB flood filled instr.  Either
                    # way I need more bytes by breaking back into recv loop.

                    # Is this a failure after re-read and if so did it help?
                    if needmore and needmore == self.instr:
                        self.clear()
                        return None  # might be some dangling OOBs

                    # There's only a 1 in bufsz chance that a read really
                    # ended exactly on the boundary.  In other words, a
                    # full read probably means there's more.
                    if appended >= self._bufsz:  # decode() might make more
                        needmore = self.instr  # the '+='rebinds instr
                        break

                    # Does it at least smell like JSON?  Maybe it's the middle
                    # of one message and the beginning of another.  That's
                    # authoritative, so remove pre-cruft and try again.
                    leftright = self.instr.find('}{')
                    if leftright != -1:
                        self.instr = self.instr[leftright + 1:]
                        continue

                    # What about a good start, but unfinished end?  Might
                    # not be able to tell because of sub-objects.
                    leftcurly = self.instr.find('{')
                    if leftcurly == 0:
                        needmore = self.instr
                        break

                    # at this stage, it's not recoverable
                    self.clear()
                    return None

                except Exception as e:
                    set_trace()
                    raise ThisIsReallyBadError

                raise ButThisIsWorseError

    def clear(self):
        self.instr = ''

    def clearOOB(self):
        self.inOOB = []


class Client(SocketReadWrite):
    """ A simple synchronous client for the Librarian """

    def __init__(self, **kwargs):
        super(self.__class__, self).__init__(**kwargs)

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

    def __init__(self, parseargs):
        super(self.__class__, self).__init__(**(dict(parseargs._get_kwargs())))
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

    def serv(self, handler):
        """ "event-loop" for the server this is where the server starts
        listening and serving requests.

        Args:
            handler: commands received by the server are sent here
        Returns:
            Nothing.
        """

        clients = []
        XLO = 50
        XHI = 2000
        xlimit = XLO
        transactions = 0
        t0 = time.time()
        to_write = []

        while True:

            if self.verbose:
                print('Waiting for request...')
            try:
                readable, writeable, _ = select.select(
                    [ self ] + clients, to_write, [], 5.0)
            except Exception as e:
                # Usually ValueError on a negative fd from a remote close
                assert self.fileno() != -1, 'Server socket has died'
                dead = [sock for sock in clients if sock.fileno() == -1]
                for d in dead:
                    if d in clients:  # avoid error
                        clients.remove(d)
                continue

            if not readable and not writeable:  # timeout: reset counters
                transactions = 0
                t0 = time.time()
                xlimit = XLO
                continue

            for w in writeable:
                if w.send_all('', JSON=False):
                    to_write.remove(w)

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

                if s is self:  # New connection
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
                    cmdict = s.recv_all()
                    if cmdict is None:  # need more, not available now
                        continue

                    if self.verbose:
                        if self.verbose == 1:
                            print('%s: %s' % (s, cmdict['command']))
                        else:
                            print('%s: %s' % (s, str(cmdict)))
                    result = OOBmsg = None
                    result, OOBmsg = handler(cmdict)
                except ConnectionError as e:  # Base class in Python3
                    print(str(e))
                    clients.remove(s)
                    if s in to_write:
                        to_write.remove(s)
                    continue
                except Exception as e:  # Internal, lower level stuff
                    if e.__class__ in (AssertionError, RuntimeError):
                        msg = str(e)
                    else:
                        msg = 'UNEXPECTED SOCKET ERROR: %s' % str(e)
                    print('%s: %s' % (s, msg))
                    set_trace()
                    raise

                # NO "finally": it circumvents "continue" in error clause(s)
                if not s.send_result(result):
                    to_write.append(s)
                    continue  # no need to check OOB for now

                if OOBmsg:
                    print('-' * 20, 'OOB:', OOBmsg['OOBmsg'])
                    for c in clients:
                        if str(c) != str(s):
                            if not self._perf:
                                print(str(c))
                            c.send_result(OOBmsg)


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
