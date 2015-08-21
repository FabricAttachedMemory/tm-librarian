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

    _OOBprefix = '<@<'
    _OOBsuffix = '>@>'
    _OOBformat = _OOBprefix + '%s' + _OOBsuffix

    def __init__(self, **kwargs):
        self._perf = kwargs.get('perf', 0)
        self.verbose = kwargs.get('verbose', 0)
        peertuple = kwargs.get('peertuple', None)
        selectable = kwargs.get('selectable', True)
        sock = kwargs.get('sock', None)
        self.jsond = JSONDecoder()

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
        else:   # A dead socket can't getpeername so cache it now
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
        self._sock = None       # Force AttributeError on methods
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
        if self.verbose:
            print('%s: sending %s' % (self,
                'NULL' if obj is None
                else '%d bytes' % len(outbytes)))

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

    _bufsz = 4096

    def recv_all(self):
        """ Receive the next part of a message and decode it to a
            python3 string.
        Args:
        Returns:
        """

        appended = 0    # forward references from exceptions
        while True:
            if self.inOOB:  # make caller deal with OOB first
                return None
            last = len(self.instr)
            if last and not self._perf:
                if last > 60:
                    print('INSTR: %d bytes' % last)
                else:
                    print('INSTR: %s' % self.instr)

            # Is this a go around because the eatery below was exhausted,
            # or did it get re-entered after an early leave to consume OOB?
            try:
                retryok = True
                if last:    # maybe trying to finish off a fragment
                    self._sock.settimeout(1.0)  # akin to blocking mode
                self.instr += self._sock.recv(self._bufsz).decode('utf-8')

                appended = len(self.instr) - last
                if not self._perf:
                    print('%s: received %d bytes' % (self._str, appended))

                if not appended:   # Far side is gone
                    msg = '%s: closed by remote' % str(self)
                    self.close()    # May lead to AttributeError below
                    raise OSError(errno.ECONNABORTED, msg)

            except ConnectionAbortedError as e: # see two lines up
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
                retryok = False
                pass
            except Exception as e:
                # AttributeError is ops on closed socket.  Other stuff
                # can just go through.
                if self._sock is None:
                    raise OSError(errno.ECONNABORTED, str(self))
                self.safe_setblocking() # just in case it's live
                raise
            self.safe_setblocking()  # undo timeout

            partialOOB = ''
            while True:
                try:    # If it loads, this recv is complete
                    # result = loads(self.instr)
                    result, nextjson = self.jsond.raw_decode(self.instr)
                    self.instr = partialOOB
                    return result
                except ValueError as e:
                    # Bad JSON conversion.  Extract OOB and try JSON again,
                    # else "break" out of here back to select.  Start with
                    # prefix.  Note: JSON complaint is not always a range,
                    # and "ValueError" can have different complaints:
                    # "Unterminated string" from a looooong argument

                    badrange = str(e).split('(char')[1].strip()
                    badrange = badrange.replace(')', '')
                    badindex = int(badrange.split()[0].strip())
                    incompleteJSON = str(e).startswith('Unterminated string')
                    fullread = appended == self._bufsz

                    if not incompleteJSON:
                        set_trace() # what IS the message with OOB flood?

                    pre = self.instr.find(self._OOBprefix)
                    if pre != -1:
                        suf = self.instr.find(self._OOBsuffix, pre + 3) + 3
                        if suf != -1:
                            self.inOOB.append(self.instr[pre + 3:suf - 3])
                            self.instr = self.instr[0:pre] + self.instr[suf:]
                            if not self.instr:  # Nothing for JSON, so...
                                return None     # ...eat the current OOBs

                        else: # OOB started.  Is there anything else?
                            if pre == 0 and retryok:
                                # that's all there is in instr, need more
                                break
                            # clear it out of the way and retry JSON
                            set_trace()
                            partialOOB = self.instr[pre:]
                            self.instr = self.instr[:pre]
                            continue

                        if retryok:
                            if self.inOOB and len(self.instr) < 10000:
                                return None # eat the OOB
                        continue

                    # instr is a partial something.  Is there any correct
                    # forward progress at the moment?
                    if self.inOOB:
                        return None # finish this off

                    # No sign of full/start OOB, what seems to be partial?
                    # setxattr can easily go past _bufsiz, or perhaps an OOB
                    # OOB flood filled instr.  Is there an OOB end?

                    if not incompleteJSON:
                        suf = self.instr.find(self._OOBsuffix)
                        if suf != -1:   # kill it and try try again
                            set_trace()
                            self.instr = self.instr[suf +3:]
                            if self.instr:
                                continue    # try JSON parse
                            if retryok:
                                break

                    # No full prefix or suffix.  Is there any sign of JSON
                    # start?  Since JSON messages are synchronous, other
                    # stuff is probably partial OOB fragments.
                    leftcurly = self.instr.find('{')
                    if leftcurly != -1:
                        if leftcurly:   # Move it down
                            self.instr = self.instr[leftcurly:]
                            if self.instr:
                                continue    # try again
                        if retryok:
                            break

                    if fullread and retryok:
                    # 1 in _bufz chance there's NOT more
                        set_trace()
                        break

                    # at this stage, it's not recoverable, but I don't
                    # want to crash any exchange going on with client.
                    # What if...
                    set_trace()
                    if not retryok:
                        self.clear()
                    return None

                except Exception as e:
                    set_trace()
                    raise ThisIsReallyBadError

    def clear(self):
        self.instr = ''

    def clearOOB(self):
        self.inOOB = []


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
                    [ self ] + clients, to_write, [], 10.0)
            except Exception as e:
                # Usually ValueError on a negative fd from a remote close
                assert self.fileno() != -1, 'Server socket has died'
                dead = [sock for sock in clients if sock.fileno() == -1]
                for d in dead:
                    if d in clients:    # avoid error
                        clients.remove(d)
                continue

            if not readable and not writeable: # timeout: reset counters
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
                except ConnectionAbortedError as e:
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
                    continue    # no need to check OOB for now

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
