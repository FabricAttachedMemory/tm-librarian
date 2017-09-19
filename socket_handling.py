#!/usr/bin/python3 -tt
""" Module to handle socket communication for Librarian and Clients """

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

import errno
import logging
import logging.handlers
import socket
import select
import sys
import time

from pdb import set_trace
from json import dumps, loads, JSONDecoder

###########################################################################
# Located here because we have no utils module, this is low-level to client
# and server, and tying it directly to sockets might make sense.

# Increasing value of "verbose" should get increasing levels of output.
# EXCEPTION: verbose == 1 -> magic overloaded value for perf stats; it's
# just the way this evolved.   Normal trace logging starts at verbose == 2.
# There weren't really any warnings, so it got co-opted as a secondary INFO.

# Value Logger  Client (lfs_fuse et al)     Server
# 1     PERF    n/a                         transactions/second
# 2     WARNING prentry arguments
# 3     INFO    prentry return values
#               book lists
#               address space values
# 4     DEBUG   Socket byte streams         Socket byte streams
# 5     NOTSET  no threads, no children

_verbose2level = {
    0:  logging.ERROR,          # called as error(), just the icky parts
    1:  logging.CRITICAL,       # called as critical(): performance stats
    2:  logging.WARNING,        # called as warning(), printed as "INFO": basic
    3:  logging.INFO,           # called as info(), printed as "INFO++"
    4:  logging.DEBUG,          # called as debug(), most data of all
}


class perfFilter(logging.Filter):
    '''If verbose == 1 only pass CRITICAL logs.'''

    def __init__(self, verbose):
        # Homework for suppressing CRITICAL
        self.normal = verbose != 1

    def filter(self, record):
        if self.normal:
            return record.levelno != logging.CRITICAL   # suppressed
        return True     # levelno is already CRITICAL


def lfsLogger(loggername, verbose=0, logfilebase=''):
    '''Bigger verbose, more output.  File will go in /var/log else stderr.'''
    format = '%(asctime)s %(levelname)-5s %(name)s: %(message)s'
    if logfilebase:
        h = logging.handlers.RotatingFileHandler(
            '/var/log/' + logfilebase,
            maxBytes=1024*1024,
            backupCount=3)
        datefmt = '%Y-%m-%d %H:%M:%S'
    else:
        h = logging.StreamHandler(stream=sys.stderr)
        datefmt = '%H:%M:%S'
    h.setFormatter(logging.Formatter(format, datefmt=datefmt))

    # socket_handling calls generic logging.xxxx() which, without an
    # explicit root handler, ends up with basicConfig.  This works
    # regardless of whether it's pre- or post- socket_handling.
    logger = logging.root
    logger.name = loggername
    logger.addHandler(h)
    level = _verbose2level.get(verbose, logging.NOTSET)
    logger.setLevel(level)

    # Juggle names.  "Adding" an existing level overwrites its name.
    logging.addLevelName(logging.INFO, 'INFO++')
    logging.addLevelName(logging.WARNING, 'INFO')
    logging.addLevelName(logging.CRITICAL, 'PERF')
    logger.addFilter(perfFilter(verbose))
    # Only output when verbose == 1.   Backwards compatibility sucks.
    logger.critical('PERFORMANCE TRACING ONLY')
    return logger

###########################################################################


class SocketReadWrite(object):
    """ Object that will read and write from a socket.  Used primarily as a
        base class for the Client and Server, or an instance for nodes.
        Logging is done against root logger.
    """
    blocking_retry_max = 5

    def __init__(self, **kwargs):
        self.verbose = kwargs.get('verbose', 0)
        peertuple = kwargs.get('peertuple', None)
        selectable = kwargs.get('selectable', True)
        sock = kwargs.get('sock', None)

        # Allow chars such as CRLF.  A fresh instance seems to be important
        # in a reconnect.
        self.jsond = JSONDecoder(strict=False)

        if sock is None:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self._sock = sock
        self._created_blocking = not selectable
        self.safe_setblocking()

        if peertuple is None:
            self._host = ''
            self._port = 0
            self._str = ''
        else:  # A dead socket can't getpeername so cache it now
            self._host, self._port = peertuple
            self._str = '{0}:{1}'.format(*peertuple)
        self.clear()
        self.inOOB = []
        self.outbytes = bytes()
        self.blocking_retry = 0

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

    def check_blocking_retry_max(self):
        self.blocking_retry += 1
        time.sleep(.2)
        return (self.blocking_retry > self.blocking_retry_max)

    def reset_blocking_retry(self):
        self.blocking_retry = 0

    #----------------------------------------------------------------------
    # Send stuff

    def send_all(self, obj, JSON=True):
        """ Send an object after optional transformation

        Args:
            obj: the object to be sent, None means work off backlog

        Returns:
               True or raised error.
        """

        self.sent = 0
        if JSON:
            # Error possible here: "not JSON serializable", let it raise
            outbytes = dumps(obj).encode()
        else:
            outbytes = obj.encode()
        logging.info('%s: sending %s' % (self,
                                         'NULL' if obj is None else '%d bytes' % len(outbytes)))

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
                self.sent += n
                self.outbytes = self.outbytes[n:]
            self.reset_blocking_retry()
            return True
        except BlockingIOError as e:
            # Far side is full.  FIXME: raising OSError is a weak response
            if self.check_blocking_retry_max():
                raise OSError(errno.EWOULDBLOCK, 'blocking_retry_max')
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
            logging.error('%s: send_all failed: %s' % (self, str(e)))
            set_trace()
            pass
            raise
        return False

    def send_result(self, result, JSON=True):
        try:
            self.last_errmsg = ''
            return self.send_all(result, JSON)  # True or raise
        except Exception as e:  # could be blocking IO
            self.last_errmsg = '%s: %s' % (self, str(e))
            logging.error(self.last_errmsg)
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
            if last:
                if last > 60:
                    logging.debug('INSTR: %d bytes' % last)
                else:
                    logging.debug('INSTR: %s' % self.instr)
            appended = 0

            # First time through OR go-around with partial buffer?
            if not last or needmore:
                try:
                    if last:  # maybe trying to finish off a fragment
                        self._sock.settimeout(0.5)  # akin to blocking mode
                    self.instr += self._sock.recv(self._bufsz).decode('utf-8')

                    appended = len(self.instr) - last

                    logging.info('%s: recvd %d bytes' % (self._str, appended))

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
                    set_trace()     # probably created with selectable=True
                    if not self._created_blocking:
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
            # Json raw_decode() can return something other than a dict.
            # Scalars indicate a partial parsing of the middle of a full
            # JSON response. Let the parser keep at it until it exhausts
            # those symbols.
            while self.instr:
                try:
                    if len(self.inOOB) > self._OOBlimit and \
                       len(self.instr) < self._bufhi:
                        return None  # Deal with this part of the flood
                    result, nextjson = self.jsond.raw_decode(self.instr)
                    self.instr = self.instr[nextjson:]
                    if not isinstance(result, dict):
                        continue
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
                        # A "return None" might be better here when OOB
                        # processing is actually used.
                        break

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
                    raise RuntimeError(
                        'JSON decode results have been misinterpreted')

                raise RuntimeError(
                    'Unexpectedly reached end of JSON parsing loop')

    def clear(self):
        self.instr = ''

    def clearOOB(self):
        self.inOOB = []


class Client(SocketReadWrite):
    """ A simple synchronous client for the Librarian """

    def __init__(self, **kwargs):
        super(self.__class__, self).__init__(**kwargs)

    def connect(self, host='localhost', port=9093, retry=True, reconnect=False):
        """ Connect socket to port on host

        Args:
            host: the host to connect to
            port: the port to connect to
            retry: enter an infinite loop (cuz why not)
            reconnect: assumes an existing connection was broken

        Returns:
            True if it worked, False otherwise
        """

        if reconnect:
            host = self._host
            port = self._port
            assert host and port, 'Insufficient data for reconnect'
            self.__init__(selectable=False, peertuple=(host, port))
            retry = False

        while True:
            try:
                self._sock.connect((host, port))
                break
            except Exception as e:
                if not retry:
                    return False
                logging.info('Retrying connection...')
                time.sleep(2)

        while True:
            try:
                peertuple = self._sock.getpeername()
                break
            except Exception as e:
                if not retry:
                    raise
                logging.info('Retrying getpeername...')
                time.sleep(2)

        self._host, self._port = peertuple
        self._str = '{0}:{1}'.format(*peertuple)
        return True


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

            logging.info('Waiting for request...')
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

            # Something remains in the outbytes buffer, give it another shot
            for w in writeable:
                if w.send_result('', JSON=False):
                    to_write.remove(w)

            for s in readable:
                transactions += 1

                if transactions > xlimit:
                    deltat = time.time() - t0
                    tps = int(float(transactions) / deltat)
                    logging.critical('%d transactions/second' % tps)
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
                            verbose=self.verbose)
                        clients.append(newsock)
                        logging.info('%s: new connection' % newsock)
                    except Exception as e:
                        pass
                    continue

                result = OOBmsg = None
                try:    # get the next command from a client
                    cmdict = s.recv_all()
                    if cmdict is None:  # need more, not available now
                        continue

                    if self.verbose == 1:
                        logging.critical('%s: %s' % (s, cmdict['command']))
                    else:
                        # "True" == print heartbeat; "False" == suppress it
                        if (False or not
                            cmdict['command'].startswith('update_node')
                           ):
                            logging.warning('%s: %s' % (s, str(cmdict)))
                except (ConnectionError, OSError) as e:
                    # These are mostly seen on the server side because it's
                    # tremendously more stable than a pile of nodes.
                    # ConnectionError is a base class in Python3.
                    # OSError 104 (ECONNRESET) happens when lfs_fuse.py gets
                    # kill or kill -9 in the middle of a transaction.
                    # Client sent RST/ACK.
                    # OSError 106 (ECONNABORTED, SW caused conn abort) happens
                    # when lfs_fuse shuts down gracefully.
                    # OSError 113 (EHOSTUNREACH, no route to host) occurs when
                    # a node dies in middle of transaction and can't receive
                    # the # response.  It usually shows up on a (forced)
                    # Jreboot # of the node.
                    logging.error(str(e))
                    if isinstance(e, OSError):
                        if e.errno not in (errno.EHOSTUNREACH,
                                           errno.ECONNABORTED,
                                           errno.ECONNRESET):
                            if sys.stdin.isatty():
                                print('Unhandled OSError during response')
                                set_trace()
                            continue    # Don't yet have a reason to kill it
                    clients.remove(s)
                    if s in to_write:
                        to_write.remove(s)
                    continue
                except Exception as e:  # Shouldn't happen
                    msg = 'UNEXPECTED SOCKET ERROR: %s' % str(e)
                    logging.error('%s: %s' % (s, msg))
                    if sys.stdin.isatty():
                        print(msg)
                        set_trace()
                    raise

                try:    # process the next command
                    result, OOBmsg = handler(cmdict)
                except Exception as e:  # Shouldn't happen
                    set_trace()
                    msg = 'UNEXPECTED HANDLER ERROR: %s' % str(e)
                    logging.error('%s: %s' % (s, msg))
                    raise

                # NO "finally": it circumvents "continue" in error clause(s)
                if not s.send_result(result):   # holdoff or error?
                    if s.outbytes:  # conversion was ok
                        to_write.append(s)
                    else:
                        if not s.sent:  # nothing queued, nothing sent: error
                            s.send_result(
                                { 'errmsg': s.last_errmsg,
                                  'errno': errno.EPROTO }
                            )
                            pass
                    continue  # no need to check OOB for now

                if OOBmsg:
                    logging.debug('-' * 20, 'OOB:', OOBmsg['OOBmsg'])
                    for c in clients:
                        if str(c) != str(s):
                            logging.debug(str(c))
                            c.send_result(OOBmsg)


def main():
    """ Run simple echo server to exercise the module """

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
        return dumps({'status': 'Processed @ %s' % time.ctime()})

    from function_chain import IdentityChain
    chain = IdentityChain()
    server = Server(None)
    print('Waiting for connection...')
    server.serv(echo_handler, chain)


if __name__ == "__main__":
    main()
