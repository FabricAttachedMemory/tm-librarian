#!/usr/bin/python3 -tt


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

# https://www.kernel.org/doc/Documentation/filesystems/vfs.txt

import argparse
import errno
import glob
import os
import psutil
import shlex
import socket
import subprocess
import stat
import sys
import threading
import time
import logging

from pdb import set_trace

from tm_fuse import TMFS, TmfsOSError, Operations, LoggingMixIn, tmfs_get_context

from book_shelf_bos import TMShelf
from cmdproto import LibrarianCommandProtocol
from frdnode import FRDnode, FRDFAModule
from socket_handling import Client, lfsLogger

from lfs_shadow import the_shadow_knows

ACPI_NODE_UID = '/sys/devices/LNXSYSTM:00/LNXSYBUS:00/ACPI0004:00/uid'


class Heartbeat:
    def __init__(self, timeout_seconds, callback):
        self._timeout_seconds = timeout_seconds
        self._callback = callback
        self._heartbeat_timer = None            # assist (re)scheduling

    def schedule(self, seconds_this_time=None):
        if self._heartbeat_timer is not None:   # don't just unbind it
            self._heartbeat_timer.cancel()
        if seconds_this_time is None:
            seconds_this_time = self._timeout_seconds
        self._heartbeat_timer = threading.Timer(
            float(seconds_this_time), self._callback)
        self._heartbeat_timer.daemon = True
        self._heartbeat_timer.start()

    def unschedule(self):
        if self._heartbeat_timer is not None:
            self._heartbeat_timer.cancel()
        self._heartbeat_timer = None

###########################################################################
# Decorator only for instance methods as it assumes args[0] == "self".


def prentry(func):
    def new_func(*args, **kwargs):
        self = args[0]
        # remove unschedule to match removal of schedule below
        # self.heartbeat.unschedule()
        self.logger.info('----------------------------------')
        if self.verbose:
            tmp = ', '.join([str(a) for a in args[1:]])
            p_data = str(self.lcp._context['pid'])
            if self.verbose > 1:
                # Could use psutil if we need more functionality
                comm = '/proc/' + str(self.lcp._context['pid']) + '/comm'
                try:
                    with open(comm, 'r') as f:
                        p_data += '/' + f.read().replace('\n', '')
                except IOError:
                    pass
            self.logger.warning(
                '%s(%s) [pid=%s]' % (func.__name__, tmp[:60], p_data))
        self._ret_is_string = True  # ie, has a length
        ret = func(*args, **kwargs)
        if self.verbose > 1:
            if self._ret_is_string:
                tmp = str(ret)
                if len(tmp) > 128:
                    tmp = tmp[:128] + '...'
                self.logger.info(tmp)

        # Now that the heartbeat has more data, don't put it off
        # just because this command is proof-of-life.
        # if self.lfs_status != FRDnode.SOC_STATUS_OFFLINE:
            # self.heartbeat.schedule()

        return ret

    # Be a well-behaved decorator
    new_func.__name__ = func.__name__
    new_func.__doc__ = func.__doc__
    new_func.__dict__.update(func.__dict__)
    return new_func

###########################################################################
# __init__ is called before doing the "mount" (ie, libfuse.so is not
# invoked until after this returns).  Errors that that get raised from
# __init__ will terminate the process which is probably a good thing.

# Errors raised anywhere else (including "init") get swallowed by fuse.py
# so that lfs_fuse.py can struggle on.   That's another good thing overall
# but dictates where certain operations should be placed.


class LibrarianFS(Operations):  # Name shows up in mount point

    _MODE_DEFAULT_BLK = stat.S_IFBLK + 0o666
    _MODE_DEFAULT_DIR = stat.S_IFDIR + 0o777

    _ZERO_PREFIX = '.lfs_pending_zero_'

    _LOST_FOUND_PATH = '/lost+found'

    def __init__(self, args):
        '''Validate command-line parameters'''
        basename = ''
        if args.daemon:
            basename = 'lfs.%s.%s:%d.log' % (
                os.uname().nodename, args.hostname, args.port)
        self.logger = lfsLogger(args.mountpoint, args.verbose, basename)
        self.verbose = args.verbose
        self.host = args.hostname
        self.port = args.port
        self.mountpoint = args.mountpoint
        self.fakezero = args.fakezero

        # Fake it to start.  The umask dance is Pythonic, unfortunately.
        umask = os.umask(0)
        os.umask(umask)
        physloc = '_'.join((
            str(args.physloc.rack),
            str(args.physloc.enc),
            str(args.physloc.node)))
        context = {
            'umask': umask,
            'node_id': args.physloc.node_id,
            'physloc': physloc,
        }
        self.lcp = LibrarianCommandProtocol(context)
        self.inflight = threading.Lock()

        # Command-line miscues like a bad shadow path.  However that needs a
        # socket, so do it now.

        # connect() has an infinite retry
        self.torms = Client(selectable=False, verbose=self.verbose)
        self.torms.connect(host=self.host, port=self.port)
        self.logger.info('%s: connected' % self.torms)

        lfs_globals = self.librarian(self.lcp('get_fs_stats'))
        self.bsize = lfs_globals['book_size_bytes']

        args.logger = self.logger
        self.shadow = the_shadow_knows(args, lfs_globals)
        self.zerosema = threading.Semaphore(value=8)

        self.heartbeat = Heartbeat(
            FRDnode.SOC_HEARTBEAT_SECS, self.send_heartbeat)
        self.heartbeat_interval = FRDnode.SOC_HEARTBEAT_SECS
        self.lfs_status = FRDnode.SOC_STATUS_ACTIVE
        psutil.cpu_percent(interval=None) # dummy call to set interval baseline
        net_io = psutil.net_io_counters(pernic=False)
        self.prev_net_io_in = net_io.bytes_recv
        self.prev_net_io_out = net_io.bytes_sent
        self.librarian(self.lcp('update_node_soc_status',
                                status=FRDnode.SOC_STATUS_ACTIVE,
                                cpu_percent=int(psutil.cpu_percent(interval=None)),
                                rootfs_percent=int(psutil.disk_usage('/')[-1]),
                                network_in=0,
                                network_out=0,
                                mem_percent=int(psutil.virtual_memory().percent)))
        self.librarian(self.lcp('update_node_mc_status',
                                status=FRDFAModule.MC_STATUS_ACTIVE))
        self.heartbeat.schedule()

    # started with "mount" operation.  root is usually ('/', ) probably
    # influenced by FuSE builtin option.  All errors here will essentially
    # be ignored, so if there's potentially fatal stuff, do it in __init__()

    @prentry
    def init(self, root, *args, **kwargs):
        assert not args and not kwargs, 'Unexpected parameters to init()'

        # FIXME: in C FUSE, data returned from here goes into 'getcontext'.
        # Note: I no longer remember my concern on this, look into it again.

    @prentry
    def destroy(self, path):    # fusermount -u or SIGINT aka control-C
        self.lfs_status = FRDnode.SOC_STATUS_OFFLINE
        self.librarian(self.lcp('update_node_soc_status',
                                status=FRDnode.SOC_STATUS_OFFLINE,
                                cpu_percent=0,
                                rootfs_percent=0,
                                network_in=0,
                                network_out=0,
                                mem_percent=0))
        self.librarian(self.lcp('update_node_mc_status',
                                status=FRDFAModule.MC_STATUS_OFFLINE))
        assert threading.current_thread() is threading.main_thread()
        self.torms.close()
        del self.torms

    # helpers

    def get_bos(self, shelf):
        path = self.get_shelf_path(shelf)
        shelf.bos = self.librarian(self.lcp('list_shelf_books', path=path))
        # Replaced a per-book loop of lcp('get_book') which was done
        # in anticipation of drilling down on more info.  Turns out
        # we have everything we need.   This could easily be in the
        # Librarian but then that increases the data size of the
        # list_shelf_books call.  Remember an MFT LZA is 20 bits of
        # info shifted 33 bits (an 8G book offset).
        self.logger.info('%s BOS: %s' % (shelf.name, shelf.bos))

    def get_shelf_path(self, shelf):
        tmp = self.lcp('get_shelf_path', shelf)
        path = self.librarian(tmp)
        return path

    # Round 1: flat namespace at / requires a leading / and no others.
    @staticmethod
    def _legacy_path2name(path):
        elems = path.split('/')
        # check for len(elems) > 2 used to exist, but is was called to ignore
        # at every spot it was called so now is removed, as well as default
        # param to ignore the check
        shelf_name = elems[-1]        # if empty, original path was '/'
        return shelf_name

    # First level:  tenants
    # Second level: tenant group
    # Third level:  shelves

    def handleOOB(self):
        # ALTERNATIVE: Put the message on a Queue for the main thread.
        assert threading.current_thread() is threading.main_thread()
        for oob in self.torms.inOOB:
            self.logger.warning('\t\t!!!!!!!!!!!!!!!!!!!!!!!! %s' % oob)
        self.torms.clearOOB()

    def librarian(self, cmdict, errorOK=False):
        '''Dictionary in, dictionary out'''
        # There are times when the process that invoked an action,
        # notably release(), has died by the time this point is reached.
        # In that case uid/gid/pid will all be zero.
        # BUG: PID is the original process, not a twice-forked daemon.
        context = cmdict['context']
        (context['uid'],
         context['gid'],
         context['pid']) = tmfs_get_context()
        context['tid'] = threading.get_ident()
        try:
            # validate primary keys
            command = cmdict['command']
            seq = cmdict['context']['seq']
        except KeyError as e:
            self.logger.error(str(e))
            raise TmfsOSError(errno.ENOKEY)

        # Connection failures: simple testing (killing librarian at quiescent
        # point) usually lets the send_all() succeed, then dies on recv_all().
        # socket_handling.py is the actor that raises ECONNABORTED.  As a
        # first approximation, just do a (re) connect on detection.   Better
        # attempts need to add a reconnect timeout, split send_all from
        # recv_all error processing, put a "while" around certain things...
        errmsg = { }
        rspdict = None
        with self.inflight:
            try:
                self.torms.send_all(cmdict)
                while rspdict is None:
                    rspdict = self.torms.recv_all()
                    if self.torms.inOOB:
                        self.handleOOB()
                if 'errmsg' in rspdict:  # higher-order librarian internal error
                    errmsg['errmsg'] = rspdict['errmsg']
                    errmsg['errno'] = rspdict['errno']
            except OSError as e:
                errmsg['errmsg'] = 'Communications error with librarian'
                errmsg['errno'] = e.errno   # was always HOSTDOWN
                if e.errno in (errno.ECONNABORTED, ):
                    tmp = self.torms.connect(reconnect=True)
                    if tmp:
                        # If request is idempotent, re-issue (need a while loop)
                        pass  # for now
            except MemoryError as e:  # OOB storm and internal error not pull instr
                errmsg['errmsg'] = 'OOM BOOM'
                errmsg['errno'] = errno.ENOMEM
            except Exception as e:
                errmsg['errmsg'] = str(e)
                errmsg['errno'] = errno.EREMOTEIO

            # if rspdict is None a comms error occurred and it's game over.
            # Otherwise an error occurred in evaluating the request (ie, shelf
            # not found).  In general quitting here is sufficent.
            if errmsg:
                self.logger.error('%s failed: %s' % (command, errmsg['errmsg']))
                if rspdict is not None and errorOK:
                    return rspdict
                raise TmfsOSError(errmsg['errno'])

            try:
                value = rspdict['value']
                rspseq = rspdict['context']['seq']
                if seq != rspseq:
                    msg = 'Response not for me %s != %s' % (seq, rspseq)
                    self.logger.error(msg)
                    # raise OSError(errno.EILSEQ, msg)
                    raise TmfsOSError(errno.EILSEQ)
            except KeyError as e:
                raise OSError(errno.ERANGE, 'Bad response format')

            return value  # None is legal, let the caller deal with it.

    def send_heartbeat(self):
        try:
            net_io = psutil.net_io_counters(pernic=False)
            self.librarian(self.lcp('update_node_soc_status',
                            status=self.lfs_status,
                            cpu_percent=int(psutil.cpu_percent(interval=None)),
                            rootfs_percent=int(psutil.disk_usage('/')[-1]),
                            network_in=int((net_io.bytes_recv -
                                self.prev_net_io_in)/self.heartbeat_interval),
                            network_out=int((net_io.bytes_sent -
                                self.prev_net_io_out)/self.heartbeat_interval),
                            mem_percent=int(psutil.virtual_memory().percent)))
            self.prev_net_io_in = net_io.bytes_recv
            self.prev_net_io_out = net_io.bytes_sent
        except Exception as e:
            # Connection failure with Librarian ends up here.
            # FIXME shorten the heartbeat interval to speed up reconnect?
            pass
        self.heartbeat.schedule()

    # Higher-level FS operations

    # Called early on nearly all accesses.  Returns os.lstat() equivalent
    # or OSError(errno.ENOENT).  When tenants go live, EPERM can occur.
    # FIXME: move this into Librarian proper.  It should be doing
    # all the calculations, doubly so when we implement tenancy.
    # fh is set if original call was fstat (vs stat or lstat), not sure
    # if it matters or not.  Maybe validate it against cache in shadow files?

    @prentry
    def getattr(self, path, fh=None):
        # ROSS remove some root hardcoding now that a real root exists
        if fh is not None:
            raise TmfsOSError(errno.ENOENT)  # never saw this in 8 months
        rsp = self.librarian(self.lcp('get_shelf', path=path))
        shelf = TMShelf(rsp)
        tmp = {
            'st_ctime':     shelf.ctime,
            'st_mtime':     shelf.mtime,
            'st_uid':       42,
            'st_gid':       42,
            'st_mode':      shelf.mode,
            'st_nlink':     shelf.link_count,
            'st_size':      shelf.size_bytes
        }
        if shelf.name.startswith('block'):
            tmp['st_mode'] = self._MODE_DEFAULT_BLK
        return tmp

    @prentry
    def readdir(self, path, index):
        '''Either be a real generator, or get called like one.'''
        rsp = self.librarian(self.lcp('list_shelves', path=path))
        for shelf in rsp:
            yield shelf['name']

    # os.getaccess(path, mode): returns nothing (None), or raise EACCESS.
    # FIXME: will have to be tenant-aware
    @prentry
    def access(self, path, mode):   # returned nothing, but maybe 0?
        try:
            attrs = self.getattr(path)
        except Exception as e:
            raise TmfsOSError(errno.EACCES)

    #----------------------------------------------------------------------
    # Extended attributes: apt-get install attr, then man 5 attr.  Legal
    # namespaces are (user, system, trusted, security).  Anyone can
    # see "user", but the others take CAP_SYS_ADMIN.  Currently only
    # "user" works, even with sudo, not sure why, or if it really matters.
    # Something (fusepy, I imagine) always calls getattr() first, so it's
    # a reasonable assumption the shelf exists.

    @prentry
    def getxattr(self, path, xattr, position=0):
        """Called with a specific namespace.name xattr.  Can return either
           a bytes array OR an int."""
        if position:
            raise TmfsOSError(errno.ENOSYS)    # never saw this in 8 months

        rsp = self.librarian(self.lcp('get_shelf', path=path))
        shelf = TMShelf(rsp)

        # Does this also need changed to support path instead of name?
        # Piggy back for queries by kernel (globals & fault handling).
        if xattr.startswith('_obtain_'):
            # this will need some work
            data = self.shadow.getxattr(shelf, xattr)
            try:
                return bytes(data.encode())
            except AttributeError as e:     # probably the "encode()"
                self._ret_is_string = False
                return bytes(data)

        # "ls" starts with simple getattr but then comes here for
        # security.selinux, system.posix_acl_access, and posix_acl_default.
        # ls -l can also do the same thing on '/'.  Save the round trips.

        # if xattr.startswith('security.') or not shelf_name:  # path == '/'
        if xattr.startswith('security.'):  # path == '/' is legal now
            return bytes(0)

        try:
            rsp = self.librarian(
                self.lcp('get_xattr', path=path, xattr=xattr))
            value = rsp['value']
            assert value is not None    # 'No such attribute'
            if isinstance(value, int):
                return value
            elif isinstance(value, str):
                # http://stackoverflow.com/questions/606191/convert-bytes-to-a-python-string
                return bytes(value.encode('cp437'))
            else:
                bytes(value.encode())
        except Exception as e:
            raise TmfsOSError(errno.ENODATA)    # syn for ENOATTR

    @prentry
    def listxattr(self, path):
        """getfattr(1) -d calls listxattr(2).  Return a list of names."""
        rsp = self.librarian(
                self.lcp('list_xattrs', path=path))
        value = rsp['value']
        return value

    _badjson = tuple(map(str.encode, ('"', "'", '{', '}')))

    @prentry
    def setxattr(self, path, xattr, valbytes, flags, position=0):
        # flags from linux/xattr.h: XATTR_CREATE = 1, XATTR_REPLACE = 2
        if flags or position:
            raise TmfsOSError(errno.ENOSYS)     # haven't actually seen it yet

        # 'Extend' user.xxxx syntax and screen for it here
        elems = xattr.split('.')
        if elems[0] != 'user' or len(elems) < 2:
            raise TmfsOSError(errno.EINVAL)

        # Don't forget the setfattr command, and the shell it runs in, does
        # things to a "numeric" argument.  setfattr processes a leading
        # 0x and does a byte-by-byte conversion, yielding a byte array.
        # It needs pairs of digits and can be of arbitrary length.  Any
        # other argument ends up here as a pure string (well, byte array).
        try:
            value = valbytes.decode()
        except ValueError as e:
            # http://stackoverflow.com/questions/606191/convert-bytes-to-a-python-string
            value = valbytes.decode('cp437')

        rsp = self.librarian(
                self.lcp('set_xattr', path=path,
                         xattr=xattr, value=value))
        if rsp is not None:  # unexpected
            raise TmfsOSError(errno.ENOTTY)

    @prentry
    def removexattr(self, path, xattr):
        rsp = self.librarian(
            self.lcp('remove_xattr', path=path, xattr=xattr))
        if rsp is not None:  # unexpected
            raise TmfsOSError(errno.ENOTTY)

    @prentry
    def statfs(self, path):  # "df" command; example used statVfs.
        lfs_globals = self.librarian(self.lcp('get_fs_stats'))
        blocks = lfs_globals['books_total']
        bfree = bavail = blocks - lfs_globals['books_used']
        # 2015-12-06: df works with 8M books but breaks with 8G.  My guess:
        # a 32-bit int somewhere.  Not sure if this matters anyhow.
        # Use a 4K block size, modify other attributes accordingly.
        bsize = 4096
        # This assumes book_size_bytes is evenly divisible by
        # bsize, if not the stats will be wrong.
        bm = int(lfs_globals['book_size_bytes'] / bsize)
        blocks *= bm
        bfree *= bm
        bavail *= bm

        return {
            'f_bavail':     bavail,  # free blocks for unpriv users
            'f_bfree':      bfree,   # total free DATA blocks
            'f_blocks':     blocks,  # total DATA blocks
            'f_bsize':      bsize,   # optimal transfer block size???
            'f_favail':     bavail,  # free inodes for unpriv users
            'f_ffree':      bfree,   # total free file inodes
            'f_files':      blocks,  # total number of inodes
            'f_flag':       63,      # mount flags
            'f_frsize':     0,       # fragment size
            'f_namemax':    255,     # maximum filename length
        }

    @staticmethod
    def _cmd2sub(cmd):
        args = shlex.split(cmd)
        p = subprocess.Popen(args)
        time.sleep(2)   # Because poll() seems to have some lag time
        return p

    # Just say no to the socket.
    @prentry
    def _zero(self, shelf):
        assert shelf.name.startswith(self._ZERO_PREFIX)
        fullpath = '%s/%s' % (self.mountpoint, shelf.name)
        if self.fakezero:
            cmd = '/bin/sleep 5'
        else:
            cmd = '/bin/dd if=/dev/zero of=%s bs=64k conv=notrunc,fsync iflag=count_bytes count=%d' % (
                fullpath, shelf.size_bytes)

        dd = self._cmd2sub(cmd)
        self.logger.info('%s: PID %d' % ('dd', dd.pid))
        with self.zerosema:
            try:
                polled = dd.poll()   # None == not yet terminated, else retval
                while polled is None:
                    try:
                        dd.send_signal(os.SIGUSR1)  # gets status readout
                        stdout, stderr = dd.communicate(timeout=5)
                    except TimeoutExpired as e:
                        self.logger.error(str(stderr))
                    time.sleep(3)
                    polled = dd.poll()
            except Exception as e:
                pass
        if dd is not None:
            dd.wait()

        truncate = self._cmd2sub('/usr/bin/truncate -s0 %s' % fullpath)
        if truncate is not None:
            truncate.wait()

        unlink = self._cmd2sub('/usr/bin/unlink %s' % fullpath)
        if unlink is not None:
            unlink.wait()

    # "man fuse" regarding "hard_remove": an "rm" of a file with active
    # opens tries to rename it, only issuing a real unlink when all opens
    # have released.  If this is entered with such a renamed file, VFS
    # thinks there are no open handles and so should LFS.  Similar logic
    # should be followed on unlink of a zeroed file.

    @prentry
    def unlink(self, path, *args, **kwargs):
        assert not args and not kwargs, 'unlink: unexpected args'
        shelf = TMShelf(self.librarian(self.lcp('get_shelf', path=path)))

        # Paranoia check for (dangling) opens.  Does VFS catch these first?
        cached = self.shadow[(shelf.id, None)]
        if cached is not None:                  # Not a good sign
            open_handles = cached.open_handle
            # This was if not none, but cached.open_handle was returning empty dict,
            # not a NoneObject
            if open_handles:        # Definitely a bad sign
                raise TmfsOSError(errno.EBUSY)

                # Once upon a time I forced it...
                for fh in open_handles.values():
                    try:
                        self.librarian(self.lcp('close_shelf',
                                                id=shelf.id,
                                                fh=fh))
                    except Exception as e:
                        pass

        self.shadow.unlink(shelf)  # empty cache INCLUDING dangling opens

        # Early exit: no explicit zeroing required if:
        # 1. it's zero bytes long
        # 2. it's passed through zeroing, meaning this is the second trip to
        #    unlink()..  Even if length is non-zero, remove it, assuming the
        #   _zero subprocess failed.
        # 3. The shadow subclass doesn't need it
        if ((not shelf.size_bytes) or
            shelf.name.startswith(self._ZERO_PREFIX) or
                (not self.shadow.zero_on_unlink)):
            self.librarian(self.lcp('destroy_shelf', path=path))
            return 0

        # Schedule for zeroing; a second entry to unlink() will occur on
        # this shelf with the new name.
        zeroname = '%s%d' % (self._ZERO_PREFIX, shelf.id)
        if zeroname != shelf.name:

            self.rename(self.get_shelf_path(shelf), zeroname)
            shelf.name = zeroname
            shelf.mode = stat.S_IFREG   # un-block it as was done on server
        if self.verbose <= 3:
            threading.Thread(target=self._zero, args=(shelf, )).start()
        else:
            set_trace()
            self._zero(shelf)   # this will block: lfs_fuse is single-threaded
        return 0

    @prentry
    def utimens(self, path, times=None):
        if times is not None:
            times = tuple(map(int, times))
            if abs(int(time.time() - times[1])) < 3:  # "now" on this system
                times = None
        if times is None:
            times = (0, 0)  # let librarian pick it
        self.librarian(
            self.lcp('set_am_time', path=path,
                     atime=times[0],
                     mtime=times[1]))
        return 0  # os.utime

    @prentry
    def open(self, path, flags, mode=None):
        # looking for filehandles?  See FUSE docs.  Librarian will do
        # all access calculations so call it first.
        rsp = self.librarian(self.lcp('open_shelf', path=path))
        shelf = TMShelf(rsp)
        self.get_bos(shelf)
        # File handle is a proxy for FuSE to refer to "real" file descriptor.
        # Different shadow types may return different things for kernel.
        fx = self.shadow.open(shelf, flags, mode)
        return fx

    # POSIX: Librarian returns an open shelf, either extant or newly created.
    @prentry
    def create(self, path, mode, fh=None, supermode=None):
        if fh is not None:
            # createat(2), methinks, but I never saw this in 8 months
            raise TmfsOSError(errno.ENOSYS)
        if supermode is None:
            mode &= 0o777
            tmpmode = stat.S_IFREG + mode
        else:
            mode = supermode & 0o777
            tmpmode = supermode
        tmp = self.lcp('create_shelf', path=path, mode=tmpmode)
        rsp = self.librarian(tmp)
        shelf = TMShelf(rsp)                # This is an open shelf...
        fx = self.shadow.create(shelf, mode)     # ...added to the cache...
        return fx            # ...with this value.

    @prentry
    def read(self, path, length, offset, fh):
        # FIXME: this might break shadow directories and shadow files
        # but those have not been made to work with subs anyway
        rsp = self.librarian(self.lcp('get_shelf', path=path))
        shelf = TMShelf(rsp)
        return self.shadow.read(shelf, length, offset, fh)

    @prentry
    def write(self, path, buf, offset, fh):
        # FIXME: see read comment
        rsp = self.librarian(self.lcp('get_shelf', path=path))
        shelf = TMShelf(rsp)

        # Resize shelf "on the fly" for writes past EOF
        # BUG: what if shelf was resized elsewhere?  And what about read?
        req_size = offset + len(buf)
        if self.shadow[(shelf.id, None)].size_bytes < req_size:
            self.truncate(path, req_size, fh)  # updates the cache

        return self.shadow.write(shelf, buf, offset, fh)

    @prentry
    def truncate(self, path, length, fh=None):
        '''truncate(2) calls with fh == None; based on path but access
           must be checked.  ftruncate passes in open handle'''
        zero_enabled = self.shadow.zero_on_unlink

        # ALWAYS get the shelf by name, even if fh is valid.
        # FIXME: Compare self.shadow[fh] to returned shelf.
        # IMPLICIT ASSUMPTION: without tenants this will never EPERM
        rsp = self.librarian(self.lcp('get_shelf', path=path))
        req = self.lcp('resize_shelf',
                       path=path,
                       size_bytes=length,
                       id=rsp['id'],
                       zero_enabled=zero_enabled)
        rsp = self.librarian(req)

        # If books were removed from the shelf and added to a zeroing
        # shelf, start a process to zero the books on that shelf.
        if rsp['z_shelf_path'] is not None:
            z_rsp = self.librarian(self.lcp(
                'get_shelf', path=rsp['z_shelf_path']))
            z_shelf = TMShelf(z_rsp)
            threading.Thread(target=self._zero, args=(z_shelf,)).start()

        # Refresh shelf info
        rsp = self.librarian(self.lcp('get_shelf', path=path))
        shelf = TMShelf(rsp)
        if shelf.size_bytes < length:
            raise TmfsOSError(errno.EINVAL)
        self.get_bos(shelf)
        return self.shadow.truncate(shelf, length, fh)

    @prentry
    def ioctl(self, path, cmd, arg, fh, flags, data):
        rsp = self.librarian(self.lcp('get_shelf', path=path))
        shelf = TMShelf(rsp)
        return self.shadow.ioctl(shelf, cmd, arg, fh, flags, data)

    @prentry
    def fallocate(self, path, mode, offset, length, fh=None):
        # LFS doesn't support sparse files or hole punching (lazy allocation).
        # mode == 0 is essentially posix_fallocate and that's all there is.
        # Case in point: mkfs on a shelf (or a loopback device to a shelf)
        # wants mode == 3 == FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE
        # for quick-and-dirty zeroed blocks.  See it in the e2fsprogs source
        # file lib/ext2fs/unix_io.c::unix_discard()
        if mode:
            raise TmfsOSError(errno.EOPNOTSUPP)
        if fh is None:
            rsp = self.librarian(self.lcp('get_shelf', path=path))
            shelf = TMShelf(rsp)
        else:
            shelf = self.shadow[(None, fh)]
            if not shelf:
                raise TmfsOSError(errno.ESTALE)
        if shelf.size_bytes >= offset + length:
            return 0
        return self.truncate(path, offset+length, None)

    # Called when last reference to an open file (in one PID) is closed.
    @prentry
    def release(self, path, fh):
        try:
            shelf = self.shadow.release(fh)
            req = self.lcp(
                'close_shelf', id=shelf.id, open_handle=shelf.open_handle)
            self.librarian(req)  # None or raise
        except Exception as e:
            raise TmfsOSError(errno.ESTALE)

    @prentry
    def flush(self, path, fh):
        '''May be called zero, one, or more times per shelf open.  It's a
           chance to report delayed errors, not a syscall passthru.'''
        return 0

    # @prentry
    # def opendir(self, path, *args, **kwargs):
        # raise TmfsOSError(errno.ENOSYS)

    @prentry
    def fsync(self, path, datasync, fh):
        raise TmfsOSError(errno.ENOSYS)

    @prentry
    def fsyncdir(self, path, datasync, fh):
        raise TmfsOSError(errno.ENOSYS)

    @prentry
    def bmap(self, path, blocksize, blockno):
        '''Only if "target" is a filesystem on a block device.  Convert
           file-relative blockno to device-relative block.'''
        raise TmfsOSError(errno.ENOSYS)

    @prentry
    def rename(self, old, new):
        # 0 or raise
        rsp = self.librarian(self.lcp('get_shelf', path=new), errorOK=True)
        if 'errmsg' not in rsp:
            self.unlink(new)
        rsp = self.librarian(self.lcp('get_shelf', path=old))
        shelf = TMShelf(rsp)
        # one of the only places path2name still exists
        # renamed from old path2shelf
        new_name = self._legacy_path2name(new)
        old_name = self._legacy_path2name(old)
        self.shadow.rename(shelf, old_name, new_name)
        req = self.lcp('rename_shelf', path=old, id=shelf.id, newpath=new)
        self.librarian(req)  # None or raise
        return 0

    #######################################################################
    # Not gonna happen...ever?

    @prentry
    def chmod(self, path, mode, **kwargs):
        raise TmfsOSError(errno.ENOSYS)

    @prentry
    def chown(self, path, uid, gid):
        raise TmfsOSError(errno.ENOSYS)

    @prentry
    def readlink(self, path):
        rsp = self.librarian(self.lcp('readlink', path=path))
        return rsp

    @prentry
    def mknod(self, path, mode, dev):
        # File can't exist already or upper levels of VFS reject the call.
        if mode & stat.S_IFBLK != stat.S_IFBLK:
            raise TmfsOSError(errno.ENOTBLK)
        rsp = self.librarian(self.lcp('get_shelf', path=path),
                             errorOK=True)
        if 'errmsg' not in rsp:
            raise TmfsOSError(errno.EEXIST)
        nbooks = dev & 0xFF     # minor
        if not nbooks:
            raise TmfsOSError(errno.EINVAL)
        mode &= 0o777
        fh = self.create(path, 0, supermode=stat.S_IFBLK + mode)  # w/shadow
        self.setxattr(path, 'user.LFS.AllocationPolicy',
                      'LocalNode'.encode(), 0)
        self.truncate(path, nbooks * self.bsize, fh)
        self.release(path, fh)
        # mknod(1m) immediately does a stat looking for S_IFBLK.
        # Not sure what else to do about shadow mode...
        return 0

    @prentry
    def rmdir(self, path):
        # small check to keep lost+found from being deleted
        if path == self._LOST_FOUND_PATH:
            raise TmfsOSError(errno.EPERM)
        rsp = self.librarian(self.lcp('rmdir', path=path))
        return 0

    @prentry
    def mkdir(self, path, mode):
        mode = self._MODE_DEFAULT_DIR
        tmp = self.lcp('mkdir', path=path, mode=mode)
        rsp = self.librarian(tmp)
        return 0

    @prentry
    def symlink(self, path, target):
        rsp = self.librarian(self.lcp('symlink', path=path, target=target))
        return 0

    @prentry
    def link(self, target, name):
        raise TmfsOSError(errno.ENOSYS)


def mount_LFS(args):
    '''Expects an argparse::Namespace argument.
       Validate fields and call FUSE'''

    os.makedirs(args.mountpoint, mode=0o777, exist_ok=True)
    os.chmod(args.mountpoint, mode=0o777)

    msg = 'explicit'
    if not args.physloc:
        msg = 'derived'
        try:
            with open(ACPI_NODE_UID, 'r') as uid_file:  # actually a coordinate
                node_uid = uid_file.read().strip()
                assert node_uid.startswith('/MachineVersion/1/Datacenter'), \
                    'Incompatible machine revision in %s' % ACPI_NODE_UID
                elems = node_uid.split('/')
                node_rack = '1'   # MFT, only one rack
                node_enc = elems[elems.index('EncNum') + 1]
                node_id = elems[elems.index('Node') + 1]
                args.physloc = node_rack + ":" + node_enc + ":" + node_id
        except Exception as e:
            # Fabric Emulation auto start shortcut.  If the last three octets
            # of the MAC are equal use that value as the node id (1-80).
            # FAME VMs produced under a container have unpredictable
            # network names, not just eth0.
            HPEOUI = '48:50:42'     # see emulation_configure.bash
            try:
                for fname in glob.glob('/sys/class/net/*/address'):
                    with open(fname) as mac_file:
                        mac = mac_file.read().strip()
                        if not mac.startswith(HPEOUI):
                            continue
                        mac = mac.split(':')
                        assert ((mac[3] == mac[4] == mac[5]) and
                                1 <= int(mac[5]) <= 40), 'Not a FAME node'
                        args.physloc = int(mac[5])
                        break
                else:
                    raise RuntimeError('No network MAC match for %s' % HPEOUI)
            except Exception as e:
                raise SystemExit(
                    'Could not automatically derive coordinate, use --physloc')

    try:
        args.physloc = FRDnode(args.physloc)
    except Exception as e:
        raise SystemExit(
            'Bad %s physical location \'%s\'' % (msg, args.physloc))

    try:
        tmp = socket.gethostbyname(args.hostname)
    except Exception as e:
        logging.warning(
            'could not verify (--hostname) argument \'%s\'' % args.hostname)

    d = int(bool(args.shadow_dir))
    f = int(bool(args.shadow_file))
    tmp = sum((d, f))
    if tmp == 1:
        if args.fixed1906:
            logging.info('shadow_xxxx overrides fixed1906')
            args.fixed1906 = False
    elif tmp > 1:
        raise RuntimeError('Only one of shadow_[dir|file] is allowed')

    try:
        TMFS(LibrarianFS(args),
             args.mountpoint,
             dev=True,
             allow_other=True,
             noatime=True,
             noexec=True,
             foreground=not bool(args.daemon),
             nothreads=True)
    except Exception as e:
        set_trace()    # this should never happen :-)
        raise SystemExit('%s' % str(e))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Librarian File System daemon (lfs_fuse.py)',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--hostname',
        help='ToRMS host running the Librarian',
        type=str,
        default='torms')
    parser.add_argument(
        '--port',
        help='Port on which the Librarian is listening',
        type=int,
        default='9093')
    parser.add_argument(
        '--physloc',
        help='Node physical location "rack:enc:node"',
        type=str,
        default='')
    parser.add_argument(
        '--fixed1906',
        help='Magic mode dependent on zbridge descriptor autoprogramming (TM(AS) only)',
        action='store_true',
        default=False)
    parser.add_argument(
        '--enable_Z',
        help='Enable zbridge/flushtm interaction (FAME only)',
        action='store_true',
        default=False)
    parser.add_argument(
        '--daemon',
        help='Daemonize the program',
        action='store_true',
        default=False)
    parser.add_argument(
        '--mountpoint',
        help='Local directory mountpoint',
        type=str,
        default='/lfs')
    parser.add_argument(
        '--shadow_dir',
        help='directory path for individual shelf shadow files',
        type=str,
        default='')
    parser.add_argument(
        '--shadow_file',
        help='file path for one regular shadow file',
        type=str,
        default='')
    parser.add_argument(
        '--verbose',
        help='level of runtime output (0=ERROR, 1=PERF, 2=NOTICE, 3=INFO, 4=DEBUG, 5=OOB)',
        type=int,
        default=0)
    parser.add_argument(
        '--fakezero',
        help='do not zero deallocated books; use short sleep in zombie state',
        action='store_true',
        default=False)
    args = parser.parse_args(sys.argv[1:])

    msg = 0
    try:
        mount_LFS(args)
    except Exception as e:
        msg = str(e)
    raise SystemExit(msg)
