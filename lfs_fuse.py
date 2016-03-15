#!/usr/bin/python3 -tt

# https://www.kernel.org/doc/Documentation/filesystems/vfs.txt

import errno
import os
import shlex
import subprocess
import stat
import sys
import threading
import time

from pdb import set_trace

from tm_fuse import TMFS, TmfsOSError, Operations, LoggingMixIn, tmfs_get_context

from book_shelf_bos import TMShelf
from cmdproto import LibrarianCommandProtocol
import socket_handling
from frdnode import FRDnode

from lfs_shadow import the_shadow_knows

###########################################################################
# Decorator only for instance methods as it assumes args[0] == "self".
# 0=ERROR, 1=PERF, 2=NOTICE, 3=INFO, 4=DEBUG, 5=OOB)',
# Value     Client (lfs_fuse et al)         Server (Librarian et al)
# == 1      nada                            transactions/second
# >= 2      entry point entry values
# >= 3      entry point return values
#           book lists
#           address space values
# >= 4      Socket byte streams             Socket byte streams
# >= 5      set_trace() (prentry)

def prentry(func):
    def new_func(*args, **kwargs):
        verbose = getattr(args[0], 'verbose', 0)
        if verbose > 1:
            print('----------------------------------')
            tmp = ', '.join([str(a) for a in args[1:]])
            print('%s(%s)' % (func.__name__, tmp[:60]))
        ret = func(*args, **kwargs)
        if verbose > 2:
            tmp = str(ret)
            print('Return', tmp[:128], '...' if len(tmp) > 128 else '')
            if verbose > 4:
                set_trace()
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

    def __init__(self, args):
        '''Validate command-line parameters'''
        self.verbose = args.verbose
        self.tormsURI = args.hostname
        self.mountpoint = args.mountpoint
        elems = args.hostname.split(':')
        assert len(elems) <= 2
        self.host = elems[0]
        try:
            self.port = int(elems[1])
        except Exception as e:
            self.port = 9093

        # Fake it to start.  The umask dance is Pythonic, unfortunately.
        umask = os.umask(0)
        os.umask(umask)
        context = {
            'umask': umask,
            'node_id': args.physloc.node_id,
        }
        self.lcp = LibrarianCommandProtocol(context)

        # Command-line miscues like a bad shadow path.  However that needs a
        # socket, so do it now.

        # connect() has an infinite retry
        self.torms = socket_handling.Client(selectable=False, verbose=self.verbose)
        self.torms.connect(host=self.host, port=self.port)
        if self.verbose > 1:
            print('%s: connected' % self.torms)

        lfs_globals = self.librarian(self.lcp('get_fs_stats'))
        self.bsize = lfs_globals['book_size_bytes']

        self.shadow = the_shadow_knows(args, lfs_globals)
        self.zerosema = threading.Semaphore(value=8)

    # started with "mount" operation.  root is usually ('/', ) probably
    # influenced by FuSE builtin option.  All errors here will essentially
    # be ignored, so if there's potentially fatal stuff, do it in __init__()

    @prentry
    def init(self, root, *args, **kwargs):
        assert not args and not kwargs, 'Unexpected parameters to init()'

        # FIXME: in C FUSE, data returned from here goes into 'getcontext'.
        # Note: I no longer remember my concern on this, look into it again.

    @prentry
    def destroy(self, root):    # fusermount -u
        assert threading.current_thread() is threading.main_thread()
        self.torms.close()
        del self.torms

    # helpers

    def get_bos(self, shelf):
        bos = self.librarian(self.lcp('list_shelf_books', shelf))
        shelf.bos = []
        for b in bos:
            # Returns a TMBook in dict form
            book = self.librarian(self.lcp('get_book', b['book_id']))
            data = {
                    'intlv_group': book['intlv_group'],
                    'book_num': book['book_num'],
                    'lza': book['id'],  # a shifted concatentation of those
                }
            shelf.bos.append(data)
        if self.verbose > 2:
            print('%s BOS: %s' % (shelf.name, shelf.bos))

    # Round 1: flat namespace at / requires a leading / and no others.
    @staticmethod
    def path2shelf(path):
        elems = path.split('/')
        if len(elems) > 2:
            raise TmfsOSError(errno.E2BIG)
        shelf_name = elems[-1]        # if empty, original path was '/'
        return shelf_name

    # First level:  tenants
    # Second level: tenant group
    # Third level:  shelves

    def handleOOB(self):
        # ALTERNATIVE: Put the message on a Queue for the main thread.
        assert threading.current_thread() is threading.main_thread()
        for oob in self.torms.inOOB:
            print('\t\t!!!!!!!!!!!!!!!!!!!!!!!! %s' % oob)
        self.torms.clearOOB()

    def librarian(self, cmdict, errorOK=False):
        '''Dictionary in, dictionary out'''
        # There are times when the process that invoked an action,
        # notably release(), has died by the time this point is reached.
        # In that case uid/gid/pid will all be zero.
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
            print(str(e), file=sys.stederr)
            raise TmfsOSError(errno.ENOKEY)

        errmsg = { }
        rspdict = None
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
            print('%s failed: %s' %
                  (command, errmsg['errmsg']), file=sys.stderr)
            if rspdict is not None and errorOK:
                return rspdict
            raise TmfsOSError(errmsg['errno'])

        try:
            value = rspdict['value']
            rspseq = rspdict['context']['seq']
            if seq != rspseq:
                msg = 'Response not for me %s != %s' % (seq, rspseq)
                raise OSError(errno.EILSEQ, msg)
        except KeyError as e:
            raise OSError(errno.ERANGE, 'Bad response format')

        return value  # None is legal, let the caller deal with it.

    # Higher-level FS operations

    # Called early on nearly all accesses.  Returns os.lstat() equivalent
    # or OSError(errno.ENOENT).  When tenants go live, EPERM can occur.
    # FIXME: move this into Librarian proper.  It should be doing
    # all the calculations, doubly so when we implement tenancy.
    # fh is set if original call was fstat (vs stat or lstat), not sure
    # if it matters or not.  Maybe validate it against cache in shadow files?

    @prentry
    def getattr(self, path, fh=None):
        if fh is not None:
            raise TmfsOSError(errno.ENOENT)  # never saw this in 5 months

        if path == '/':
            now = int(time.time())
            shelves = self.librarian(self.lcp('list_shelves'))
            tmp = {
                'st_uid':       42,
                'st_gid':       42,
                'st_mode':      stat.S_ISVTX + stat.S_IFDIR + 0o777,
                'st_nlink':     len(shelves) + 2,   # '.' and '..'
                'st_size':      4096,
                'st_atime':     now,
                'st_ctime':     now,
                'st_mtime':     now,
            }
            return tmp

        shelf_name = self.path2shelf(path)
        rsp = self.librarian(self.lcp('get_shelf', name=shelf_name))
        shelf = TMShelf(rsp)
        tmp = {
            'st_ctime':     shelf.ctime,
            'st_mtime':     shelf.mtime,
            'st_uid':       42,
            'st_gid':       42,
            'st_mode':      shelf.mode,
            'st_nlink':     1,
            'st_size':      shelf.size_bytes
        }
        if shelf_name.startswith('block'):
            tmp['st_mode'] = self._MODE_DEFAULT_BLK
        return tmp

    @prentry
    def readdir(self, path, index):
        '''Either be a real generator, or get called like one.'''
        if path != '/':
            raise TmfsOSError(errno.ENOENT)
        rsp = self.librarian(self.lcp('list_shelves'))
        yield '.'
        yield '..'
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
            raise TmfsOSError(errno.ENOSYS)    # never saw this in 4 months

        shelf_name = self.path2shelf(path)

        # Piggy back for queries by kernel (globals & fault handling)
        if xattr.startswith('_obtain_'):
            data = self.shadow.getxattr(shelf_name, xattr)
            return bytes(data.encode())

        # "ls" starts with simple getattr but then comes here for
        # security.selinux, system.posix_acl_access, and posix_acl_default.
        # ls -l can also do the same thing on '/'.  Save the round trips.

        if xattr.startswith('security') or not shelf_name:  # path == '/'
            return bytes(0)

        try:
            rsp = self.librarian(
                self.lcp('get_xattr', name=shelf_name, xattr=xattr))
            value = rsp['value']
            assert value is not None    # 'No such attribute'
            return value if isinstance(value, int) else bytes(value.encode())
        except Exception as e:
            raise TmfsOSError(errno.ENODATA)    # syn for ENOATTR

    @prentry
    def listxattr(self, path):
        """getfattr(1) -d calls listxattr(2).  Return a list of names."""
        shelf_name = self.path2shelf(path)
        rsp = self.librarian(
                self.lcp('list_xattrs', name=shelf_name))
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

        shelf_name = self.path2shelf(path)
        for bad in self._badjson:
            if bad in valbytes:
                raise TmfsOSError(errno.EDOMAIN)
        try:
            value = int(valbytes)
        except ValueError as e:
            value = valbytes.decode()

        rsp = self.librarian(
                self.lcp('set_xattr', name=shelf_name,
                         xattr=xattr, value=value))
        if rsp is not None:  # unexpected
            raise TmfsOSError(errno.ENOTTY)

    @prentry
    def removexattr(self, path, xattr):
        shelf_name = self.path2shelf(path)
        rsp = self.librarian(
            self.lcp('remove_xattr', name=shelf_name, xattr=xattr))
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
        print('%s: PID %d' % (args[0], p.pid), file=sys.stderr)
        return p

    # Just say no to the socket.
    def _zero(self, shelf):
        assert shelf.name.startswith(self._ZERO_PREFIX)
        fullpath = '%s/%s' % (self.mountpoint, shelf.name)
        dd = self._cmd2sub(
            '/bin/dd if=/dev/zero of=%s bs=64k conv=notrunc iflag=count_bytes count=%d' % (
            fullpath, shelf.size_bytes))

        with self.zerosema:
            try:
                polled = dd.poll()
                while polled is not None and polled.returncode is None:
                    try:
                        dd.send_signal(os.SIGURS1)
                        stdout, stderr = dd.communicate(timeout=5)
                    except TimeoutExpired as e:
                        print('\n\t%s\n' % stderr, file=sys.stderr)
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
        shelf_name = self.path2shelf(path)
        shelf = TMShelf(self.librarian(self.lcp('get_shelf', name=shelf_name)))

        # Paranoia check for (dangling) opens.  Does VFS catch these first?
        cached = self.shadow[shelf.name]
        if cached is not None:                  # Not a good sign
            open_handles = cached.open_handle
            if open_handles is not None:        # Definitely a bad sign
                set_trace()
                raise TmfsOSError(errno.EBUSY)

                # Once upon a time I forced it...
                for fh in open_handles.values():
                    try:
                        self.librarian(self.lcp('close_shelf',
                                                id=shelf.id,
                                                fh=fh))
                    except Exception as e:
                        pass

        self.shadow.unlink(shelf_name)  # empty cache INCLUDING dangling opens

        # Early exit: no explicit zeroing required if:
        # 1. it's zero bytes long
        # 2. it's passed through zeroing, meaning this is the second trip to
        #    unlink()..  Even if length is non-zero, remove it, assuming the
        #   _zero subprocess failed.
        # 3. The shadow subclass doesn't need it
        if ((not shelf.size_bytes) or
             shelf.name.startswith(self._ZERO_PREFIX) or
             (not self.shadow.zero_on_unlink)):
            self.librarian(self.lcp('destroy_shelf', name=shelf.name))
            return 0

        # Schedule for zeroing; a second entry to unlink() will occur on
        # this shelf with the new name.
        zeroname = '%s%d' % (self._ZERO_PREFIX, shelf.id)
        if zeroname != shelf_name:
            self.rename(shelf.name, zeroname)
            shelf.name = zeroname
        threading.Thread(target=self._zero, args=(shelf, )).start()
        return 0

    @prentry
    def utimens(self, path, times=None):
        shelf_name = self.path2shelf(path)  # bomb here on '/'
        if times is not None:
            times = tuple(map(int, times))
            if abs(int(time.time() - times[1])) < 3:  # "now" on this system
                times = None
        if times is None:
            times = (0, 0)  # let librarian pick it
        self.librarian(
            self.lcp('set_am_time', name=shelf_name,
                     atime=times[0],
                     mtime=times[1]))
        return 0  # os.utime

    @prentry
    def open(self, path, flags, mode=None):
        # looking for filehandles?  See FUSE docs.  Librarian will do
        # all access calculations so call it first.
        shelf_name = self.path2shelf(path)
        rsp = self.librarian(self.lcp('open_shelf', name=shelf_name))
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
            # createat(2), methinks, but I never saw this in 6 months
            raise TmfsOSError(errno.ENOSYS)
        shelf_name = self.path2shelf(path)
        if supermode is None:
            mode &= 0o777
            tmpmode = stat.S_IFREG + mode
        else:
            mode = supermode & 0o777
            tmpmode = supermode
        tmp = self.lcp('create_shelf', name=shelf_name, mode=tmpmode)
        rsp = self.librarian(tmp)
        shelf = TMShelf(rsp)                # This is an open shelf...
        fx = self.shadow.create(shelf, mode)     # ...added to the cache...
        return fx            # ...with this value.

    @prentry
    def read(self, path, length, offset, fh):
        shelf_name = self.path2shelf(path)
        return self.shadow.read(shelf_name, length, offset, fh)

    @prentry
    def write(self, path, buf, offset, fh):
        shelf_name = self.path2shelf(path)

        # Resize shelf "on the fly" for writes past EOF
        # BUG: what if shelf was resized elsewhere?  And what about read?
        req_size = offset + len(buf)
        if self.shadow[shelf_name].size_bytes < req_size:
            self.truncate(path, req_size, fh) # updates the cache

        return self.shadow.write(shelf_name, buf, offset, fh)

    @prentry
    def truncate(self, path, length, fh=None):
        '''truncate(2) calls with fh == None; based on path but access
           must be checked.  ftruncate passes in open handle'''
        shelf_name = self.path2shelf(path)
        # ALWAYS get the shelf by name, even if fh is valid.
        # IMPLICIT ASSUMPTION: without tenants this will never EPERM
        rsp = self.librarian(self.lcp('get_shelf', name=shelf_name))
        req = self.lcp('resize_shelf',
                       name=shelf_name,
                       size_bytes=length,
                       id=rsp['id'])
        rsp = self.librarian(req)
        shelf = TMShelf(rsp)
        if shelf.size_bytes < length:
            raise TmfsOSError(errno.EINVAL)
        self.get_bos(shelf)
        return self.shadow.truncate(shelf, length, fh)

    @prentry
    def ioctl(self, path, cmd, arg, fh, flags, data):
        shelf_name = self.path2shelf(path)
        return self.shadow.ioctl(shelf_name, cmd, arg, fh, flags, data)

    @prentry
    def fallocate(self, path, mode, offset, length, fh=None):
        if mode > 0:
            return -1
        shelf_name = self.path2shelf(path)
        rsp = self.librarian(self.lcp('get_shelf', name=shelf_name))
        shelf = TMShelf(rsp)
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
        old = self.path2shelf(old)
        rsp = self.librarian(self.lcp('get_shelf', name=old))
        shelf = TMShelf(rsp)
        new = self.path2shelf(new)
        self.shadow.rename(old, new)
        req = self.lcp('rename_shelf', name=old, id=shelf.id, newname=new)
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
        raise TmfsOSError(errno.ENOSYS)

    @prentry
    def mknod(self, path, mode, dev):
        # File can't exist already or upper levels of VFS reject the call.
        if mode & stat.S_IFBLK != stat.S_IFBLK:
            raise TmfsOSError(errno.ENOTBLK)
        shelf_name = self.path2shelf(path)
        rsp = self.librarian(self.lcp('get_shelf', name=shelf_name),
                                      errorOK=True)
        if 'errmsg' not in rsp:
            raise TmfsOSError(errno.EEXIST)
        nbooks = dev & 0xFF     # minor
        if not nbooks:
            raise TmfsOSError(errno.EINVAL)
        mode &= 0o777
        fh = self.create(path, 0, supermode=stat.S_IFBLK + mode)  # w/shadow
        self.truncate(path, nbooks * self.bsize, fh)
        self.release(path, fh)
        # mknod(1m) immediately does a stat looking for S_IFBLK.
        # Not sure what else to do about shadow mode...
        return 0

    @prentry
    def rmdir(self, path):
        raise TmfsOSError(errno.ENOSYS)

    @prentry
    def mkdir(self, path, mode):
        raise TmfsOSError(errno.ENOSYS)

    @prentry
    def symlink(self, name, target):
        raise TmfsOSError(errno.ENOSYS)

    @prentry
    def link(self, target, name):
        raise TmfsOSError(errno.ENOSYS)


def mount_LFS(args):
    '''Expects an argparse::Namespace argument.
       Validate fields and call FUSE'''
    assert os.path.isdir(
        args.mountpoint), 'No such directory %s' % args.mountpoint
    try:
        args.physloc = FRDnode(args.physloc)
    except Exception as e:
        raise SystemExit('Bad physical location %s' % args.physloc)
    d = int(bool(args.shadow_dir))
    f = int(bool(args.shadow_file))
    tmp = sum((d, f))
    if tmp == 1:
        if args.fixed1906:
            print('shadow_xxxx overrides fixed1906')
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
        raise SystemExit('%s' % str(e))

if __name__ == '__main__':

    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(
        description='Librarian File System daemon (lfs_fuse.py)')
    parser.add_argument(
        'hostname',
        help='ToRMS host running the Librarian',
        type=str)
    parser.add_argument(
        'physloc',
        help='Node physical location "rack:enc:node"',
        type=str)
    parser.add_argument(
        '--fixed1906',
        help='Magic mode dependent on zbridge descriptor autoprogramming',
        action='store_false',
        default=True)       # for now
    parser.add_argument(
        '--noZ',
        help='Don\'t talk to zbridge driver (FAME only)',
        action='store_false',
        default=True)       # for now
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
    args = parser.parse_args(sys.argv[1:])

    msg = 0
    try:
        mount_LFS(args)
    except Exception as e:
        msg = str(e)
    raise SystemExit(msg)
