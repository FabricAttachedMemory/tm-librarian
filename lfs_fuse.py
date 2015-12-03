#!/usr/bin/python3 -tt

# https://www.kernel.org/doc/Documentation/filesystems/vfs.txt

import errno
import os
import sys
import time

from pdb import set_trace

from tm_fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context

from book_shelf_bos import TMShelf
from cmdproto import LibrarianCommandProtocol
import socket_handling

from lfs_shadow import the_shadow_knows

# 0 == all prints, 1 == fewer prints, >1 == turn off other stuff

_perf = int(os.getenv('PERF', 0))   # FIXME: clunky

# Decorator only for instance methods as it assumes args[0] == "self".
# FIXME: find a better spot to place this.


def prentry(func):
    if _perf > 1:
        return func  # No print, no OOB check

    def new_func(*args, **kwargs):
        self = args[0]
        if not _perf:
            print('----------------------------------')
            tmp = ', '.join([str(a) for a in args[1:]])
            print('%s(%s)' % (func.__name__, tmp[:60]))
        ret = func(*args, **kwargs)
        return ret

    # Be a well-behaved decorator
    new_func.__name__ = func.__name__
    new_func.__doc__ = func.__doc__
    new_func.__dict__.update(func.__dict__)
    return new_func

###########################################################################
# Errors that that get raised from __init__ will terminate the process.
# Errors raised anywhere else (including "init") get swallowed by fuse.py
# so that lfs_fuse.py can struggle on.   That's a good thing overall,
# but dictaes where certain operations should be placed.

class LibrarianFS(Operations):  # Name shows up in mount point

    _mode_default_file = int('0100666', 8)  # isfile, 666
    _mode_default_dir = int('0040777', 8)  # isdir, 777
    ig_gap = {}

    def __init__(self, args):
        '''Validate command-line parameters'''
        self.verbose = args.verbose
        self.tormsURI = args.hostname
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
            'node_id': args.node_id,
        }
        self.lcp = LibrarianCommandProtocol(context)

        # Command-line miscues like a bad shadow path.  However that needs a
        # socket, so do it now.

        # connect() has an infinite retry
        self.torms = socket_handling.Client(selectable=False, perf=_perf)
        self.torms.connect(host=self.host, port=self.port)
        if self.verbose > 1:
            print('%s: connected' % self.torms)

        globals = self.librarian(self.lcp('get_fs_stats'))
        self.shadow = the_shadow_knows(args, globals)

    # started with "mount" operation.  root is usually ('/', ) probably
    # influenced by FuSE builtin option.

    @prentry
    def init(self, root, **kwargs):
        globals = self.librarian(self.lcp('get_fs_stats'))

        # FIXME: in C FUSE, data returned here goes into 'getcontext'
        # Note: I no longer remember my concern on this, look into it again.

        # Calculate node LZA gap
        self.bsize = globals['book_size_bytes']
        books = self.librarian(self.lcp('get_book_all'))

        prev_ig = -1
        prev_lza = -1
        total_gap = 0

        for book in books:
            cur_lza = book['id']
            cur_ig = book['intlv_group']

            if prev_ig != cur_ig:
                cur_ig_gap = cur_lza - prev_lza - 1
                total_gap += cur_ig_gap
                self.ig_gap[cur_ig] = total_gap

            prev_lza = cur_lza
            prev_ig = cur_ig

        if self.verbose > 2:
            print("ig_gap:", self.ig_gap)

    @prentry
    def destroy(self, root):    # fusermount -u
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
                    'lza': book['id'],
                    'intlv_group': book['intlv_group']
                }
            shelf.bos.append(data)
        if self.verbose > 2:
            print('%s BOS: %s' % (shelf.name, shelf.bos))

    # Round 1: flat namespace at / requires a leading / and no others
    @staticmethod
    def path2shelf(path, needShelf=True):
        elems = path.split('/')
        if len(elems) != 2:
            raise FuseOSError(errno.E2BIG)
        shelf_name = elems[-1]        # if empty, original path was '/'
        if needShelf and not shelf_name:
            raise FuseOSError(errno.EINVAL)
        return shelf_name

    # First level:  tenants
    # Second level: tenant group
    # Third level:  shelves

    def handleOOB(self):
        for oob in self.torms.inOOB:
            print('\t\t!!!!!!!!!!!!!!!!!!!!!!!! %s' % oob)
        self.torms.clearOOB()

    def librarian(self, cmdict):
        '''Dictionary in, dictionary out'''
        # There are times when the process that invoked an action,
        # notably release(), has died by the time this point is reached.
        # In that case uid/gid/pid will all be zero.
        context = cmdict['context']
        (context['uid'],
         context['gid'],
         context['pid']) = fuse_get_context()
        try:
            # validate primary keys
            command = cmdict['command']
            seq = cmdict['context']['seq']
        except KeyError as e:
            print(str(e), file=sys.stederr)
            raise FuseOSError(errno.ENOKEY)

        errmsg = { }
        try:
            self.torms.send_all(cmdict)
            rspdict = None
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

        if errmsg:
            print('%s failed: %s' %
                  (command, errmsg['errmsg']), file=sys.stderr)
            raise FuseOSError(errmsg['errno'])

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
    @prentry
    def getattr(self, path, fd=None):
        '''fd is set if original call was fstat (vs stat or lstat)'''
        if path == '/':
            now = int(time.time())
            shelves = self.librarian(self.lcp('list_shelves'))
            tmp = {
                'st_uid':       42,
                'st_gid':       42,
                'st_mode':      0o0041777,      # isdir, sticky, 777
                'st_nlink':     len(shelves) + 2,   # '.' and '..'
                'st_size':      4096,
                'st_atime':     now,
                'st_ctime':     now,
                'st_mtime':     now,
            }
            return tmp

        if fd is None:
            shelf_name = self.path2shelf(path)
            rsp = self.librarian(self.lcp('get_shelf', name=shelf_name))
            shelf = TMShelf(rsp)
            tmp = {
                'st_ctime':     shelf.ctime,
                'st_mtime':     shelf.mtime,
                'st_uid':       42,
                'st_gid':       42,
                'st_mode':      self._mode_default_file,
                'st_nlink':     1,
                'st_size':      shelf.size_bytes
              }
            return tmp

        # Haven't seen this yet...
        try:
            set_trace()
            tmp = self.shadow.getattr(fd)
        except Exception as e:
            raise FuseOSError(errno.ENOENT)

    @prentry
    def readdir(self, path, index):
        '''Either be a real generator, or get called like one.'''
        if path != '/':
            raise FuseOSError(errno.ENOENT)
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
            raise FuseOSError(errno.EACCES)

    #----------------------------------------------------------------------
    # Extended attributes: apt-get install attr, then man 5 attr.  Legal
    # namespaces are (user, system, trusted, security).  Anyone can
    # see "user", but the others take CAP_SYS_ADMIN.  Currently only
    # "user" works, even with sudo, not sure why, or if it really matters.
    # Something (fusepy, I imagine) always calls getattr() first, so it's
    # a reasonable assumption the shelf exists.

    @prentry
    def getxattr(self, path, attr, position=0):
        """Called with a specific namespace.name attr.  Can return either
           a bytes array OR an int."""
        shelf_name = self.path2shelf(path)
        if position:
            set_trace()

        # Piggy back on getxattr to retrieve LZA during fault handling
        # input : "fault_get_lza":<byte offset into shelf>
        # output: <lza>:<book offset>:<book size>:<aperture base>
        if "fault_get_lza" in attr:
            data = self.shadow.getxattr(shelf_name, attr)
            return bytes(data.encode())

        # "ls" starts with simple getattr but then comes here for
        # security.selinux, system.posix_acl_access, and posix_acl_default.
        # ls -l can also do the same thing on '/'.  Save the round trips.

        try:
            rsp = self.librarian(
                self.lcp('get_xattr', name=shelf_name, xattr=attr))
            value = rsp['value']
            assert value
            return value if isinstance(value, int) else bytes(value.encode())
        except Exception as e:
            raise FuseOSError(errno.ENODATA)    # syn for ENOATTR

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
    def setxattr(self, path, xattr, valbytes, options, position=0):
        # options from linux/xattr.h: XATTR_CREATE = 1, XATTR_REPLACE = 2
        if options:
            set_trace()  # haven't actually seen it yet

        # 'Extend' user.xxxx syntax and screen for it here
        elems = xattr.split('.')
        if elems[0] != 'user' or len(elems) < 2:
            raise FuseOSError(errno.EINVAL)

        shelf_name = self.path2shelf(path)
        for bad in self._badjson:
            if bad in valbytes:
                raise FuseOSError(errno.EDOMAIN)
        try:
            value = int(valbytes)
        except ValueError as e:
            value = valbytes.decode()

        rsp = self.librarian(
                self.lcp('set_xattr', name=shelf_name,
                         xattr=xattr, value=value))
        if rsp is not None:  # unexpected
            raise FuseOSError(errno.ENOTTY)

    @prentry
    def removexattr(self, path, xattr):
        shelf_name = self.path2shelf(path)
        rsp = self.librarian(
            self.lcp('remove_xattr', name=shelf_name, xattr=xattr))
        if rsp is not None:  # unexpected
            raise FuseOSError(errno.ENOTTY)

    @prentry
    def statfs(self, path):  # "df" command; example used statVfs.
        globals = self.librarian(self.lcp('get_fs_stats'))
        # A book is a block.  Let other commands do the math.
        blocks = globals['books_total']
        bfree = bavail = blocks - globals['books_used']
        bsize = globals['book_size_bytes']
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

    @prentry
    def unlink(self, path, *args, **kwargs):
        assert not args and not kwargs, 'unlink: nexpected args'
        shelf_name = self.path2shelf(path)
        self.shadow.unlink(shelf_name)
        rsp = self.librarian(self.lcp('destroy_shelf', name=shelf_name))
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
        fd = self.shadow.open(shelf, flags, mode)
        return fd

    # from shell: touch | truncate /lfs/nofilebythisname
    # return os.open().
    @prentry
    def create(self, path, mode, fi=None):
        if fi is not None:
            print('create(%s) with FI not implemented' % str(fi))
            set_trace()
        shelf_name = self.path2shelf(path)
        rsp = self.librarian(self.lcp('create_shelf', name=shelf_name))
        shelf = TMShelf(rsp)
        fd = self.shadow.create(shelf, mode)
        return fd

    @prentry
    def read(self, path, length, offset, fd):

        shelf_name = self.path2shelf(path)
        return self.shadow.read(shelf_name, length, offset, self.ig_gap, fd)

    @prentry
    def write(self, path, buf, offset, fd):

        shelf_name = self.path2shelf(path)

        # Resize shelf "on the fly" for writes past EOF
        # BUG: what if shelf was resized elsewhere?  And what about read?
        req_size = offset + len(buf)
        if self.shadow[shelf_name].size_bytes < req_size:
            self.truncate(path, req_size, None) # updates the cache

        return self.shadow.write(shelf_name, buf, offset, self.ig_gap, fd)

    @prentry
    def truncate(self, path, length, fd=None):
        '''truncate(2) calls with fd == None; based on path but access
           must be checked.  ftruncate passes in open handle'''
        shelf_name = self.path2shelf(path)
        # ALWAYS get the shelf by name, even if fd is valid.
        # IMPLICIT ASSUMPTION: without tenants this will never EPERM
        rsp = self.librarian(self.lcp('get_shelf', name=shelf_name))
        req = self.lcp('resize_shelf',
                       name=shelf_name,
                       size_bytes=length,
                       id=rsp['id'])
        rsp = self.librarian(req)
        shelf = TMShelf(rsp)
        if shelf.size_bytes < length:
            raise FuseOSError(errno.EINVAL)
        self.get_bos(shelf)
        self.shadow.truncate(shelf, length, fd)

    @prentry
    def fallocate(self, path, mode, offset, length, fd=None):
        if mode > 0:
            raise FuseOSError(errno.EPERM)
        self.truncate(path, length, None)

    # Called when last reference to an open file is closed.
    @prentry
    def release(self, path, fd):  # fd == shadow file descriptor
        try:
            shelf = self.shadow.release(fd)
            req = self.lcp(
                'close_shelf', id=shelf.id, open_handle=shelf.open_handle)
            self.librarian(req)  # None or raise
        except Exception as e:
            raise FuseOSError(errno.ESTALE)

    @prentry
    def flush(self, path, fd):
        '''May be called zero, one, or more times per shelf open.  It's a
           chance to report delayed errors, not a syscall passthru.'''
        return 0

    @prentry
    def fsync(self, path, datasync, fd):
        raise FuseOSError(errno.ENOSYS)

    @prentry
    def fsyncdir(self, path, datasync, fd):
        raise FuseOSError(errno.ENOSYS)

    @prentry
    def bmap(self, path, blocksize, blockno):
        '''Only if "target" is a filesystem on a block device.  Convert
           file-relative blockno to device-relative block.'''
        raise FuseOSError(errno.ENOSYS)

    #######################################################################
    # Not gonna happen...ever?

    @prentry
    def rename(self, old, new):
        raise FuseOSError(errno.ENOSYS)

    @prentry
    def chmod(self, path, mode, **kwargs):
        raise FuseOSError(errno.ENOSYS)

    @prentry
    def chown(self, path, uid, gid):
        raise FuseOSError(errno.ENOSYS)

    @prentry
    def readlink(self, path):
        raise FuseOSError(errno.ENOSYS)

    @prentry
    def mknod(self, path, mode, dev):
        raise FuseOSError(errno.ENOSYS)

    @prentry
    def rmdir(self, path):
        raise FuseOSError(errno.ENOSYS)

    @prentry
    def mkdir(self, path, mode):
        raise FuseOSError(errno.ENOSYS)

    @prentry
    def symlink(self, name, target):
        raise FuseOSError(errno.ENOSYS)

    @prentry
    def link(self, target, name):
        raise FuseOSError(errno.ENOSYS)


def mount_LFS(args):
    '''Expects an argparse::Namespace argument.
       Validate fields and call FUSE'''
    assert os.path.isdir(
        args.mountpoint), 'No such directory %s' % args.mountpoint
    assert 1 <= args.node_id <= 80, 'Node ID must be from 1 - 999'
    d = int(bool(args.shadow_dir))
    f = int(bool(args.shadow_file))
    i = int(bool(args.shadow_ivshmem))
    m = int(bool(args.fam))
    assert sum(
        (d, f, i, m)) == 1, 'Exactly one of shadow_[dir|file|ivshmem] | fam is required'

    try:
        FUSE(LibrarianFS(args),
             args.mountpoint,
             allow_other=True,
             noatime=True,
             foreground=not bool(args.daemon),
             nothreads=True)
    except Exception as e:
        raise SystemExit('%s' % str(e))

if __name__ == '__main__':

    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(
        description='Librarian File System Daemon (LFSd)')
    parser.add_argument(
        'hostname',
        help='ToRMS host running the Librarian',
        type=str)
    parser.add_argument(
        'mountpoint',
        help='Local directory mountpoint',
        type=str,
        default='/lfs')
    parser.add_argument(
        'node_id',
        help='Numeric node id',
        type=int)
    parser.add_argument(
        '--daemon',
        help='Daemonize the program',
        action='store_true',
        default=False)
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
        '--shadow_ivshmem',
        help='file path to IVSHMEM device',
        type=str,
        default='')
    parser.add_argument(
        '--fam',
        help='physical address of FAM base(hexadecimal format 0x...)',
        type=str,
        default='')
    parser.add_argument(
        '--verbose',
        help='level of runtime output, larger -> more',
        type=int,
        default=0)
    args = parser.parse_args(sys.argv[1:])

    msg = 0
    try:
        mount_LFS(args)
    except Exception as e:
        msg = str(e)
    raise SystemExit(msg)
