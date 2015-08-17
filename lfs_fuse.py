#!/usr/bin/python3 -tt

# From Stavros

import errno
import os
import sys
import time

from pdb import set_trace

from fuse import FUSE, FuseOSError, Operations

from book_shelf_bos import TMShelf
from cmdproto import LibrarianCommandProtocol
import socket_handling

# 0 == all prints, 1 == fewer prints, >1 == turn off other stuff

_perf = int(os.getenv('PERF', 0))   # FIXME: clunky

# Decorator only for instance methods as it assumes args[0] == "self".
# Probably a better spot to place this.
def prentry(func):
    if _perf > 1:
        return func # No print, no OOB check

    def new_func(*args, **kwargs):
        self = args[0]
        if not _perf:
            print('----------------------------------')
            tmp = ', '.join([str(a) for a in args[1:]])
            print('%s(%s)' % (func.__name__, tmp[:60]))
        ret = func(*args, **kwargs)
        if self.torms.inOOB:
            print('\n!!!!!!!!!!!!!!!!!!!!!!!!! %s !!!!!!!!!!!!!!!!!!!!!!!!!!!!\n ' %
                  self.torms.inOOB)
            self.torms.inOOB = ''
        return ret

    # Be a well-behaved decorator
    new_func.__name__ = func.__name__
    new_func.__doc__ = func.__doc__
    new_func.__dict__.update(func.__dict__)
    return new_func

class LibrarianFS(Operations):  # Name shows up in mount point

    _mode_default_file = int('0100666', 8)  # isfile, 666
    _mode_default_dir =  int('0040777', 8)  # isdir, 777

    def __init__(self, source, node_id):
        '''Validate parameters'''
        path = self.shadowpath('')  # trailing '/': Looking for Mr. GoodDir
        try:
            stat = os.stat(path)    # a file will throw 'NotADirectory'
            if stat.st_mode != self._mode_default_dir:
                raise SystemExit('%s is not mode 777' % path)
        except Exception as e:
            raise SystemExit('Directory %s does not exist' % path)
        self.tormsURI = source
        elems = source.split(':')
        assert len(elems) <= 2
        self.host = elems[0]
        try:
            self.port = int(elems[1])
        except Exception as e:
            self.port = 9093

        # Calls to fuse.fuse_get_context() were...disappointing.
        # Fake it for now.  The umask dance is Pythonic, unfortunately.
        umask = os.umask(0)
        os.umask(umask)
        context = {
            'uid': os.geteuid(),
            'gid': os.getegid(),
            'pid': os.getpid(),
            'umask': umask,
            'node_id': node_id,
        }
        self.lcp = LibrarianCommandProtocol(context)

    # started with "mount" operation.  root is usually ('/', ) probably
    # influenced by FuSE builtin option.

    def init(self, root, **kwargs):
        try:    # set up a blocking socket
            self.torms = socket_handling.Client(
                selectable=False, perf=_perf)
            self.torms.connect(host=self.host, port=self.port)
            print('%s: connected' % self.torms)
        except Exception as e:
            raise FuseOSError(errno.EHOSTUNREACH)
       # FIXME: in C FUSE, data returned here goes into 'getcontext'

    def destroy(self, root):
        self.torms.close()
        del self.torms

    # helpers

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

    fd2shelf_id = { }

    @staticmethod
    def shadowpath(shelf_name):
        return '/var/lib/lfs/shadow/%s' % shelf_name

    # First level:  tenants
    # Second level: tenant group
    # Third level:  shelves

    def librarian(self, cmdict):
        '''Dictionary in, dictionary out'''
        try:
            # validate primary keys
            command = cmdict['command']
            seq = cmdict['context']['seq']
            # tid = threading.get_ident()
            # print('%s[%d:%d]' % (command, tid, seq))
        except KeyError as e:
            print(str(e))
            raise FuseOSError(errno.ENOKEY)

        value = { }
        try:
            self.torms.send_all(cmdict)
            rsp = self.torms.recv_chunk(selectable=False)
            value = rsp['value']
            rspseq = rsp['context']['seq']
            assert seq == rspseq, 'Not for me %s != %s' % (seq, rspseq)
        except OSError as e:
            value['errmsg'] = 'Communications error with librarian'
            value['errno'] = errno.EHOSTDOWN
        except KeyError as e:
            value['errmsg'] = 'No key: %s' % str(e)
            value['errno'] = errno.ENOKEY
        except Exception as e:
            value['errmsg'] = str(e)
            value['errno'] = errno.EREMOTEIO

        if value and 'errmsg' in value:
            print('%s failed: %s' % (command, value['errmsg']),
                  file=sys.stderr)
            raise FuseOSError(value['errno'])
        return value # None is legal, let the caller deal with it.

    # Higher-level FS operations

    # Called early on nearly all accesses.  Returns os.lstat() equivalent
    # or OSError(errno.ENOENT)
    @prentry
    def getattr(self, path, fh=None):
        if not fh is None:      # dir listings no dice, only open files
            set_trace()
            pass
        if path == '/':
            now = int(time.time())
            shelves = self.librarian(self.lcp('list_shelves'))
            tmp = {
                'st_uid':       42,
                'st_gid':       42,
                'st_mode':      int('0041777', 8),  # isdir, sticky, 777
                'st_nlink':     len(shelves) + 2,   # account for '.' and '..'
                'st_size':      4096,
                'st_atime':     now,
                'st_ctime':     now,
                'st_mtime':     now,
            }
            return tmp

        shelf_name = self.path2shelf(path)
        rsp = self.librarian(self.lcp('get_shelf', name=shelf_name))
        try:
            assert rsp['name'] == shelf_name
        except Exception as e:
            raise FuseOSError(errno.ENOENT)

        # Calculate mode on the fly.  For now, all books must come
        # from same node.
        shelf = TMShelf(rsp)
        mode = self._mode_default_file  # gotta start somewhere
        if False and shelf.book_count:
            set_trace()
            bos = self.db.get_bos_by_shelf_id(shelf.id)
            book = self.db.get_book_by_id(bos[0].book_id)
            if book.node_id != self.lcp._context['node_id']:
                mode = int('0100111', 8)

        tmp = {
                'st_ctime':     shelf.ctime,
                'st_mtime':     shelf.mtime,
                'st_uid':       42,
                'st_gid':       42,
                'st_mode':      mode,
                'st_nlink':     1,
                'st_size':      shelf.size_bytes
              }
        return tmp

    @prentry
    def readdir(self, path, fh):
        if path != '/':
            raise FuseOSError(errno.ENOENT)
        rsp = self.librarian(self.lcp('list_shelves'))
        yield '.'
        yield '..'
        try:
            for shelf in rsp:
                yield shelf['name']
        except Exception as e:
            print(str(e))
            set_trace()
            raise

    # os.getaccess(path, mode): returns nothing (None), or
    # raise EACCESS.
    @prentry
    def access(self, path, mode):   # returned nothing, but maybe 0?
        try:
            attrs = self.getattr(path)
        except Exception as e:
            raise FuseOSError(errno.EACCES)
        # FIXME: compare mode to attrs

    #----------------------------------------------------------------------
    # Extended attributes: apt-get install attr, then man 5 attr.  Legal
    # namespaces are (user, system, trusted, security).  Anyone can
    # see "user", but the others take CAP_SYS_ADMIN.  Currently only
    # "user" works, even with sudo, not sure why.  Or if it matters.
    # Something (fusepy, I imagine) always calls getattr() first, so it's
    # a reasonable assumption the shelf exists.

    @prentry
    def getxattr(self, path, attr, position=0):
        """Called with a specific namespace.name attr.  Can return either
           a bytes array OR an int."""
        if position:
            set_trace()

        # "ls" starts with simple getattr but then comes here for
        # security.selinux, system.posix_acl_access, and posix_acl_default.
        # ls -l can also do the same thing on '/'.  Save the round trips.

        try:
            shelf_name = self.path2shelf(path)
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
    def setxattr(self, path, attr, valbytes, options, position=0):
        # options from linux/xattr.h: XATTR_CREATE = 1, XATTR_REPLACE = 2
        if options:
            set_trace() # haven't actually seen it yet
        shelf_name = self.path2shelf(path)
        assert attr.startswith('user.')
        for bad in self._badjson:
            if bad in valbytes:
                raise FuseOSError(errno.EDOMAIN)
        try:
            value = int(valbytes)
        except ValueError as e:
            value = valbytes.decode()

        rsp = self.librarian(
                self.lcp('set_xattr', name=shelf_name,
                         xattr=attr, value=value))
        if rsp is not None: # unexpected
            raise FuseOSError(errno.ENOTTY)

    @prentry
    def removexattr(self, path, xattr):
        shelf_name = self.path2shelf(path)
        rsp = self.librarian(
            self.lcp('destroy_xattr', name=shelf_name, xattr=attr))
        if rsp is not None: # unexpected
            raise FuseOSError(errno.ENOTTY)

    @prentry
    def statfs(self, path): # "df" command; example used statVfs.
        globals = self.librarian(self.lcp('get_fs_stats'))
        # A book is a block.  Let other commands do the math.

        # Where a stress error occurs, jfdi is
        # df /lfs
        # ls -al /lfs
        # touch /lfs/junk
        # what comes in here is the response for ls -al, a list.
        # not sure how this gets out of sync.  A two-stanza jfdi w/o
        # touch does not get out of sync.  Now try a truncate and
        # see if that goes out of sync, ie, it's the writes or is it
        # just touch?
        try:
            blocks = globals['books_total']
        except Exception as e:
            raise   # back out to fuse.py
        bfree = bavail = blocks - globals['books_used']
        bsize =globals['book_size_bytes']
        return {
            'f_bavail':     bavail, # free blocks for unpriv users
            'f_bfree':      bfree,  # total free DATA blocks
            'f_blocks':     blocks, # total DATA blocks
            'f_bsize':      bsize,  # optimal transfer block size???
            'f_favail':     bavail, # free inodes for unpriv users
            'f_ffree':      bfree,  # total free file inodes
            'f_files':      blocks, # total number of inodes
            'f_flag':       63,     # mount flags
            'f_frsize':     0,      # fragment size
            'f_namemax':    255,    # maximum filename length
        }

    @prentry
    def unlink(self, path, *args, **kwargs):
        assert not args and not kwargs, 'Unexpected args'
        shelf_name = self.path2shelf(path)
        rsp = self.librarian(self.lcp('destroy_shelf', name=shelf_name))
        assert rsp['id'] not in self.fd2shelf_id.values()
        try:
            os.unlink(self.shadowpath(shelf_name))
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
        return 0

    @prentry
    def utimens(self, path, times=None):
        shelf_name = self.path2shelf(path)  # bomb here on '/'
        if times is not None:
            times = tuple(map(int, times))
            if abs(int(time.time() - times[1])) < 3:    # "now" on this system
                times = None
        if times is None:
            times = (0, 0)  # let librarian pick it
        self.librarian(
            self.lcp('set_am_time', name=shelf_name,
                     atime=times[0],
                     mtime=times[1]))
        return 0    # os.utime

    @prentry
    def open(self, path, flags, **kwargs):
        # looking for filehandles?  See FUSE docs
        assert not kwargs, 'open() with kwargs %s' % str(kwargs)
        shelf_name = self.path2shelf(path)
        rsp = self.librarian(self.lcp('open_shelf', name=shelf_name))
        try:
            shelf_id = rsp['id']
            fd = os.open(self.shadowpath(shelf_name), flags)
        except Exception as e:
            raise FuseOSError(errno.ENOENT)
        self.fd2shelf_id[fd] = shelf_id
        return fd

    # from shell: touch | truncate /lfs/nofilebythisname
    # return os.open()
    @prentry
    def create(self, path, mode, fi=None):
        assert fi is None, 'create(%s) with FI not implemented' % str(fi)
        shelf_name = self.path2shelf(path)
        rsp = self.librarian(self.lcp('create_shelf', name=shelf_name))
        if rsp['name'] is None:
            raise FuseOSError(errno.EEXIST)
        try:
            shelf_id = rsp['id']
            fd = os.open(self.shadowpath(shelf_name), mode + os.O_CREAT)
        except Exception as e:
            raise FuseOSError(errno.ENOENT)
        self.fd2shelf_id[fd] = shelf_id
        return fd

    @prentry
    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    @prentry
    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    @prentry
    # it was opened before this, but where is the fd (aka shelf id)?
    # Example code shows an explicit open by name in here.
    # example returned nothing?
    def truncate(self, path, length, *args, **kwargs):
        assert not kwargs, 'truncate() with kwargs %s' % str(kwargs)
        shelf_name = self.path2shelf(path)
        listrsp = self.librarian(self.lcp('get_shelf', name=shelf_name))
        req = self.lcp('resize_shelf',
                        name=shelf_name,
                        size_bytes=length,
                        id=listrsp['id'])
        rsp = self.librarian(req)
        if not 'size_bytes' in rsp:
            raise FuseOSError(errno.EINVAL)
        return os.truncate(self.shadowpath(shelf_name), length)

    @prentry
    def release(self, path, fh):    # fh == shelfid
        shelf_name = self.path2shelf(path)
        req = self.lcp('close_shelf', name=shelf_name,
                        id=self.fd2shelf_id[fh])
        rsp = self.librarian(req)
        try:
            assert rsp['name'] == shelf_name
            del self.fd2shelf_id[fh]
            return 0    # os.close...
        except Exception as e:
            raise FuseOSError(errno.EEXIST)

    @prentry
    def flush(self, path, fh):      # fh == shelfid
        return 0

    @prentry
    def fsync(self, path, fdatasync, fh):
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

def main(source, mountpoint, node_id):
    try:
        FUSE(LibrarianFS(source, node_id),
            mountpoint,
            allow_other=True,
            noatime=True,
            foreground=True,
            nothreads=True)
    except Exception as e:
        raise SystemExit('fusermount probably failed, retry in foreground')

if __name__ == '__main__':
    main(*sys.argv[1:])  # source mountpoint node_id
