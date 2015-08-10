#!/usr/bin/python3 -tt

# From Stavros

import errno
import json
import os
import socket
import sys

from pdb import set_trace

from fuse import FUSE, FuseOSError, Operations

from book_shelf_bos import TMShelf
from cmdproto import LibrarianCommandProtocol

def prentry(func):
    # return func
    def new_func(*args, **kwargs):
        # args[0] is usually 'self', so ...
        tmp = ', '.join([str(a) for a in args[1:]])
        print('%s(%s)' % (func.__name__, tmp))
        return func(*args, **kwargs)
    # Be a well-behaved decorator
    new_func.__name__ = func.__name__
    new_func.__doc__ = func.__doc__
    new_func.__dict__.update(func.__dict__)
    return new_func

class LibrarianFSd(Operations):

    # No protocol or DB support yet, just fake it for all of them.

    _basetimes = {
        'st_atime':     1436739200,
        'st_ctime':     1436739200,
        'st_mtime':     1436739200,
    }

    _mode_default_file = int('0100666', 8)  # isfile, 666

    def __init__(self, source, node_id):
        '''Validate parameters'''
        self.torms = source
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

    # start with mount/unmount.  root is usually ('/', ) probably
    # influenced by FuSE builtin option.

    def init(self, root, **kwargs):
        try:
            self.tormsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.tormsock.connect((self.host, self.port))
        except Exception as e:
            set_trace()
            raise FuseOSError(errno.EHOSTUNREACH)
       # FIXME: in C FUSE, data returned here goes into 'getcontext'

    def destroy(self, root):    # ditto
        try:
            self.tormsock.shutdown(socket.SHUT_RDWR)
            self.tormsock.close()
        except socket.error as e:
            if e.errno != errno.ENOTCONN:
                set_trace()
                pass
        except Exception as e:
            set_trace()
            pass
        del self.tormsock

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

    def valid_shelf(self, path): # FIXME: use this eslewhere?
        shelf_name = self.path2shelf(path)
        req = self.lcp('list_shelf', name=shelf_name)
        if not req:
            raise FuseOSError(errno.ENOENT)
        return shelf_name

    # First level:  tenants
    # Second level: tenant group
    # Third level:  shelves

    def librarian(self, cmd, trace=False):
        '''Dictionary in, dictionary out'''
        if trace:
            set_trace()
        try:
            cmd['command']  # idiot check: is it a dict with this keyword
            cmdJS = json.dumps(cmd)
            cmdJSenc = cmdJS.encode()
        except KeyError:
            print('Bad originating command', cmd)
            raise FuseOSError(errno.EBADR)
        except Exception as e:
            set_trace()
            raise FuseOSError(errno.EINVAL)

        try:
            self.tormsock.send(cmdJSenc)
            rspJSenc = self.tormsock.recv(4096)
        except Exception as e:
            raise FuseOSError(errno.EIO)

        try:
            rspJS = rspJSenc.decode()
            rsp = json.loads(rspJS)
        except Exception as e:
            set_trace()
            rsp = { 'error': 'LFSd: %s' % str(e) }

        if rsp and 'error' in rsp:
            print('%s failed: %s' % (cmd['command'], rsp['error']))
            raise FuseOSError(errno.ESTALE)    # Unique in this code
        return rsp  # None is now legal, let the caller interpret it.

    # Higher-level FS operations

    # Called early on nearly all accesses.  Returns os.lstat() equivalent
    # or OSError(errno.ENOENT)
    @prentry
    def getattr(self, path, fh=None):
        if not fh is None:      # dir listings no dice, only open files
            set_trace()
            pass
        shelf_name = self.path2shelf(path, needShelf=False)
        if not shelf_name:   # '/'
            tmp = {
                'st_uid':       42,
                'st_gid':       42,
                'st_mode':      int('0041777', 8),  # isdir, sticky, 777
                'st_nlink':     1,
                'st_size':      4096
            }
            tmp.update(self._basetimes)
            return tmp

        req = self.lcp('list_shelf', name=shelf_name)
        rsp = self.librarian(req)
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
        rsp = self.librarian({'command': 'list_shelves'})
        yield '.'
        for shelf in rsp:
           yield shelf['name']

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

    @prentry
    def getxattr(self, path, attr, position=0):
        """Called with a specific namespace.name attr.  Can return either
           a bytes array OR an int."""
        if position:
            set_trace()
        shelf_name = self.valid_shelf(path)
        rsp = self.librarian({
                'command': 'get_xattr',
                'name': shelf_name,
                'xattr': attr
        })
        if rsp is None:
            raise FuseOSError(errno.ENODATA)    # syn for ENOATTR
        value = rsp['value']
        return value if isinstance(value, int) else bytes(value.encode())

    @prentry
    def listxattr(self, path, *args, **kwargs):
        """getfattr(1), which calls listxattr(2).  Return a list of
           NS<dot>NAME, not their values."""
        shelf_name = self.valid_shelf(path)
        try:
            return list(self._shelf2xattrs[shelf_name].keys())
        except KeyError as e:
            return None

    _badjson = tuple(map(str.encode, ('"', "'", '{', '}')))

    @prentry
    def setxattr(self, path, attr, valbytes, options, position=0):
        # options from linux/xattr.h: XATTR_CREATE = 1, XATTR_REPLACE = 2
        if options:
            set_trace() # haven't actually seen it yet
        shelf_name = self.valid_shelf(path)
        for bad in self._badjson:
            if bad in valbytes:
                raise FuseOSError(errno.EDOMAIN)
        try:
            value = int(valbytes)
        except ValueError as e:
            value = valbytes.decode()

        rsp = self.librarian({
                'command': 'set_xattr',
                'name': shelf_name,
                'xattr': attr,
                'value': value
        })
        if rsp is not None: # unexpected
            raise FuseOSError(errno.ENOTTY)

    @prentry
    def removexattr(self, path):
        shelf_name = self.valid_shelf(path)
        try:
            del self._shelf2xattrs[shelf_name][attr]
        except KeyError as e:
            raise FuseOSError(errno.ENODATA)    # syn for ENOATTR

    @prentry
    def chmod(self, path, mode, **kwargs):
        raise FuseOSError(errno.ENOTSUP)

    @prentry
    def chown(self, path, uid, gid):
        raise FuseOSError(errno.ENOTSUP)

    @prentry
    def readlink(self, path):
        raise FuseOSError(errno.ENOTSUP)

    @prentry
    def mknod(self, path, mode, dev):
        raise FuseOSError(errno.ENOTSUP)

    @prentry
    def rmdir(self, path):
        raise FuseOSError(errno.ENOTSUP)

    @prentry
    def mkdir(self, path, mode):
        raise FuseOSError(errno.ENOTSUP)

    @prentry
    def statfs(self, path): # "df" command; example used statVfs
        # path is don't care. Using stuff from bodemo
        return {
            'f_bavail':     100,    # free blocks for unpriv users
            'f_bfree':      100,    # total free DATA blocks
            'f_blocks':     200,    # total DATA blocks
            'f_bsize':      65536,  # optimal transfer block size
            'f_favail':     100,    # free inodes for unpriv users
            'f_ffree':      100,    # total free file inodes
            'f_files':      200,    # total number of inodes
            'f_flag':       63,     # mount flags
            'f_frsize':     0,      # fragment size
            'f_namemax':    255,    # maximum filename length
        }

    @prentry
    def unlink(self, path):
        shelf_name = self.path2shelf(path)
        rsp = self.librarian({
                                'cmd':          'destroy_shelf',
                                'shelf_name':   shelf_name,
                             })
        return 0

    @prentry
    def symlink(self, name, target):
        raise FuseOSError(errno.ENOTSUP)

    @prentry
    def rename(self, old, new):
        raise FuseOSError(errno.ENOTSUP)

    @prentry
    def link(self, target, name):
        raise FuseOSError(errno.ENOTSUP)

    @prentry
    def utimens(self, path, times=None):
        try:
            self._basetimes['st_atime'] = times[0]
            self._basetimes['st_mtime'] = times[1]
            return 0    # os.utime
        except Exception as e:
            pass
        raise FuseOSError(errno.ENOTSUP)

    # File methods
    # ============

    @prentry
    def open(self, path, flags, **kwargs):
        if kwargs:
            set_trace() # looking for filehandles?  See FUSE docs
        shelf_name = self.path2shelf(path)
        req = self.lcp('open_shelf', name=shelf_name)
        rsp = self.librarian(req)
        try:
            return rsp['id']
        except Exception as e:
            raise FuseOSError(errno.ENOENT)

    # from shell: touch | truncate /lfs/nofilebythisname
    # return os.open()
    @prentry
    def create(self, path, mode, fi=None):
        if fi is not None:
            set_trace()
        shelf_name = self.path2shelf(path)
        req = self.lcp('create_shelf', name=shelf_name)
        rsp = self.librarian(req, trace=False)
        if rsp['name'] is None:
            raise FuseOSError(errno.EEXIST)
        return rsp['id']

    @prentry
    def read(self, path, length, offset, fh):
        set_trace()
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    @prentry
    def write(self, path, buf, offset, fh):
        set_trace()
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    @prentry
    # it was opened before this, but where is the fd (aka shelf id)?
    # Example code shows an explicit open by name in here.
    # example returned nothing?
    def truncate(self, path, length, **kwargs):
        if kwargs:
            set_trace()
        shelf_name = self.path2shelf(path)
        req = self.lcp('list_shelf', name=shelf_name)
        listrsp = self.librarian(req)

        req = self.lcp('resize_shelf',
                        name=shelf_name,
                        size_bytes=length,
                        id=listrsp['id'])
        rsp = self.librarian(req)
        if not 'size_bytes' in rsp:
            raise FuseOSError(errno.EINVAL)
        return 0

    @prentry
    def flush(self, path, fh):      # fh == shelfid
        return 0

    @prentry
    def release(self, path, fh):    # fh == shelfid
        shelf_name = self.path2shelf(path)
        req = self.lcp('close_shelf', name=shelf_name, id=fh)
        rsp = self.librarian(req)
        try:
            assert rsp['name'] == shelf_name
        except Exception as e:
            raise FuseOSError(errno.EEXIST)
        return 0    # os.close...

    @prentry
    def fsync(self, path, fdatasync, fh):
        set_trace()
        return self.flush(path, fh)

def main(source, mountpoint, node_id):
    try:
        FUSE(LibrarianFSd(source, node_id),
            mountpoint,
            allow_other=True, foreground=True)
    except Exception as e:
        raise SystemExit('fusermount probably failed, retry in foreground')

if __name__ == '__main__':
    main(*sys.argv[1:])  # source mountpoint node_id
