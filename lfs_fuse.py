#!/usr/bin/python -tt

# From Stavros

import errno
import json
import os
import socket
import sys

from pdb import set_trace

from fuse import FUSE, FuseOSError, Operations

def prentry(func):
    def new_func(*args, **kwargs):
        # args[0] is usually 'self', so ...
        tmp = ', '.join([str(a) for a in args[1:]])
        print '%s(%s)' % (func.__name__, tmp)
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

    def __init__(self, source):
        '''Validate parameters'''
        self.torms = source
        elems = source.split(':')
        assert len(elems) <= 2
        self.host = elems[0]
        try:
            self.port = int(elems[1])
        except Exception, e:
            self.port = 9090

    # Mount and unmount
    # =================
    # root is usually ('/', ) probably influenced by FuSE builtin option.

    def init(self, root):
        try:
            self.tormsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.tormsock.connect((self.host, self.port))
        except Exception, e:
            set_trace()
            raise FuseOSError(errno.EHOSTUNREACH)

    def destroy(self, root):    # ditto
        try:
            self.tormsock.shutdown(socket.SHUT_RDWR)
            self.tormsock.close()
        except socket.error, e:
            if e.errno != errno.ENOTCONN:
                set_trace()
                pass
        except Exception, e:
            set_trace()
            pass
        del self.tormsock

    # Helpers
    # =======

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

    def librarian(self, cmd, trace=False):
        '''Dictionary in, dictionary out'''
        if trace:
            set_trace()
        try:
            cmd['cmd']  # idiot check: is it a dict with this keyword
            cmdJS = json.dumps(cmd)
            cmdJSenc = cmdJS.encode()
        except Exception, e:
            set_trace()
            raise FuseOSError(errno.EINVAL)

        try:
            self.tormsock.send(cmdJSenc)
            rspJSenc = self.tormsock.recv(4096)
        except Exception, e:
            raise FuseOSError(errno.EIO)

        try:
            rspJS = rspJSenc.decode()
            rsp = json.loads(rspJS)
        except Exception, e:
            set_trace()
            raise FuseOSError(errno.EINVAL)

        return rsp

    # Filesystem methods
    # ==================

    # Called early on nearly all accesses
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
                'st_mode':      int('0041777', 8),  # directory, sticky, 777
                'st_nlink':     1,
                'st_size':      4096
            }
            tmp.update(self._basetimes)
            return tmp

        rsp = self.librarian({
                                'cmd':          'listshelf',
                                'shelf_name':   shelf_name,
                             })
        if rsp['shelf_name'] != shelf_name:
            raise FuseOSError(errno.ENOENT)

        tmp = {
                'st_uid':       42,
                'st_gid':       42,
                'st_mode':      int('0100777', 8),  # regular file, 777
                'st_nlink':     1,
                'st_size':      rsp['size_bytes']
              }
        tmp.update(self._basetimes)
        return tmp

    @prentry
    def readdir(self, path, fh):
        if path != '/':
            raise FuseOSError(errno.ENOENT)
        rsp = self.librarian({'cmd': 'listshelfall'})

        dirents = ['.', ]
        for shelf in rsp['shelves']:
            dirents.append(shelf['shelf_name'])
        for r in dirents:
            yield r

    @prentry
    def access(self, path, mode):   # returned nothing, but maybe 0?
        junk = self.getattr(path)
        # noop but need to compare attrs against mode, see access(2)
        return 0    # or a raise

    @prentry
    def chmod(self, path, mode):
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
    def statfs(self, path):
        raise FuseOSError(errno.ENOTSUP)
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    @prentry
    def unlink(self, path):
        shelf_name = self.path2shelf(path)
        rsp = self.librarian({
                                'cmd':          'destroyshelf',
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
        except Exception, e:
            pass
        raise FuseOSError(errno.ENOTSUP)

    # File methods
    # ============

    @prentry
    def open(self, path, flags):
        shelf_name = self.path2shelf(path)
        rsp = self.librarian({
                                'cmd':          'openshelf',
                                'shelf_name':   shelf_name,
                                'res_owner':  'UncleTouchy'
                             })
        try:
            return rsp['shelf_id']
        except KeyError, e:
            raise FuseOSError(errno.ENOENT)

    # from shell: touch | truncate /lfs/nofilebythisname
    # return os.open()
    @prentry
    def create(self, path, mode, fi=None):
        if fi is not None:
            set_trace()
        shelf_name = self.path2shelf(path)
        rsp = self.librarian({
                    'cmd':          'createshelf',
                    'shelf_name':   shelf_name,
                    'shelf_owner':  'UncleTouchy'
                },
                trace=False)
        if rsp['shelf_name'] is None:
            raise FuseOSError(errno.EEXIST)
        return rsp['shelf_id']

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
    def truncate(self, path, length, fh=None):  # example returned nothing?
        if fh is not None:
            set_trace()
        shelf_name = self.path2shelf(path)
        rsp = self.librarian({
                    'cmd':          'resizeshelf',
                    'shelf_name':   shelf_name,
                    'size_bytes':   length
                })
        if not 'size_bytes' in rsp:
            raise FuseOSError(errno.EINVAL)
        return 0

    @prentry
    def flush(self, path, fh):      # fh == shelfid
        return 0

    @prentry
    def release(self, path, fh):    # fh == shelfid
        shelf_name = self.path2shelf(path)
        rsp = self.librarian({
                    'cmd':          'closeshelf',
                    'shelf_name':   shelf_name,
                    'res_owner':    'UncleTouchy'
                })
        if rsp['shelf_name'] != shelf_name:
            raise FuseOSError(errno.EEXIST)
        return 0    # os.close...

    @prentry
    def fsync(self, path, fdatasync, fh):
        set_trace()
        return self.flush(path, fh)

def main(mountpoint, source):
    FUSE(LibrarianFSd(source), mountpoint, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])  # source mountpoint
