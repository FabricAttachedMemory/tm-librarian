#!/usr/bin/python3 -tt

# Support classes for two different types of shadow files, before direct
# kernel support of mmap().  Shadowing must accommodate graceless restarts
# including book_register.py rewrites.

import errno
import os
import tempfile
import mmap

from pdb import set_trace

from fuse import FuseOSError


class shadow_support(object):
    '''Provide private data storage for subclasses.'''

    def __init__(self):
        self._fd2obj = { }

    def __getitem__(self, index):       # Now this object acts like a dict
        return self._fd2obj.get(index, None)

    def __setitem__(self, index, obj):
        self._fd2obj[index] = obj

    def __contains__(self, index):
        return index in self._fd2obj

    def __delitem__(self, index):
        try:
            del self._fd2obj[index]
        except KeyError:
            pass

    def items(self):
        return self._fd2obj.items()

#--------------------------------------------------------------------------


class shadow_directory(shadow_support):
    '''Create a regular file for each shelf as backing store.  These files
       will exist in the file system of the entity running lfs_fuse,
       ie, if you're on a VM, the file storage is in the VM disk image.'''

    def __init__(self, args, lfs_globals):
        super(self.__class__, self).__init__()
        assert os.path.isdir(
            args.shadow_dir), 'No such directory %s' % args.shadow_dir
        self._shadowpath = args.shadow_dir
        try:
            probe = tempfile.TemporaryFile(dir=args.shadow_dir)
            probe.close()
        except OSError as e:
            raise RuntimeError('%s is not writeable' % args.shadow_dir)

    def shadowpath(self, shelf_name):
        return '%s/%s' % (self._shadowpath, shelf_name)

    def unlink(self, shelf_name):
        for k, v in self.items():
            if v[0].name == shelf_name:
                del self._fd2obj[k]
                break
        try:
            return os.unlink(self.shadowpath(shelf_name))
        except OSError as e:
            if e.errno == errno.ENOENT:
                return 0
            raise FuseOSError(e.errno)

    def _create_open_common(self, shelf, flags, mode):
        if mode is None:
            mode = 0o666
        try:
            fd = os.open(self.shadowpath(shelf.name), flags, mode=mode)
        except OSError as e:
            if flags & os.O_CREAT:
                if e.errno != errno.EEXIST:
                    raise FuseOSError(e.errno)
            else:
                raise FuseOSError(e.errno)
        self[fd] = shelf
        return fd

    def open(self, shelf, flags, mode=None):
        return self._create_open_common(shelf, flags, mode)

    def create(self, shelf, mode=None):
        flags = os.O_CREAT | os.O_RDWR | os.O_CLOEXEC
        return self._create_open_common(shelf, flags, mode)

    def read(self, path, length, offset, shadow_offset, fd):
        os.lseek(fd, offset, os.SEEK_SET)
        return os.read(fd, length)

    def write(self, path, buf, offset, shadow_offset, fd):
        os.lseek(fd, offset, os.SEEK_SET)
        return os.write(fd, buf)

    def truncate(self, shelf, length, fd):
        try:
            if fd:
                assert shelf == self[fd], 'Oops'
            os.truncate(
                fd if fd is not None else self.shadowpath(shelf.name),
                length)
            if fd is not None:
                self[fd].size_bytes = length
            return 0
        except OSError as e:
            raise FuseOSError(e.errno)

    def release(self, fd):
        shelf = self[fd]
        del self[fd]
        os.close(fd)    # I don't think this ever raises....
        return shelf

#--------------------------------------------------------------------------


class shadow_file(shadow_support):
    '''Use one (large) shadow file indexed by "normalized" LZA (ie,
       discontiguous holes in LZA are made smooth for the file.  This
       file lives in the file system of the entity running lfs_fuse.'''

    def __init__(self, args, lfs_globals):
        super(self.__class__, self).__init__()

        print("shadow_file: %s" % args.shadow_file)

        (head, tail) = os.path.split(args.shadow_file)

        assert os.path.isdir(head), 'No such directory %s' % head

        try:
            probe = tempfile.TemporaryFile(dir=head)
            probe.close()
        except OSError as e:
            raise RuntimeError('%s is not writeable' % head)

        if os.path.isfile(args.shadow_file):
            print("file exists: %s" % args.shadow_file)
            fd = os.open(args.shadow_file, os.O_RDWR)
        else:
            print("file does not exist: %s" % args.shadow_file)
            fd = os.open(args.shadow_file, os.O_RDWR | os.O_CREAT)
            size = lfs_globals['nvm_bytes_total']
            os.ftruncate(fd, size)

        # Compare node requirements to file size
        statinfo = os.stat(args.shadow_file)
        _mode_rw_file = int('0100600', 8)  # isfile, 600
        assert _mode_rw_file == _mode_rw_file & statinfo.st_mode, \
            '%s is not RW'
        assert statinfo.st_size >= lfs_globals['nvm_bytes_total']

        self._shadow_fd = fd

    def unlink(self, shelf_name):
        return 0

    def open(self, shelf, flags, mode=None):
        self[shelf.open_handle] = shelf
        return shelf.open_handle

    def create(self, shelf, mode):
        self[shelf.open_handle] = shelf
        return shelf.open_handle

    def truncate(self, shelf, length, fd):
        return 0

    def read(self, path, length, offset, shadow_offset, fd):
        os.lseek(self._shadow_fd, shadow_offset, os.SEEK_SET)
        return os.read(self._shadow_fd, length)

    def write(self, path, buf, offset, shadow_offset, fd):
        os.lseek(self._shadow_fd, shadow_offset, os.SEEK_SET)
        return os.write(self._shadow_fd, buf)

    def release(self, fd):
        shelf = self[fd]
        del self[fd]
        return shelf

#--------------------------------------------------------------------------


class shadow_ivshmem(shadow_support):

    def __init__(self, args, lfs_globals):

        super(self.__class__, self).__init__()

        print("ivshmem:", args.shadow_ivshmem)

        assert (os.path.isfile(
            args.shadow_ivshmem)), '%s is not a file' % args.shadow_ivshmem

        # Compare node requirements to file size
        _mode_rw_file = int('0100600', 8)  # isfile, 600
        statinfo = os.stat(args.shadow_ivshmem)

        print("statinfo:", statinfo)

        assert _mode_rw_file == _mode_rw_file & statinfo.st_mode, \
            '%s is not RW'
        assert statinfo.st_size >= lfs_globals['nvm_bytes_total']

        self._shadow_fd = -1

        # os.open vs. built-in allows all the low-level stuff I need.
        self._shadow_fd = os.open(args.shadow_ivshmem, os.O_RDWR)
        self._m = mmap.mmap(
            self._shadow_fd, 0, prot=mmap.PROT_READ | mmap.PROT_WRITE)

    def unlink(self, shelf_name):
        return 0

    def open(self, shelf, flags, mode=None):
        self[shelf.open_handle] = shelf
        return shelf.open_handle

    def create(self, shelf, mode):
        self[shelf.open_handle] = shelf
        return shelf.open_handle

    def truncate(self, shelf, length, fd):
        return 0

    def read(self, path, length, offset, shadow_offset, fd):
        self._m.seek(shadow_offset, 0)
        return self._m.read(length)

    def write(self, path, buf, offset, shadow_offset, fd):
        self._m.seek(shadow_offset, 0)
        # write to mmap file always returns "None"
        self._m.write(buf)
        return len(buf)

    def release(self, fd):
        shelf = self[fd]
        del self[fd]
        return shelf

#--------------------------------------------------------------------------


def the_shadow_knows(args, lfs_globals):
    '''args is command-line arguments from lfs_fuse.py'''
    try:
        if args.shadow_dir:
            return shadow_directory(args, lfs_globals)
        elif args.shadow_file:
            return shadow_file(args, lfs_globals)
        elif args.shadow_ivshmem:
            return shadow_ivshmem(args, lfs_globals)
        else:
            raise ValueError('Illegal shadow setting "%s"' % args.shadow_dir)
    except Exception as e:
        msg = str(e)
    # seems to be ignored, as is SystemExit
    raise OSError(errno.EINVAL, 'lfs_shadow: %s' % msg)
