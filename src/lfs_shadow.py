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

# Support classes for two different types of shadow files, before direct
# kernel support of mmap().  Shadowing must accommodate graceless restarts
# including book_register.py rewrites.

import errno
import math
import os
import stat
import struct
import sys
import tempfile
import mmap
import ctypes

from copy import deepcopy
from subprocess import getoutput
from pdb import set_trace

from frdnode import BooksIGInterpretation as BII
from tm_fuse import TmfsOSError, tmfs_get_context
import tm_ioctl_opt as IOCTL

#--------------------------------------------------------------------------
# _shelfcache is essentially a copy of the Librarian's "opened_shelves"
# table data generated on the fly.  The goal was to avoid a round trip
# to the Librarian for many FuSE interactions.  This class duck-types a
# dict with multiple keys (name and file handles).  The value is a single,
# modified TMShelf object that holds all open-related data.  The data
# will assist PTE management for processes holding the shelf open.
# Account for multiple opens by same PID as well as different PIDs.
# Multinode support will require OOB support and a few more Librarian calls.

# EDIT: the dictionary is being modified to index by shelf id instead of name
# so that same name shelves in different directories can still be acted upon

# shadow file and shadow directory yet to be implemented for subs


class shadow_support(object):
    '''Provide private data storage for subclasses.'''

    _S_IFREG_URW = stat.S_IFREG + 0o600  # regular file

    # MUST agree with tmfs::lfs.c ADDRESS_MODES
    _MODE_NONE = 0          # Something will eventually throw an error
    _MODE_FAME = 1          # FAME direct flat area only, no zbridge calls
    _MODE_FAME_DESC = 2     # FAME direct, but talk to zbridge (no real HW)
    _MODE_1906_DESC = 3     # TMAS: Zbridge directly programs DESBK, no chat
    _MODE_FULL_DESC = 4     # TM(AS): full operation
    _MODE_FALLBACK = 5      # Use existing mmap() operations, not lfs stuff

    def __init__(self, args, lfs_globals):
        self.verbose = args.verbose
        self.logger = args.logger
        self.book_size = lfs_globals['book_size_bytes']
        self.book_shift = int(math.log(self.book_size, 2))
        self._shelfcache = { }
        self.zero_on_unlink = True

        # The field is called aperture_base even if direct mapping is used.
        # Along with book size, tmfs kernel module always asks for these.
        # See getxattr below.

        self.aperture_base = args.aperture_base
        self.aperture_size = args.aperture_size
        self.BIImode = args.BIImode
        self.addr_mode = getattr(args, 'addr_mode', self._MODE_NONE)
        self._igstart = {}

        if self.addr_mode not in (
                self._MODE_FALLBACK, self._MODE_FAME, self._MODE_FAME_DESC):
            return

        # Backing store (shadow_file or FAME direct) is contiguous but IG
        # ranges are not.  Map IG ranges onto areas of backing store.
        # First get numerically sorted keys.  Again, humans like sorting.
        # Index 0 is the books per IG, and index 1 is the starting physaddr
        # if appropriate.

        books_per_IG = lfs_globals['books_per_IG']
        igkeys = sorted([int(igstr) for igstr in books_per_IG.keys()])

        # IG came over as strings...amend that.
        for key in igkeys:
            books_per_IG[key] = books_per_IG[str(key)]

        # This and other changes might necessitate another round of subclassing
        if self.BIImode == BII.MODE_LZA:    # And I'm already FAMEish
            offset = 0
            for ig in igkeys:
                self.logger.info('0x%016x (%d) IG %d relative offset' % (
                        offset, offset, ig))
                # insure running Librarian DB fits in available space
                assert offset < args.aperture_size, \
                    'Absolute address for IG %d out of range' % ig
                self._igstart[ig] = offset
                books = books_per_IG[ig][0]
                offset += books * self.book_size
            return

        assert self.BIImode == BII.MODE_PHYSADDR, 'Not today, bobo!'
        for ig in igkeys:
            physaddr = books_per_IG[ig][1]
            self.logger.info('0x%016x (%d) IG %d raw physaddr' % (
                        physaddr, physaddr, ig))
            self._igstart[ig] = physaddr

    def _consistent(self, cached):
        try:
            all_fh = [ ]
            for vlist in cached.open_handle.values():
                all_fh += vlist
            for v_fh in all_fh:
                assert (None, v_fh) in self._shelfcache, 'Inconsistent list members'
        except Exception as e:
            self.logger.error('Shadow cache is corrupt: %s' % str(e))
            if self.verbose > 3:
                set_trace()
            raise TmfsOSError(errno.EBADFD)

    # Most uses send an open handle fh (integer) as key.  truncate by name
    # is the exception.  An update needs to be reflected for all keys.

    def __setitem__(self, key, shelf):
        '''Part of the support for duck-typing a dict with multiple keys.'''
        # This object only has support for either a shelf.id or a fh
        # The tuple indicates to shadow which it is, either (shelf.id, None)
        # or (None, fh). (shelf.id, fh) is not supported. Stop it here
        # rather than deal with it later
        assert key[0] is None or key[1] is None, 'Shadow calls must have one None field'
        fh = shelf.open_handle
        assert fh is not None or key[0] == shelf.id, 'Might take more thought on this'
        # DO NOT change following lookup from (shelf.id, None) to key, although it
        # mirrors one possible key. If it exists at all it has to be found here,
        # if not breaks upon multiple opens
        cached = self[(shelf.id, None)]
        pid = tmfs_get_context()[2]

        # Is it a completely new addtion?  Remember, only cache open shelves.
        if cached is None:
            if fh is None:
                return
            # Create a copy because "open_handle" will be redefined.  This
            # single copy will be retrievable by the shelf name and all of
            # its open handles.  The copy itself has a list of the fh keys
            # indexed by pid, so open_handle.keys() is all the pids.
            cached = deepcopy(shelf)
            self._shelfcache[(cached.id, None)] = cached
            self._shelfcache[key] = cached
            cached.open_handle = { }
            # was key, hopefully key[1] still holds
            cached.open_handle[pid] = [ key[1], ]
            return

        self._consistent(cached)    # As long as I'm here...

        # Has the shelf changed somehow?  If so, replace the cached copy
        # and perhaps take other steps.  Break down the comparisons in
        # book_shelf_bos.py::TMShelf::__eq__()
        if cached != shelf:
            invalidate = True   # and work to make it false
            assert cached.id == shelf.id, 'Shelf aliasing error?'  # TSNH :-)

            # If it grew OR remained the same size, are the first "n" books
            # still the same?  Order matters.
            if shelf.size_bytes >= cached.size_bytes:
                for i, book in enumerate(cached.bos):
                    if book != shelf.bos[i]:
                        break
                else:
                    invalidate = False  # the first "n" books match

            # Update cached object variant fields.  Beware references.
            cached.size_bytes = shelf.size_bytes
            cached.bos = deepcopy(shelf.bos)
            cached.book_count = shelf.book_count
            cached.mtime = shelf.mtime

            # Originally intended to support book caching in user space,
            # this logic may still find use...
            if invalidate and self.verbose > 2:
                # print('\n\tNEED TO INVALIDATE PTES!!!\n')
                pass

        # fh is unique (created by Librarian as table index).  Does it
        # need to be appended?
        if not isinstance(fh, int) or (None, fh) in self._shelfcache:
            return
        self._shelfcache[key] = cached
        try:
            cached.open_handle[pid].append(key[1])  # again, now key[1]
        except KeyError as e:
            cached.open_handle[pid] = [ key[1], ]  # new pid, same as ^^

    def __getitem__(self, key):
        '''Part of the support for duck-typing a dict with multiple keys.
           Suppress KeyError, returning None if no value exists.'''
        return self._shelfcache.get(key, None)

    def __contains__(self, key):
        '''Part of the support for duck-typing a dict with multiple keys.'''
        return key in self._shelfcache

    def __delitem__(self, key):
        '''Part of the support for duck-typing a dict with multiple keys.'''
        cached = self[key]
        if cached is None:
            # Not currently open, something like "rm somefile"
            if key[1] is None:
                return
            raise AssertionError('Deleting a missing fh?')

        self._consistent(cached)    # As long as I'm here...

        # and now delete
        del self._shelfcache[key]  # always
        if key[1] is not None:
            # Remove this direct shelf reference plus the back link
            open_handles = cached.open_handle
            for pid, fhlist in open_handles.items():
                if key[1] in fhlist:
                    fhlist.remove(key[1])
                    if not fhlist:
                        del open_handles[pid]
                    if not open_handles:            # Last reference
                        del self._shelfcache[(cached.id, None)]
                    return
            # There has to be one
            raise AssertionError('Cannot find fh to delete')

        # It's a string so remove the whole thing.  This is only called
        # from unlink; so VFS has done the filtering job on open handles.
        if cached.open_handle is None:  # probably "unlink"ing
            return
        all_fh = [ ]
        open_handles = cached.open_handle
        if open_handles is not None:
            for vlist in cached.open_handle.values():
                all_fh += vlist
            all_fh = frozenset(all_fh)  # paranoid: remove dupes
            for fh in all_fh:
                del self._shelfcache[(None, fh)]

    def keys(self):
        '''Part of the support for duck-typing a dict with multiple keys.'''
        return self._shelfcache.keys()

    def items(self):
        '''Part of the support for duck-typing a dict with multiple keys.'''
        return self._shelfcache.items()

    def values(self):
        '''Part of the support for duck-typing a dict with multiple keys.'''
        return self._shelfcache.values()

    # End of dictionary duck typing, now use that cache

    def shadow_offset(self, shelf, shelf_offset):
        '''Translate shelf-relative offset to flat shadow (file) offset'''
        bos = self[(shelf.id, None)].bos
        bos_index = shelf_offset // self.book_size  # (0..n)

        # Stop FS read ahead past shelf, but what about writes?  Later.
        try:
            book = bos[bos_index]
        except Exception as e:
            return -1

        # Offset into flat space has several contributors.  The concatenated
        # LZA field has already been broken down into constituent parts.
        # Note: in BII.MODE_LZA there are absolute values to work with in
        # book['id'].  Turns out the IG-relative math works just fine.
        intlv_group = book['intlv_group'] & BII.VALUE_MASK
        book_num = book['book_num']                     # Relative to IG
        ig_base = self._igstart[intlv_group]            # absolute
        book_start = book_num * self.book_size          # relative to ig_base
        book_offset = shelf_offset % self.book_size     # relative to book
        tmp = ig_base + book_start + book_offset
        if self.aperture_size:  # Based on BIImode
            assert tmp < self.aperture_size, 'BAD SHADOW OFFSET'
        return tmp

    # Provide ABC noop defaults.  Note they're not all actually noop.
    # Top men are insuring this works with multiple opens of a shelf.

    def truncate(self, shelf, length, fh):
        if fh is not None:
            # This is an update, but there's no good way to flag that to
            # __setitem__.  Do an idiot check here.
            assert (None, fh) in self, 'VFS thinks %s is open but LFS does not, shelf_id: %s' % (
                shelf.name, shelf.id)
        self[(shelf.id, None)] = shelf
        return 0

    def unlink(self, shelf):
        try:
            del self[(shelf.id, None)]
        except Exception as e:
            set_trace()
            raise
        return 0

    # "man fuse" regarding "hard_remove": an "rm" of a file with active
    # opens tries to rename it.
    def rename(self, shelf, oldname, newname):
        cached = self[(shelf.id, None)]
        if cached is not None:
            # Retrieve shared object, fix it, and rebind to new name.
            assert cached.name == oldname, 'Mismatch - shelf id = %d does not have name %s' % (
                shelf.id, oldname)
            cached.name = newname
            self[(shelf.id, None)] = cached
        else:
            if newname.startswith('.tmfs_hidden'):
                # VFS thinks it's there so I should too
                raise TmfsOSError(errno.ESTALE)
        return 0

    # Idiot checking and caching: shadow_support.  These routines should
    # be called LAST, after any localized ops on the shelf, like _fd.
    def open(self, shelf, flags, mode=None):
        assert isinstance(shelf.open_handle, int), 'Bad handle in open()'
        self[(None, shelf.open_handle)] = shelf
        return shelf.open_handle

    # Idiot checking and caching: shadow_support
    def create(self, shelf, mode=None):
        assert isinstance(shelf.open_handle, int), 'Bad handle in create()'
        assert self[(shelf.id, None)] is None and self[(
            None, shelf.open_handle)] is None, 'Cache inconsistency'
        self[(None, shelf.open_handle)] = shelf     # should be first one
        return shelf.open_handle

    # Idiot checking and UNcaching: shadow_support
    def release(self, fh):                  # Must be an fh, not an fd
        cached = self[(None, fh)]
        retval = deepcopy(cached)
        retval.open_handle = fh             # could be larger list
        del self[(None, fh)]
        return retval

    # Support for getxattr, it comes soon
    def _obtain_shadow_igstart(self):
        '''Return all 128 IG start addresses in the flat shadow space.
           Zero-pad as required.'''
        response = bytearray()
        nextIndex = 0
        for groupId in sorted(self._igstart.keys()):
            for i in range(groupId - nextIndex):    # non-contiguous IGs
                response.extend(struct.pack('Q', 0))
            # This works for BII.MODE_PHYSADDR cuz aperture_base == 0
            physaddr = self._igstart[groupId] + self.aperture_base
            tmp = struct.pack('Q', physaddr)
            response.extend(tmp)
            nextIndex = groupId + 1
        while nextIndex < 128:  # zero pad
            response.extend(struct.pack('Q', 0))
            nextIndex += 1
        assert len(response) == 128 * 8, 'Missed it by that much'
        return response

    # Support for getxattr, it comes next
    def _map_populate(self, shelf, start_book, buflen):
        '''Get LZAs from BOS, limit is number of 64-bit unsigned
           longs that will fit into buflen'''
        # BII.MODE_PHYSADDR mode LZA contains starting physical book
        # address in bits [63:20] so the entire 64 bit address is passed.
        response = bytearray()
        bos = self[(shelf.id, None)].bos
        offset = 0
        while buflen > offset + 7 and start_book < len(bos):
            # FIXME: 33 should be "apertures._BOOK_SHIFT" but that's a
            # circular import.  Is all the data in bos[] elements now after
            # the 2017-10 'list_shelf_book' changes for 990x?
            tmp = struct.pack('Q', bos[start_book]['id'])
            response.extend(tmp)
            offset += 8
            start_book += 1
        return response

    # Piggybacked for kernel to ask for stuff.  Even in --shadow_[dir|file]
    # it wants globals, handle that here.  During mmap fault handling it
    # wants more.  In shadow modes return 'FALLBACK' to get legacy, generic
    # cache-based handler with stock read() and write() spill and fill.
    # Overridden in class apertures to do true mmaps.
    def getxattr(self, shelf, xattr):
        try:
            if xattr == '_obtain_booksize_addrmode_aperbase_biimode':
                data = '%s,%d,%s,%d' % (
                    self.book_size, self.addr_mode, self.aperture_base, self.BIImode)
                return data

            if xattr == '_obtain_shadow_igstart':
                return self._obtain_shadow_igstart()

            if xattr.startswith('_obtain_lza_for_map_populate'):
                _, start_book, buflen = xattr.split(',')
                start_book = int(start_book)
                buflen = int(buflen)
                return self._map_populate(shelf, start_book, buflen)

            return 'FALLBACK'   # might be circumvented by subclass
        except Exception as e:
            self.logger.error('!!! ERROR IN GENERIC KERNEL XATTR HANDLER (%d): %s' % (
                sys.exc_info()[2].tb_lineno, str(e)))
            return 'ERROR'

    def read(self, shelf, length, offset, fd):
        raise TmfsOSError(errno.ENOSYS)

    def write(self, shelf, buf, offset, fd):
        raise TmfsOSError(errno.ENOSYS)

    def ioctl(self, shelf, cmd, arg, fh, flags, data):
        return -1

#--------------------------------------------------------------------------


class shadow_directory(shadow_support):
    '''Create a regular file for each shelf as backing store.  These files
       will exist in the file system of the entity running lfs_fuse,
       ie, if you're on a VM, the file storage is in the VM disk image.'''

    def __init__(self, args, lfs_globals):
        args.addr_mode = self._MODE_FALLBACK    # FIXME: not tested
        super().__init__(args, lfs_globals)
        self.zero_on_unlink = False   # OS "clears" per-shelf backing file
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
        # FIXME: not tested since unlink was expanded to do zeroing
        for k, v in self.items():
            if v[0].name == shelf_name:
                del self[k]
                break
        try:
            return os.unlink(self.shadowpath(shelf_name))
        except OSError as e:
            if e.errno == errno.ENOENT:
                return 0
            raise TmfsOSError(e.errno)

    def _create_open_common(self, shelf, flags, mode):
        if mode is None:
            mode = 0o666
        try:
            shelf._fd = os.open(self.shadowpath(shelf.name), flags, mode=mode)
        except OSError as e:
            if flags & os.O_CREAT:
                if e.errno != errno.EEXIST:
                    raise TmfsOSError(e.errno)
            else:
                raise TmfsOSError(e.errno)

    def open(self, shelf, flags, mode=None):
        self._create_open_common(shelf, flags, mode)
        super().open(shelf, flags, mode)    # caching
        return shelf._fd    # so kernel sees a real fd for mmap under FALLBACK

    def create(self, shelf, mode=None):
        flags = os.O_CREAT | os.O_RDWR | os.O_CLOEXEC
        self._create_open_common(shelf, flags, mode)
        super().create(shelf, mode)  # caching
        return shelf._fd    # so kernel sees a real fd for mmap under FALLBACK

    def read(self, shelf_name, length, offset, fd):
        assert self[shelf_name]._fd == fd, 'fd mismatch on read'
        os.lseek(fd, offset, os.SEEK_SET)
        return os.read(fd, length)

    def write(self, shelf_name, buf, offset, fd):
        assert self[shelf_name]._fd == fd, 'fd mismatch on write'
        os.lseek(fd, offset, os.SEEK_SET)
        return os.write(fd, buf)

    def truncate(self, shelf, length, fd):  # shadow_dir, yes an fd
        try:
            if fd:  # It's an open shelf
                assert self[shelf.name]._fd == fd, 'fd mismatch on truncate'
            os.truncate(
                shelf._fd if shelf._fd >= 0 else self.shadowpath(shelf.name),
                length)
            shelf.size_bytes = length
            if shelf.open_handle is None:
                shelf = self[shelf.id]
                if shelf is not None:
                    shelf.size_bytes = length
            return 0
        except OSError as e:
            raise TmfsOSError(e.errno)

    def release(self, fd):              # shadow_dir: yes this is an fd
        os.close(fd)                    # never a raise()
        for shelf in self.values():     # search for fd to uncache shelf
            if shelf._fd == fd:
                for v in shelf.open_handle.values():
                    fh = v[0]
                    break
                break
        else:
            raise RuntimeError('release: fd=%s is not cached' % fd)
        super().release(fh)
        shelf.open_handle = fh
        shelf._fd = -1
        return shelf

#--------------------------------------------------------------------------


class shadow_file(shadow_support):
    '''Use one (large) shadow file indexed by "normalized" LZA (ie,
       discontiguous holes in LZA are made smooth for the file.  This
       file lives in the file system of the entity running lfs_fuse.'''

    def __init__(self, args, lfs_globals):
        super().__init__(args, lfs_globals)
        (head, tail) = os.path.split(args.shadow_file)
        assert os.path.isdir(head), 'No such directory %s' % head

        try:
            probe = tempfile.TemporaryFile(dir=head)
            probe.close()
        except OSError as e:
            raise RuntimeError('%s is not writeable' % head)

        if os.path.isfile(args.shadow_file):
            fd = os.open(args.shadow_file, os.O_RDWR)
        else:
            fd = os.open(args.shadow_file, os.O_RDWR | os.O_CREAT)
            size = lfs_globals['nvm_bytes_total']
            os.ftruncate(fd, size)
        self._shadow_fd = fd

        # Compare node requirements to actual file size
        statinfo = os.stat(args.shadow_file)
        assert self._S_IFREG_URW == self._S_IFREG_URW & statinfo.st_mode, \
            '%s is not RW'
        assert statinfo.st_size >= lfs_globals['nvm_bytes_total']
        args.aperture_base = 0
        args.aperture_size = statinfo.st_size
        args.addr_mode = self._MODE_FALLBACK
        super().__init__(args, lfs_globals)

    # open(), create(), release() only do caching as handled by superclass

    def read(self, shelf_name, length, offset, fd):

        if ((offset % self.book_size) + length) <= self.book_size:
            shadow_offset = self.shadow_offset(shelf_name, offset)
            os.lseek(self._shadow_fd, shadow_offset, os.SEEK_SET)
            return os.read(self._shadow_fd, length)

        # Read overlaps books, split into multiple chunks

        buf = b''
        cur_offset = offset
        tot_length = length

        while (tot_length > 0):
            cur_length = min((self.book_size - (cur_offset % self.book_size)),
                             tot_length)
            shadow_offset = self.shadow_offset(shelf_name, cur_offset)

            if shadow_offset == -1:
                break

            os.lseek(self._shadow_fd, shadow_offset, os.SEEK_SET)
            buf += os.read(self._shadow_fd, cur_length)

            self.logger.debug(
                "READ: co = %d, tl = %d, cl = %d, so = %d, bl = %d" % (
                  cur_offset, tot_length, cur_length,
                  shadow_offset, len(buf)))

            offset += cur_length
            cur_offset += cur_length
            tot_length -= cur_length

        return buf

    def write(self, shelf_name, buf, offset, fd):

        if ((offset % self.book_size) + len(buf)) <= self.book_size:
            shadow_offset = self.shadow_offset(shelf_name, offset)
            os.lseek(self._shadow_fd, shadow_offset, os.SEEK_SET)
            return os.write(self._shadow_fd, buf)

        # Write overlaps books, split into multiple chunks

        tbuf = b''
        buf_offset = 0
        cur_offset = offset
        tot_length = len(buf)
        wsize = 0

        while (tot_length > 0):
            cur_length = min((self.book_size - (cur_offset % self.book_size)),
                             tot_length)
            shadow_offset = self.shadow_offset(shelf_name, cur_offset)

            assert shadow_offset != -1, "shadow_offset -1 during write"

            # chop buffer in pieces
            buf_end = buf_offset + cur_length
            tbuf = buf[buf_offset:buf_end]

            os.lseek(self._shadow_fd, shadow_offset, os.SEEK_SET)
            wsize += os.write(self._shadow_fd, tbuf)

            self.logger.debug(
                "WRITE: co = %d, tl = %d, cl = %d, so = %d,"
                " bl = %d, bo = %d, wsize = %d, be = %d" % (
                  cur_offset, tot_length, cur_length, shadow_offset,
                  len(tbuf), buf_offset, wsize, buf_end))

            offset += cur_length
            cur_offset += cur_length
            tot_length -= cur_length
            buf_offset += cur_length
            tbuf = b''

        return wsize

#--------------------------------------------------------------------------
# Original IVSHMEM class did read/write against the resource file by mmapping
# slices # against /sys/bus/pci/..../resource2.  mmap in kernel was handled
# by legacy generic fault handles that used read/write.  There was a lot of
# overhead but it was functional against true global shared memory.

# 'class fam' supported true mmap in the kernel against the physical
# address of the IVSHMEM device, but it couldn't do read and write.  Merging
# the two classes (actually, just getxattr() from the previous "class fam")
# gets the best of both worlds (IVHSMEM and HW FAM) in one class.


class apertures(shadow_support):

    _NDESCRIPTORS = 1906            # Non-secure starting at the BAR...
    _NVM_BK = 0x01600000000         # Thus speaketh the chipset ERS

    _IG_SHIFT = 46                  # Bits of offset for 7 bit IG
    _IG_MASK = ((1 << 7) - 1)       # Mask for 7 bit IG
    _BOOK_SHIFT = 33                # Bits of offset for 20 bit book number
    _BOOK_MASK = ((1 << 13) - 1)    # Mask for 13 bit book number
    _BOOKLET_SHIFT = 16             # Bits of offset for 17 bit booklet number
    _BOOKLET_MASK = ((1 << 17) - 1)  # Mask for 17 bit booklet number

    def __init__(self, args, lfs_globals):
        '''args needs to have valid attributes for IVSHMEM and descriptors.'''

        super().__init__(args, lfs_globals)

        for ig, offset in self._igstart.items():
            self.logger.info('0x%016x is absolute start for IG %d' % (
                self.aperture_base + offset, ig))

    # open(), create(), release() only do caching as handled by superclass

    def getxattr(self, shelf, xattr):
        # Called from kernel (fault, RW, atomics), don't die here :-)
        try:
            data = super().getxattr(shelf, xattr)
            if data != 'FALLBACK':  # superclass handles some things
                return data

            assert xattr.startswith('_obtain_lza_for_page_fault'), \
                'BAD KERNEL XATTR %s' % xattr

            bos = self[(shelf.id, None)].bos
            cmd, comm, pid, PABO = xattr.split(',')
            pid = int(pid)
            PABO = int(PABO)  # page-aligned byte offset into shelf
            shelf_book_num = PABO // self.book_size  # (0..n-1)
            if shelf_book_num >= len(bos):
                return 'ERROR'

            # tmfs is always looking for an LZA.  Calculate if necessary.
            # map_addr calculations should be rolled up across two spots.
            baseBOS = bos[shelf_book_num]
            if self.BIImode in (BII.MODE_LZA, BII.MODE_PHYSADDR):
                bookID = baseBOS['id']
            else:
                return 'ERROR'

            # Remember, this IS in the kernel :-)
            reason = cmd.split('_for_')[1]
            self.logger.debug(
                'Get LZA (%s): process %s[%d] shelf_id=%s, PABO=%d (0x%x)' %
                (reason, comm, pid, shelf.name, PABO, PABO))
            self.logger.debug(
                'shelf book seq=%d, LZA=0x%x -> IG=%d, IGoffset=%d' % (
                    shelf_book_num,
                    bookID,
                    ((bookID >> self._IG_SHIFT) & self._IG_MASK),
                    ((bookID >> self._BOOK_SHIFT) & self._BOOK_MASK)))

            # FAME modes need the "flattened IG" address into the memory area.
            # shadow_offset() returns a full byte-accurate address (for use
            # in shadow_file) which is "too much info" for the kernel.  It
            # will do the right page-masking and essentially ignore those
            # last bits of "accuracy".  DON'T DO THE OFFSET ADDITION TWICE!

            if self.addr_mode in (self._MODE_FAME, self._MODE_FAME_DESC):
                if self.BIImode == BII.MODE_PHYSADDR:
                    map_addr = bookID
                else:
                    phys_offset = self.shadow_offset(shelf, PABO)
                    if phys_offset == -1:
                        return 'ERROR'
                    map_addr = self.aperture_base + phys_offset
                    # tmp = baseBOS['id'] + PABO % self.book_size # yes it agrees
            elif self.addr_mode == self._MODE_1906_DESC:
                # Should match kernel calculations.  # books <= # descriptors
                # and DESBK is preprogrammed so LZA -> aperture number.
                aper_num = ((bookID >> self._BOOK_SHIFT) & self._BOOK_MASK)
                desc_offset = aper_num * self.book_size
                book_offset = PABO % self.book_size
                map_addr = self.aperture_base + desc_offset + book_offset
            elif self.addr_mode == self._MODE_FULL_DESC:
                # MUST be done in kernel because zbridge slot lookup is needed
                map_addr = 0
            else:
                raise RuntimeError('Unimplemented mode %d' % self.addr_mode)

            data = '%d,%s,%s' % (self.addr_mode, bookID, map_addr)
            self.logger.debug('data returned to fault handler = %s' % (data))
            return data

        except Exception as e:
            self.logger.error('!!! ERROR IN LZA LOOKUP HANDLER (%d): %s' % (
                sys.exc_info()[2].tb_lineno, str(e)))
            return 'ERROR'

    def ioctl(self, shelf_name, cmd, arg, fh, flags, data):

        LFS_GET_PHYS_FROM_OFFSET = IOCTL.IOWR(ord("L"), 0x01, ctypes.c_ulong)

        if (cmd == LFS_GET_PHYS_FROM_OFFSET):

            # data ---
            #   in : byte offset into shelf
            #   out: physical address for book at offset

            # read data sent in by user
            inbuf = ctypes.create_string_buffer(8)
            ctypes.memmove(inbuf, data, 8)
            offset = (int.from_bytes(inbuf, byteorder='little'))

            lfs_offset = self.shadow_offset(shelf_name, offset)
            if lfs_offset == -1:
                return -1
            physaddr = self.aperture_base + lfs_offset

            # send data back to user
            outbuf = physaddr.to_bytes(8, byteorder='little')
            ctypes.memmove(data, outbuf, 8)

            self.logger.info(
                "LFS_GET_PHYS_FROM_OFFSET: shelf_name = %s" % shelf_name)
            self.logger.info("offset = %d (0x%x), physaddr = %d (0x%x)" %
                             (offset, offset, physaddr, physaddr))

            return 0
        else:
            return -1

#--------------------------------------------------------------------------
# Called before the class is initialized.

def _detect_memory_space(args, lfs_globals):
    '''Not compatible with, and will ignore, other shadow_xxxx options.'''
    # Discern ivshmem information.  Our convention states the first
    # IVSHMEM device found is used as fabric-attached memory.  Parse the
    # first block of lines of lspci -vv for Bus-Device-Function and
    # BAR2 information.

    args.BIImode = lfs_globals['BIImode']
    if args.BIImode == BII.MODE_PHYSADDR:
        # Make the kernel believe it's FAME.  It will eventually ask for
        # physaddrs.  However, there is no aperture base/size so don't set
        # them.  Yes it will bomb until everything is keyed by BIImode.
        args.addr_mode = shadow_support._MODE_FAME
        args.aperture_base = 0
        args.aperture_size = 0
        args.logger.debug('addr_mode = MODE_FAME with per-IG physaddr (990x)')
        return

    lspci = getoutput('lspci -vv -d1af4:1110').split('\n')[:11]
    line1 = lspci[0]    # Might say "lspci: command not found"
    if 'lspci' in line1:
        raise RuntimeError('lspci command must be installed')

    ivshmemOK = False
    if 'Red Hat, Inc' in line1:
        RHstanza = 'Inter-VM shared memory'
        # QEMU      Tested  line1
        # <= 2.4:   x86_64  endswith(RHstanza)
        # == 2.5:   aarch64 contains RHstanza, endswith(" (Rev 01)")
        # == 2.6:   aarch64 contains RHstanza, endswith(" (rev 01)") YES lower r
        machine = os.uname().machine
        ivshmemOK = machine == 'x86_64' or (
                    machine == 'aarch64' and
                    line1.lower().endswith(' (rev 01)'))
        ivshmemOK = ivshmemOK and RHstanza in line1

    # If not FAME/IVSHMEM, ass-u-me it's TMAS or real TM.  Hardcode the
    # direct descriptor mode for now, Zbridge preloads all 1906.

    if not ivshmemOK:
        args.logger.warning(
            'No match with IVSHEM PCI devices, assuming TM(AS)')
        if args.fixed1906:
            args.addr_mode = shadow_support._MODE_1906_DESC
            args.logger.warning(
                'addr_mode = MODE_1906_DESC (requires DESC autoprogramming)')
        else:
            args.addr_mode = shadow_support._MODE_FULL_DESC
            args.logger.warning(
                'addr_mode = MODE_FULL_DESC (with zbridge/flushtm interaction)')
        args.aperture_base = apertures._NVM_BK
        args.aperture_size = apertures._NDESCRIPTORS * \
            lfs_globals['book_size_bytes']
        return

    # Should be FAME, start parsing lspci output.
    elems = line1.split()
    bdf = elems[0]

    region2 = [ l for l in lspci[1:] if 'Region 2:' in l ][0]
    assert ('(64-bit, prefetchable)' in region2), \
        'IVSHMEM region 2 not found for device %s' % bdf
    assert ('ignored' not in region2), \
        'IVSHMEM region 2 has no memory %s' % bdf
    args.aperture_base = int(region2.split('Memory at')[1].split()[0], 16)
    assert args.aperture_base, \
        'Could not retrieve region 2 address of IVSHMEM device at %s' % bdf

    # At 2.6 resource2 file went away, just detect size from line " [size=64G]"
    size = region2.split('size=')[1][:-1]   # kill the right bracket
    assert size[-1] in 'MGT', \
        'Region 2 size not "G" or "T" for IVSHMEM device at %s' % bdf
    if size[-1] == 'M':
        args.aperture_size = int(size[:-1]) << 20
    elif size[-1] == 'G':
        args.aperture_size = int(size[:-1]) << 30
    else:
        args.aperture_size = int(size[:-1]) << 40

    assert args.aperture_size, \
        'Could not retrieve region 2 size of IVSHMEM device at %s' % bdf
    assert args.aperture_size >= lfs_globals['nvm_bytes_total'], \
        'IVSHMEM size < Librarian total NVM (%d < %d)' % \
        (args.aperture_size, lfs_globals['nvm_bytes_total'])

    args.logger.warning('IVSHMEM device at %s used as FAM' % bdf)
    args.logger.info('0x%016x FAM base address' % args.aperture_base)
    args.logger.info('0x%016x FAM max  address (%d bytes)' % (
        args.aperture_base + args.aperture_size - 1,
        args.aperture_size))

    if args.enable_Z:
        args.addr_mode = shadow_support._MODE_FAME_DESC
        args.logger.debug(
            'addr_mode = MODE_FAME_DESC (with zbridge/flushtm interaction)')
    else:
        args.addr_mode = shadow_support._MODE_FAME
        args.logger.debug(
            'addr_mode = MODE_FAME (without zbridge/flushtm interaction)')

    args.logger.debug(
        'IVSHMEM max offset is 0x%x; physical addresses 0x%x - 0x%x' % (
            args.aperture_size - 1,
            args.aperture_base, args.aperture_base + args.aperture_size - 1))

#--------------------------------------------------------------------------


def the_shadow_knows(args, lfs_globals):
    '''This is a factory.  args is command-line arguments from
       lfs_fuse.py and lfs_globals was received from the librarian.'''

    try:
        if args.shadow_dir:
            return shadow_directory(args, lfs_globals)
        elif args.shadow_file:
            return shadow_file(args, lfs_globals)

        if args.fixed1906:  # Idiot check the topology
            assert args.physloc.rack == 1, \
                'Bad rack for --fixed1906'
            assert args.physloc.enc == 1, \
                'Bad enclosure for --fixed1906'
            assert args.physloc.node in range(1, 5), \
                'Bad node for --fixed1906'

        _detect_memory_space(args, lfs_globals)     # Modifies args
        return apertures(args, lfs_globals)
    except Exception as e:
        msg = str(e)
        args.logger.error('ERROR IN PLATFORM SETUP: %s' % (msg))

    # seems to be ignored, as is SystemExit
    raise OSError(errno.EINVAL, 'lfs_shadow: %s' % msg)
