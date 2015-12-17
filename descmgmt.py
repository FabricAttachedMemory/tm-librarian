#!/usr/bin/python -tt

import time

import array
import fcntl
import struct
from collections import OrderedDict
from pdb import set_trace
from genericobj import GenericObject

class LZA(GenericObject):

    def __init__(self, LZA, pid):
        self.LZA = LZA                  # probably redundant
        self.pids = { pid: 1 }          # count
        self.mtime = int(time.time())   # epoch

    def update(self, pid):
        try:
            self.pids[pid] += 1         # Existing process, new fault
        except KeyError:
            self.pids.update({pid: 1})  # New process, first fault
        self.mtime = int(time.time())   # epoch

    def __cmp__(self, other):
        '''Oldest first, then by number of pids, then by total mappings'''
        tmp = cmp(self.mtime, other.mtime)
        if tmp:
            return tmp
        tmp = cmp(len(self.pids), len(other.pids))
        if tmp:
            return tmp
        selfmappings = sum(v for v in self.pids.values())
        othermappings = sum(v for v in other.pids.values())
        return cmp(selfmappings, othermappings)

class DescriptorManagement(GenericObject):

    _nApertures = None

    _descriptors = OrderedDict()    # track # of page mappings inside an LZA

    _DESBK_READ_OFF = 0xc0102100
    _DESBK_PUT      = 0xc0102102

    _descioctl = '/dev/descioctl'

    def __init__(self, nApertures = 3):
        self._nApertures = nApertures
        print([ hex(v) for v in  self.descTable])

    @property
    def descTable(self):
        fd = open(self._descioctl, 'wb')
        tmp = [ ]
        for index in range(self._nApertures):
            buf = array.array('Q', [index,] * 2)    # [0] == index, [1] == response
            junk = fcntl.ioctl(fd.fileno(), self._DESBK_READ_OFF, buf)
            tmp.append(buf[1])
        fd.close()
        return tmp

    def desbk_set(self, index, LZA):
        assert 0 <= index < self._nApertures, 'Bad index'
        fd = open(self._descioctl, 'wb')
        buf = array.array('Q', [index,] * 2)    # [0] == index, [1] == response
        buf[1] = (LZA << 33) + 1
        junk = fcntl.ioctl(fd, self._DESBK_PUT, buf)
        fd.close()

    def allocate(self, faultLZA, pid):
        existing = self._descriptors.get(faultLZA, None)
        if existing is not None:    # at least one pid exists in dict
            existing.update(pid)
            return None

        if len(self._descriptors) < self._nApertures:
            retval = None
        else:
            set_trace()
            # Evict one.  For now, simple FIFO.  Once other stuff is in
            # place (like vma_close, etc) sort via __cmp__.
            evictLZA, LZAdetails = self._descriptors.popitem(last=False)
            retval = evictLZA
        self._descriptors[faultLZA] = LZA(faultLZA, pid)     # FIFO append
        return retval

