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
        # Set up an array of 2 longs.  First is the index, second is response
        buf = array.array('Q', [0, 0])
        desbk = [ ]
        with open(self._descioctl, 'wb') as f:
            for index in range(self._nApertures):
                buf[0] = index
                try:
                    junk = fcntl.ioctl(f, self._DESBK_READ_OFF, buf)
                    desbk.append(buf[1])
                except Exception as e:
                    break
        assert len(desbk) == self._nApertures, 'Bad descriptor table read'
        return desbk

    def desbk_set(self, index, LZA):
        assert 0 <= index < self._nApertures, 'Bad index'
        # Set up an array of 2 longs.  First is the index, second is new value
        buf = array.array('Q', [index, (LZA << 33) + 1 ])
        with open(self._descioctl, 'wb') as f:
            junk = fcntl.ioctl(f, self._DESBK_PUT, buf)

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

