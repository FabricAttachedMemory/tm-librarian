#!/usr/bin/python -tt

import time

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

    def __init__(self, nApertures = 3):
        self._nApertures = nApertures
        pass

    def allocate(self, faultLZA, pid):
        existing = self._descriptors.get(faultLZA, None)
        if existing is not None:    # at least one pid exists in dict
            existing.update(pid)
            return None

        if len(self._descriptors) < self._nApertures:
            retval = None
        else:
            # Evict one.  For now, simple FIFO.  Once other stuff is in
            # place (like vma_close, etc) sort via __cmp__.
            evictLZA, LZAdetails = self._descriptors.popitem(last=False)
            retval = evictLZA
        self._descriptors[faultLZA] = LZA(faultLZA, pid)     # FIFO append
        return retval

