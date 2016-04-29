#!/usr/bin/python3 -tt

# Read a JSON document constructed per the Software ERS that describes
# the topology of an instance of The Machine.  Turn JSON into an object
# with attributes that follow the descent of the collections, i.e.,
# obj.racks.enclosures.nodes.mediaControllers. Provide properties
# that are entire collections, i.e., obj.mediaControllers.

import inspect
import json
import os
import sys

from pdb import set_trace
from pprint import pprint

###########################################################################
# This will be included from manifesting, as well as run during unit test.
# http://stackoverflow.com/questions/16981921/relative-imports-in-python-3

if __name__ != '__main__':
    from .genericobj import GenericObject
else:
    from genericobj import GenericObject

###########################################################################
def multiplier(instr, section, book_size_bytes=0):

    suffix = instr[-1].upper()
    if suffix not in 'BKMGT':
        raise ValueError(
            'Illegal size multiplier "%s" in [%s]' % (suffix, section))
    rsize = int(instr[:-1])
    if suffix == 'K':
        return rsize * 1024
    if suffix == 'M':
        return rsize * 1024 * 1024
    elif suffix == 'G':
        return rsize * 1024 * 1024 * 1024
    elif suffix == 'T':
        return rsize * 1024 * 1024 * 1024 * 1024

    # Suffix is 'B' to reach this point
    if not book_size_bytes:
        raise ValueError(
            'multiplier suffix "B" not useable in [%s]' % section)
    return rsize * book_size_bytes

###########################################################################
# Because Drew.

class OptionBaseOneTuple(tuple):
    def __getitem__(self, index):
        set_trace()
        if not index:
            raise IndexError
        if index > 0:
            index -= 1
        return super(self.__class__, self).__getitem__(index)

###########################################################################
# Subclass GenericObject so attribute errors report a more useful class.
# Dancing around __metaclasses__ here FIXME RTFM and use them.


class _GObase(GenericObject):
    pass

class _GOracks(GenericObject):
    __qualname__ = 'racks'


class _GOenclosures(GenericObject):
    __qualname__ = 'enclosures'


class _GOnodes(GenericObject):
    __qualname__ = 'nodes'

    @property
    def dotname(self):
        return 'rack.%s.enc.%s.node.%s' % (self.rack, self.enc, self.node)


class _GOmediaControllers(GenericObject):
    __qualname__ = 'mediaControllers'

###########################################################################


class tupledict(tuple):
    '''Allow indexing of TMConfig properties by int or str.  If str, treat
       it as a snippet to match against coordinate values.'''

    def __getitem__(self, index_or_key):
        if isinstance(index_or_key, int):
            return super().__getitem__(index_or_key)
        assert isinstance (index_or_key, str), 'Either an int or str'
        tmp = tuple(i for i in iter(self) if index_or_key in i.coordinate)
        return tmp

###########################################################################


class TMConfig(GenericObject):

    @staticmethod
    def unroll(obj, attr, item, depth=0, verbose=False):
      try:
        if verbose:
            print('    ' * depth, attr, end=': ')
        if isinstance(item, list):
            # ALL list elements are dicts per the ERS.  I think.
            if verbose:
                print('(list)')
            buildlist = []
            setattr(obj, attr, buildlist)
            for i, element in enumerate(item):
                if not isinstance(element, dict):
                    print('Unexpected JSON construction', file=sys.stderr)
                    set_trace()
                    continue
                GO = globals().get('_GO' + attr, GenericObject)()
                buildlist.append(GO)
                for key, value in element.items():
                    TMConfig.unroll(GO, key, value, depth + 1, verbose)

            # Make buildlist immutable and Drewable
            setattr(obj, attr, OptionBaseOneTuple(getattr(obj, attr)))

        elif isinstance(item, dict):
            if verbose:
                print('(dict)')
            GO = globals().get('_GO' + attr, GenericObject)()
            setattr(obj, attr, GO)
            for key, value in item.items():
                TMConfig.unroll(GO, key, value, depth + 1, verbose)

        else:   # assume scalar, end of recursion
            if verbose:
                if isinstance(item, str):
                    print(item[:40], '...')
                    try:
                        if len(item):
                            item = multiplier(item, attr)
                    except Exception as e:
                        pass
                else:
                    print(item)
            setattr(obj, attr, item)

      except Exception as e:
          set_trace()
          raise

    def __init__(self, path, verbose=False):
        setattr(_GOnodes, self.__class__.__name__, self)
        try:
            self.verbose = verbose
            original = open(path, 'r').read()
            self._json = json.loads(original)
            for key, value in self._json.items():
                TMConfig.unroll(self, key, value, verbose=verbose)
            self.error = ''
        except Exception as e:
            self.error = 'Line %d: %s' % (
                sys.exc_info()[2].tb_lineno, str(e))
            set_trace()
            raise RuntimeError(str(e))

        # Fixups and consistency checks.  Flesh out all relative coordinates
        # into absolutes and check for dupes, which intrinsically checks
        # a lot of things.  The list gets used again for IG checking.

        self.racks = tupledict(self.racks)  # top level needs handling now
        allMCs = [ ]
        node_id = 1
        for rack in self.racks:
            rack.coordinate = self.coordinate + '/' + rack.coordinate
            for enc in rack.enclosures:
                enc.coordinate = rack.coordinate + '/' + enc.coordinate
                for node in enc.nodes:
                    node.coordinate = enc.coordinate + '/' + node.coordinate
                    node.rack = rack.coordinate.split('/')[-1]
                    node.enc = enc.coordinate.split('/')[-1]
                    node.node = node.coordinate.split('/')[-1]
                    node.node_id = node_id
                    node_id += 1
                    for mc in node.mediaControllers:
                        mc.coordinate = node.coordinate + '/' + mc.coordinate
                        assert mc.coordinate not in allMCs, \
                            'Duplicate MC coordinate %s' % mc.coordinate
                        mc.node_id = node.node_id
                        # CID == enc[11-9]:node[8-4]:subCID[3-0] making an 11-bit CID
                        subCID = int(mc.coordinate.split('/')[-1])
                        mc.rawCID = (int(node.enc) << 8) + (int(node.node_id) << 4) + subCID
                        allMCs.append(mc.coordinate)

        # IGs already have absolute coordinates.  Compare them to the nodes.
        groupIds = [ ]
        for IG in self.interleaveGroups:
            assert IG.groupId not in groupIds, \
                'Duplicate interleave group ID %d' % IG.groupId
            groupIds.append(IG.groupId)
            for mc in IG.mediaControllers:
                if  mc.coordinate not in allMCs:
                    msg = 'IG MC %s not found in any node' % mc.coordinate
                    set_trace()
                    raise ValueError(msg)
                allMCs.remove(mc.coordinate)    # there can be only one
        self.unused_memory_controllers = tuple(allMCs)

    # Some shortcuts to commonly accessed items.   'racks" is already at
    # the top level.  Realize any generators so the caller can do len()
    # and access via [] as a tupledict.

    @property
    def enclosures(self):
        # I think I'm missing something about nested comprehensions
        # when a closure is involved, so I fell back to explicit.
        # FIXME: None-padding on missing enclosures?
        enclosures = []
        for rack in self.racks:
            enclosures.extend(rack.enclosures)
        return tupledict(enclosures)

    @property
    def IGs(self):
        IGs = []
        for IG in self.interleaveGroups:
            IGs.extend(IG)
        return tupledict(IGs)

    @property
    def nodes(self):
        nodes = []
        for enc in self.enclosures:
            nodes.extend(enc.nodes)
        return tupledict(nodes)

    @property
    def mediaControllers(self):
        MCs = []
        for node in self.nodes:
            MCs.extend(node.mediaControllers)
        return tupledict(MCs)

    @property
    def bookSize(self):
        return self.managementServer.librarian.bookSize

###########################################################################

if __name__ == '__main__':
    config = TMConfig(sys.argv[1], verbose=True)
    if config.error:
        raise SystemExit(config.error)
    racks = config.racks
    encs = config.enclosures
    nodes = config.nodes
    MCs = config.mediaControllers
    print('%d racks, %d enclosures, %d nodes, %d media controllers' %
        (len(racks), len(encs), len(nodes), len(MCs)))
    print('Book size = %d' % config.bookSize)
    if config.unused_memory_controllers:
        print('MCs not assigned to an IG:')
        pprint(config.unused_memory_controllers)

    # Use a substring of sufficient granularity to satisfy your needs
    nodes_in_enc_1 = nodes['enclosure/1']

    # This is implicitly across all enclosures
    MCs_in_all_node_2s = MCs['node/2']
    print(nodes[0].dotname)
    set_trace()
    pass
