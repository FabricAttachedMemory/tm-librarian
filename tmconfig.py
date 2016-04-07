#!/usr/bin/python3 -tt

# Read a JSON document constructed per the Software ERS that describes
# the topology of an instance of The Machine.  Turn JSON into an object
# with attributes that follow the descent of the collections, i.e.,
# obj.racks.enclosures.nodes.media_controllers. Provide properties
# that are entire collections, i.e., obj.media_controllers.

import inspect
import json
import os
import sys

from pdb import set_trace
from pprint import pprint

from book_register import multiplier
from genericobj import GenericObject

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


class _GOmedia_controllers(GenericObject):
    __qualname__ = 'media_controllers'


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
        for rack in self.racks:
            rack.coordinate = self.coordinate + '/' + rack.coordinate
            for enc in rack.enclosures:
                enc.coordinate = rack.coordinate + '/' + enc.coordinate
                for node in enc.nodes:
                    node.coordinate = enc.coordinate + '/' + node.coordinate
                    for mc in node.media_controllers:
                        mc.coordinate = node.coordinate + '/' + mc.coordinate
                        assert mc.coordinate not in allMCs, \
                            'Duplicate MC coordinate %s' % mc.coordinate
                        allMCs.append(mc.coordinate)

        # IGs already have absolute coordinates.  Compare them to the nodes.
        groupIds = [ ]
        for IG in self.interleaveGroups:
            assert IG.groupId not in groupIds, \
                'Duplicate interleave group ID %d' % IG.groupId
            groupIds.append(IG.groupId)
            for mc in IG.media_controllers:
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
    def nodes(self):
        nodes = []
        for enc in self.enclosures:
            nodes.extend(enc.nodes)
        return tupledict(nodes)

    @property
    def media_controllers(self):
        MCs = []
        for node in self.nodes:
            MCs.extend(node.media_controllers)
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
    MCs = config.media_controllers
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
    set_trace()
    pass
