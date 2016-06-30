#!/usr/bin/python3 -tt

# Read JSON constructed per the Software ERS that describes the topology
# of an instance of The Machine, aka TMCF (The Machine Config File).
# Create an object with attributes that follow the descent of the
# collections, i.e., obj.racks.enclosures.nodes.mediaControllers.
# Provide convenience properties that are entire collections, i.e.,
# obj.mediaControllers.

import inspect
import json
import os
import sys

from pdb import set_trace
from pprint import pprint

###########################################################################
# This will be included from manifesting, as well as run during unit test.
# http://stackoverflow.com/questions/16981921/relative-imports-in-python-3

try:
    from .genericobj import GenericObject   # external imports
except Exception as e:
    from genericobj import GenericObject    # __main__ below

###########################################################################


def multiplier(instr, section, book_size_bytes=0):
    '''this is used as a probe function for integers.  Return or raise.'''
    try:
        rsize = int(instr)
        return rsize                    # that was easy
    except ValueError as e:
        try:
            base = instr[:-1]
            rsize = int(base)           # so far so good
            suffix = instr[-1].upper()  # chomp one
            if suffix not in 'BKMGT':
                raise ValueError(
                    'Illegal multiplier "%s" in [%s]' % (suffix, section))
        except ValueError as e:
            raise ValueError('"%s" is not an integer' % base)
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
        if not index:
            raise IndexError('first index is 1')
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

    def __init__(self, **kwargs):
        assert 'hostname' not in kwargs, '"hostname" collides with property'
        super().__init__(**kwargs)
        self._hostname = None

    @property
    def dotname(self):
        return 'rack.%s.enc.%s.node.%s' % (self.rack, self.enc, self.node)

    @property
    def hostname(self):
        if self._hostname is None:
            # MFT/FRD: rack is always "1", or words like "A1.above_floor"
            self._hostname = 'node%02d' % (
                (int(self.enc) - 1) * 10 +
                int(self.node)
            )
        return self._hostname

    @hostname.setter
    def hostname(self, value):
        self._hostname = str(value)

class _GOmediaControllers(GenericObject):
    __qualname__ = 'mediaControllers'

###########################################################################


class tupledict(tuple):
    '''Allow indexing of TMConfig properties by int or str.  If str, treat
       it as a snippet to match against coordinate values.'''

    def __getitem__(self, index_or_key):
        try:
            i = int(index_or_key)
            try:
                return super().__getitem__(i)
            except Exception:
                return None
        except ValueError:
            pass
        try:
            key = str(index_or_key)
            tmp = tuple(i for i in iter(self) if key in i.coordinate)
            return tmp
        except Exception:
            return None

###########################################################################


class TMConfig(GenericObject):

    @staticmethod
    def unroll(obj, attr, item, depth=0, verbose=False):
      try:
        if verbose:
            print('    ' * depth, attr, end=': ')
        if isinstance(item, list):
            # MOST list elements are dicts.  The only exception as of
            # 2016-06-22 is the array of strings in the InterleaveGroup
            # mediaController expansion.
            if verbose:
                print('(list)')
            buildlist = []
            setattr(obj, attr, buildlist)
            for i, element in enumerate(item):
                if (isinstance(element, int) or
                    isinstance(element, float) or
                    isinstance(element, str)):
                    buildlist.append(element)
                elif isinstance(element, dict):
                    GO = globals().get('_GO' + attr, GenericObject)()
                    buildlist.append(GO)
                    for key, value in element.items():
                        TMConfig.unroll(GO, key, value, depth + 1, verbose)
                else:
                    print('Unexpected JSON construction', file=sys.stderr)
                    set_trace()
                    continue

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
                else:
                    print(item)
            if isinstance(item, str):
                try:    # probe for integer, maybe with multiplier
                    item = multiplier(item, attr)
                except Exception as e:
                    pass
            setattr(obj, attr, item)

      except Exception as e:
          set_trace()
          raise

    # Fix That For You so book_register can run to completion
    def _FTFY(self, attrs, obj, errfmt, vartuple):
        '''errfrmt and vartuple should agree on identifying obj.'''
        errfmt += ' missing %s'     # for the attribute
        for attr in attrs:
            if hasattr(obj, attr):
                continue
            self.FTFY.append(errfmt % (vartuple + (attr, )))
            setattr(obj, attr, 'You forgot an attribute.  BAD GEEK! BAD!')

    def __init__(self, path, verbose=False):
        setattr(_GOnodes, self.__class__.__name__, self)
        self.verbose = verbose
        self.FTFY = [ ]
        try:
            original = open(path, 'r').read()
            self._json = json.loads(original)
            for key, value in self._json.items():
                TMConfig.unroll(self, key, value, verbose=verbose)
            self._allServices = None
        except Exception as e:
            tb_lineno = sys.exc_info()[2].tb_lineno
            src, base_lineno = inspect.getsourcelines(self.__class__)
            if not src:
                raise RuntimeError('Line %d: %s' % (tb_lineno, str(e)))
            badsrc = src[tb_lineno - base_lineno][:-1].strip()
            raise RuntimeError(
                'Line %d: "%s": %s' % (tb_lineno, badsrc, str(e)))

        # Fixups and consistency checks.  Flesh out all relative coordinates
        # into absolutes and check for dupes, which intrinsically checks
        # a lot of things.  The list gets used again for IG checking.
        # Real machine hardware only has 13 bits of book number in an LZA
        max_NVM_per_MC = 8192 * self.bookSize

        self.racks = tupledict(self.racks)  # top level needs handling now
        allencs = [ ]
        allnodes = [ ]
        fullMCs = { }
        node_id = 1
        for rack in self.racks:
            rack.coordinate = self.coordinate + '/' + rack.coordinate
            for enc in rack.enclosures:
                enc.coordinate = rack.coordinate + '/' + enc.coordinate
                assert enc.coordinate not in allencs, \
                    'Duplicate enclosure coordinate %s' % enc.coordinate
                allencs.append(enc.coordinate)
                for node in enc.nodes:
                    node.coordinate = enc.coordinate + '/' + node.coordinate
                    assert node.coordinate not in allnodes, \
                        'Duplicate node coordinate %s' % node.coordinate
                    allnodes.append(node.coordinate)

                    node.soc.coordinate = node.coordinate + '/' + node.soc.coordinate
                    node.rack = rack.coordinate.split('/')[-1]
                    node.enc = enc.coordinate.split('/')[-1]
                    node.node = node.coordinate.split('/')[-1]
                    node.node_id = node_id
                    node_id += 1
                    for mc in node.mediaControllers:
                        mc.coordinate = node.coordinate + '/' + mc.coordinate
                        assert mc.coordinate not in fullMCs, \
                            'Duplicate MC coordinate %s' % mc.coordinate
                        assert mc.memorySize <= max_NVM_per_MC, \
                            'MC @ %s has too much NVM' % mc.coordinate
                        fullMCs[mc.coordinate] = mc      # for future reference
                        mc.node_id = node.node_id
                        # CID == enc[11-9]:node[8-4]:subCID[3-0] making an 11-bit CID
                        # External representations of full fields are all
                        # option base 1, but each subfield of rawCID must be
                        # option base 0
                        # SubCID is the GenZ responder for MCs, runs from 8 - 11.
                        subCID = int(mc.coordinate.split('/')[-1])
                        mc.rawCID = (((int(node.enc) - 1) << 9) +
                                     ((int(node.node) - 1) << 4) +
                                     ((subCID - 1) + 8))
                    node.totalNVM = sum(mc.memorySize for mc in node.mediaControllers)

                    # Find it earlier, report it with more clarity
                    self._FTFY(
                        ('tlsPublicCertificate', ),
                        node.soc,
                        'node SOC "%s"',
                        (node.dotname, )
                    )

        # IGs only have absolute coordinates; "update" them with node's full
        # definition.  fullMCs is now a "countdown" consistency check.
        groupIds = [ ]
        for IG in self.interleaveGroups:
            assert IG.groupId not in groupIds, \
                'Duplicate interleave group ID %d' % IG.groupId
            groupIds.append(IG.groupId)
            updateMCs = [ ]
            for mc in IG.mediaControllers:
                # Original TMCF had coord and memsize keys.  I objected to
                # the memsize duplication, but Keith removed the coord key,
                # reducing the one-item dict to a simple string.  Handle
                # both cases.
                coordinate = getattr(mc, 'coordinate', mc)
                if coordinate not in fullMCs:
                    msg = 'IG MC %s not found in any node' % coordinate
                    raise ValueError(msg)
                assert not hasattr(mc, 'memorySize'), \
                    'IG definition of MC cannot set memorySize'
                updateMCs.append(fullMCs[coordinate])   # reuse
                del fullMCs[coordinate]
            IG.mediaControllers = OptionBaseOneTuple(updateMCs)

        self.unused_mediaControllers = tuple(fullMCs.keys())

        assert self.totalNVM == sum(mc.memorySize for
            mc in self.mediaControllers), 'NVM memory mismatch'

    # Some shortcuts to commonly accessed items.   'racks" is already at
    # the top level.  Realize any generators so the caller can do len()
    # and access via [] as a tupledict.

    @property
    def totalNVM(self):
        return sum(node.totalNVM for node in self.nodes)

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
    def services(self):
        if self._allServices is not None:
            return self._allServices
        self._allServices = { }

        # There's old and new, but there could be both.  Try old first,
        # perhaps cobble up new style records and fall through.
        tmp = getattr(self, 'managementServer', None)
        if tmp is not None:
            fakeNew = GenericObject(
                services=[],
            )
            ipv4Address = ''   # all restUri attributes should agree
            for name, detail in tmp.__dict__.items():
                if isinstance(detail, GenericObject):
                    detail.service = name
                    fakeNew.services.append(detail)
                    tmp = getattr(detail, 'restUri', '')
                    if tmp:
                        try:
                            tmp = tmp.split(':')[1].split('/')[-1]
                        except Exception as e:
                            continue
                        if ipv4Address:
                            assert ipv4Address == tmp, \
                                'managementServer:%s URI hostname mismatch' % name
                        else:
                            ipv4Address = tmp
                else:
                    setattr(fakeNew, name, detail)

            assert ipv4Address, 'Cannot discern managementServer hostname'
            fakeNew.ipv4Address = ipv4Address
            try:
                self.servers.append(fakeNew)
            except AttributeError as e:
                self.servers = ( fakeNew, )

        if hasattr(self, 'servers'):    # new style
            for server in self.servers:
                hostname = server.ipv4Address
                for service in server.services:
                    assert service not in self._allServices, \
                        'Duplicate service ' + service

                    # Find it earlier, report it with more clarity
                    self._FTFY(
                        ('tlsPublicCertificate', ),
                        service,
                        'service "%s"',
                        (service.service, )
                    )
                    try:
                        service.restUri = service.restUri.replace(
                            '${ipv4Address}', hostname)
                    except AttributeError as e:
                        pass
                    self._allServices[service.service] = service

        if not self._allServices:
            raise RuntimeError('Cannot find any services')

        return self._allServices

    @property
    def bookSize(self):
        return self.services['librarian'].bookSize

###########################################################################


if __name__ == '__main__':
    try:
        config = TMConfig(sys.argv[1], verbose=True)
    except Exception as e:
        raise SystemExit(str(e))

    print()
    if config.FTFY:
        print('Added missing attribute(s):\n%s\n' % '\n'.join(config.FTFY))
    racks = config.racks
    encs = config.enclosures
    nodes = config.nodes
    MCs = config.mediaControllers
    print('Book size = %d' % config.bookSize)
    tmp = config.totalNVM >> 40
    if tmp:
        msg = '%d TB' % tmp
    else:
        tmp = config.totalNVM >> 30
        if tmp:
            msg = '%d GB' % tmp
        else:
            msg = '%d MB' % config.totalNVM >> 20

    print('%d racks, %d enclosures, %d nodes, %d media controllers == %s total NVM' %
        (len(racks), len(encs), len(nodes), len(MCs), msg))
    if config.unused_mediaControllers:
        print('MCs not assigned to an IG:')
        pprint(config.unused_mediaControllers)

    # Use a substring of sufficient granularity to satisfy your needs
    nodes_in_enc_1 = nodes['enclosure/1']

    # This "search function" is implicitly across all enclosures
    MCs_in_all_node_2s = MCs['node/2']
    print(nodes[-1].dotname, 'is', nodes[-1].hostname)
    set_trace()
    pass
