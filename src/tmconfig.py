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
            instr = instr.strip()
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


class OptionBaseOneTuple(GenericObject):
    '''If it's an item with a physically enumerated coordinate,
       force 1-based indexing.  Account for sparse populations.'''
    def __init__(self, *args, **kwargs):
        sequence = args[0]  # but what are they?
        coord = getattr(sequence[0], 'coordinate', None) if sequence else None
        if coord is None:
            self._ob1 = False   # zero-based: servers, services, IGs
            self._value = tuple(sequence)   # "freeze" it
            return
        elems = coord.split('/')
        try:
            index = int(elems[-1])
            self._ob1 = True    # fall through to padding
        except ValueError:
            # Is it a rack coord?  Those aren't explicitly enumerated.
            # Either way I just need a simple tuple, no packing.
            self._ob1 = len(elems) == 2 and elems[0] == 'Rack'
            self._value = tuple(sequence)
            return

        # Assume sequence is homogoneous, ie, they all have a coordinate
        # like [0] did above.  Also, coords are relative.
        padded = []
        for s in sequence:
            elems = s.coordinate.split('/')
            index = int(elems[-1])
            while index - 1 > len(padded):
                padded.append(None)
            padded.append(s)
        if elems[0] == 'Enclosure':
            extend = 4  # FRD 2016
        elif elems[0] == 'Node':
            extend = 10
        else:
            extend = 0
        if extend:
            padded.extend((None,) * (extend - len(padded)))
        self._value = tuple(padded)

    def __len__(self):
        return len(self._value)

    def __iter__(self):
        # return iter(self._value)  # includes sparsity
        return (v for v in self._value if v is not None)

    @property
    def populated(self):
        return [v for v in self._value]

    def __getitem__(self, index):
        if self._ob1:
            if not index:
                raise IndexError('first index is 1')
            if index > 0:
                index -= 1
        return self._value[index]

###########################################################################
# Subclass GenericObject so attribute errors report a more useful class.
# Dancing around __metaclasses__ here FIXME RTFM and use them.


class _GOanon(GenericObject):
    __qualname__ = 'anonymous'
    __title__ = 'element'


class _GOracks(GenericObject):
    __qualname__ = 'rack'
    __title__ = 'Rack'


class _GOenclosures(GenericObject):
    __qualname__ = 'enclosure'
    __title__ = 'Enclosure'


class _GOnodes(GenericObject):
    __qualname__ = 'node'
    __title__ = 'Node'

    def __init__(self, **kwargs):
        assert 'hostname' not in kwargs, '"hostname" collides with property'
        super().__init__(**kwargs)
        self._hostname = None
        # Trickle-down effect from 990 PHYSADDR.  It will always be zero here.
        # Only used in book_register.py.
        self.nvm_physaddr = [0,]

    @property
    def dotname(self):
        return 'rack.%s.enc.%s.node.%s' % (self.rack, self.enc, self.node)

    @property
    def hostname(self):     # see the first line of TMConfig.__init__()
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

    @property
    def node_id(self):
        return ((self.enc - 1) * 10 ) + self.node


class _GOmediaControllers(GenericObject):
    __qualname__ = 'mediaController'
    __title__ = 'MediaController'

###########################################################################


class tupledict(tuple):
    '''Allow indexing of TMConfig properties by int or str.  If str, treat
       it as a snippet to match against coordinate values.'''

    def __init__(self, inseq):
        self._value = inseq

    def __getitem__(self, index_or_key):
        try:
            i = int(index_or_key)
            return self._value[i]   # IndexError will raise out
        except ValueError:
            pass
        try:
            key = str(index_or_key)
            tmp = tuple(i for i in self._value if key in i.coordinate)
            return tmp
        except Exception:
            return None

###########################################################################


class TMConfig(GenericObject):
    __title__ = 'Primary'

    @staticmethod
    def unroll(obj, attr, item, depth=0, verbose=False):
        '''attr is a JSON key and is camel case (usually lower).'''
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
                        GO = globals().get('_GO' + attr, _GOanon)()
                        buildlist.append(GO)
                        for key, value in element.items():
                            TMConfig.unroll(GO, key, value, depth + 1, verbose)
                    else:
                        print('Unexpected JSON construction', file=sys.stderr)
                        set_trace()
                        continue

                # Make buildlist immutable and Drewable
                tmp = getattr(obj, attr)
                assert tmp is buildlist, 'This is sooooo not good'
                tmp = OptionBaseOneTuple(buildlist, attr=attr)
                setattr(obj, attr, tmp)

            elif isinstance(item, dict):
                if verbose:
                    print('(dict)')
                GO = globals().get('_GO' + attr, _GOanon)()
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
                if attr == 'hostname':
                    pass
                setattr(obj, attr, item)

        except Exception as e:    # Syntax, logic, whatever; I'm done
            print('Line %d: %s' % (
                  sys.exc_info()[2].tb_lineno, str(e)), file=sys.stderr)
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

    # Supporting data for validating proper case of coordinate elements.
    # Remember, the JSON keys are camel case.
    _StudlyKeys = (
        'MachineVersion', 'Datacenter', 'Frame', 'Rack',
        'Enclosure', 'EncNum', 'Node', 'MemoryBoard', 'SocBoard'
    )
    _StudlyCase = dict(zip([e.lower() for e in _StudlyKeys], _StudlyKeys))

    def finish_child(self, child, parent=None):
        '''Establish fwd/rev links, validate and extend coordinates'''
        # Initial setup and error checking
        child.parent = parent
        coord = child.coordinate
        errhdr = '%s coordinate "%s" ' % (child.__class__.__title__, coord)
        if ' ' in coord:
            self.errors.append(errhdr + 'has superfluous whitespace')

        if parent is None:
            prefix = ''
        else:
            if coord.strip('/') != coord:
                self.errors.append(errhdr + 'has leading or trailing "/"')
            prefix = parent.coordinate + '/'
            try:
                parent.children.append(child)
            except AttributeError as e:
                parent.children = [child, ]

        # Go ahead and do it, then validate
        # FIXME: calculate coordinate on the fly, instead of fixed val.
        child.coordinate = prefix + coord
        elems = coord.split('/')     # Parent gets checked earlier
        for e in elems:
            tmp = self._StudlyCase.get(e.lower(), e)
            if tmp != e:
                self.errors.append(errhdr + 'case should be "%s"' % tmp)

    def __init__(self, path, verbose=False):
        # Support for managing 'hostname' property in this class
        setattr(_GOnodes, self.__class__.__name__, self)
        self.verbose = verbose
        self.FTFY = [ ]
        try:
            with open(path, 'r') as f:
                original = f.read()
            self._json = json.loads(original)
            for key, value in self._json.items():
                TMConfig.unroll(self, key, value, verbose=verbose)
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

        self._allServices = None
        self.errors = [ ]

        # Duplicate detection
        allencs = [ ]
        allnodes = [ ]
        fullMCs = { }

        # Since unroll() blindly takes any keys, guard against typos.  Yes
        # it could be folded into finish_child but this sufficient for now.
        def getchilditer(obj, attr):
            val = getattr(obj, attr, False)
            if not val:
                self.errors.append(
                    '%s %s has no %s' % (obj.__title__, obj.coordinate, attr))
            return val

        if not hasattr(self, 'racks'):
            self.errors.append('No racks were found')
            return
        if not self.coordinate.startswith('/MachineVersion/1/'):
            self.errors.append('Illegal MachineVersion "%s"' % self.coordinate)
            # fall through, find other stuff
        self.finish_child(self)
        for rack in self.racks:
            self.finish_child(rack, self)
            enclooper = getchilditer(rack, 'enclosures')
            if not enclooper:
                return
            for enc in enclooper:
                self.finish_child(enc, rack)
                if enc.coordinate in allencs:
                    self.errors.append(
                        'Duplicate enclosure coordinate %s' % enc.coordinate)
                    continue
                allencs.append(enc.coordinate)
                nodelooper = getchilditer(enc, 'nodes')
                if not nodelooper:
                    return
                for node in nodelooper:
                    self.finish_child(node, enc)
                    if node.coordinate in allnodes:
                        self.errors.append(
                            'Duplicate node coordinate %s' % node.coordinate)
                        continue
                    allnodes.append(node.coordinate)

                    self.finish_child(node.soc, node)
                    node.rack = rack.coordinate.split('/')[-1]  # string
                    node.enc = int(enc.coordinate.split('/')[-1])
                    node.node = int(node.coordinate.split('/')[-1])
                    if hasattr(node.soc, 'hostname'):
                        node.hostname = node.soc.hostname   # property set
                    else:
                        # This won't go over well in France.
                        self._FTFY(
                            ('hostname', ),
                            node.soc,
                            'SOC coord "%s"',
                            (node.dotname, )
                        )
                    # Transition period: LMP is (still) pushing it so fake it
                    if not hasattr(node.soc, 'macAddress'):  # older file
                        node.soc.macAddress = 'n/a'

                    mclooper = getchilditer(node, 'mediaControllers')
                    if not mclooper:
                        return
                    for mc in mclooper:
                        self.finish_child(mc, node)
                        if mc.coordinate in fullMCs:
                            self.errors.append(
                                'Duplicate MC coordinate %s' % mc.coordinate)
                            continue
                        fullMCs[mc.coordinate] = mc     # for future reference
                        mc.node_id = node.node_id       # 1-80

                        # FIXUP for alignment with frdnode.FAModule.  Real
                        # FRD HW only has 13 bits of book number in an LZA
                        mc.module_size_books = mc.memorySize // self.bookSize
                        if mc.module_size_books > 8192:
                            self.errors.append(
                                'MC @ %s has too much NVM' % mc.coordinate)
                            # keep going

                        # CID == enc[11-9]:node[8-4]:subCID[3-0] making an
                        # 11-bit CID.  External representations of full fields
                        # are all option base 1, but each subfield of rawCID
                        # must be option base 0.  SubCID is the GenZ responder
                        # for MCs, runs from 8 - 11.
                        subCID = int(mc.coordinate.split('/')[-1])
                        mc.rawCID = (((int(node.enc) - 1) << 9) +
                                     ((int(node.node) - 1) << 4) +
                                     ((subCID - 1) + 8))
                    node.totalNVM = sum(mc.memorySize
                                        for mc in node.mediaControllers)

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
        IGlooper = getchilditer(self, 'interleaveGroups')
        if not IGlooper:
            return
        for IG in IGlooper:
            IG.coordinate = ''  # for completeness
            assert IG.groupId not in groupIds, \
                'Duplicate interleave group ID %d' % IG.groupId
            groupIds.append(IG.groupId)
            updateMCs = [ ]
            mclooper = getchilditer(IG, 'mediaControllers')
            if not mclooper:
                return
            for mc in mclooper:
                # Original TMCF had coord and memsize keys.  I objected to
                # the memsize duplication, but Keith removed the coord key,
                # reducing the one-item dict to a simple string.  Handle
                # both cases.
                coordinate = getattr(mc, 'coordinate', mc)
                if coordinate not in fullMCs:
                    self.errors.append('IG %d MC %s not in any node' % (
                        IG.groupId, coordinate))
                    continue
                if hasattr(mc, 'memorySize'):
                    self.errors.append('IG %d MC %s sets memorySize' % (
                        IG.groupId, coordinate))
                updateMCs.append(fullMCs[coordinate])   # reuse
                del fullMCs[coordinate]
            IG.mediaControllers = OptionBaseOneTuple(updateMCs)

            # Another FIXUP for alignment with frdnode.FAModule
            IG.total_books = sum(mc.module_size_books
                                 for mc in IG.mediaControllers)

        if self.totalNVM != sum(mc.memorySize for
                                mc in self.allMediaControllers):
            self.errors.append('NVM memory mismatch')

        self.unused_mediaControllers = tuple(fullMCs.keys())

    # Some shortcuts to commonly accessed items.   'racks" is already at
    # the top level.  Realize any generators so the caller can do len()
    # and access via [] as a tupledict.

    @property
    def allRacks(self):    # for completness
        racks = [ self.racks[1] ]
        return tupledict(racks)

    @property
    def allEnclosures(self):
        # I think I'm missing something about nested comprehensions
        # when a closure is involved, so I fell back to explicit.
        enclosures = []
        for rack in self.racks:
            enclosures.extend(rack.enclosures)
        return tupledict(enclosures)

    @property
    def allNodes(self):
        nodes = []
        for rack in self.racks:
            for enclosure in rack.enclosures:
                nodes.extend(enclosure.nodes)
        return tupledict(nodes)

    @property
    def totalNVM(self):
        return sum(node.totalNVM for node in self.allNodes)

    @property
    def allMediaControllers(self):
        MCs = []
        for node in self.allNodes:     # property ref
            MCs.extend(node.mediaControllers)
        return tupledict(MCs)

    @property
    def allIGs(self):
        IGs = []
        for IG in self.interleaveGroups:    # parsed from JSON
            IGs.extend(IG)
        return tupledict(IGs)

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
                                'managementServer:%s hostname mismatch' % name
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
    from pprint import pprint
    try:
        config = TMConfig(sys.argv[1], verbose=True)
    except Exception as e:  # Probably a parser bug
        set_trace()
        raise SystemExit(str(e))

    if config.errors:
        print('Errors detected during parse:')
        print('\n'.join(config.errors))
        raise SystemExit(1)

    if config.FTFY:
        print('Added missing attribute(s):\n%s\n' % '\n'.join(config.FTFY))

    allRacks = config.allRacks
    allEnclosures = config.allEnclosures
    allNodes = config.allNodes
    allMCs = config.allMediaControllers
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

    print('%d racks, %d enclosures, %d nodes, %d MCs -> %s total NVM' %
          (len(allRacks), len(allEnclosures), len(allNodes), len(allMCs), msg))
    if config.unused_mediaControllers:
        print('MCs not assigned to an IG:')
        pprint(config.unused_mediaControllers)
    print(allNodes[-1].dotname, 'is', allNodes[-1].hostname)

    # Use a substring of sufficient granularity to satisfy your needs
    nodes_in_enc_1 = allNodes['EncNum/1']

    # This "search function" is implicitly across all enclosures
    MCs_in_all_node_2s = allMCs['Node/2']

    # Get populated enclosures, None means no item in that slot.
    print('\nPopulated enclosures:')
    pprint(config.racks[1].enclosures.populated)

    if 'sparseNode.json' in sys.argv[1]:
        print('\nPopulated nodes in enclosure 3:')
        pprint(config.racks[1].enclosures[3].nodes.populated)

    set_trace()
    pass
