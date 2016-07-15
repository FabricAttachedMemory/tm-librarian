#!/usr/bin/python3 -tt

# Full Rack Demo (FRD) for June 2016
# Max of one rack of eight enclosures of ten nodes: exclude rack.  FRD
# has 4T per node, or 512 books per node, or 128 books per media controller.

# Chipset ERS 2.02 section 2.21.3.1 option 1: CID== enc[3]:node[4]:subCID[4]
# making an 11-bit CID.  Media controllers are  GenZ responders 1000b - 1111b
# (0x8 - 0xF) but only first 4 # used in FRD (0x8 - 0xB).  "module_size" is
# a phrase from the ERS; there it's in bytes, here it's in books.

# __repr__ functions are really about easier debugging than intended purpose

import os
from pdb import set_trace

from genericobj import GenericObject

#--------------------------------------------------------------------------


class FRDnodeID(object):
    """Reverse-calculation of a node ID 1-80 from enc 1-8  and node 1-10."""

    @property
    def node_id(self):
        return (self.rack - 1) * 80 + (self.enc - 1) * 10 + self.node

    @property
    def hostname(self):
        return 'node%02d' % self.node_id

    @property
    def REN(self):
        '''Rack # Enc # Node # as a string'''
        return 'r%de%dn%d' % (self.rack, self.enc, self.node)

    def __eq__(self, other):
        return self.node_id == other.node_id

    def __hash__(self):
        return self.node_id

#--------------------------------------------------------------------------


class FRDFAModule(FRDnodeID):
    '''One Media Controller (MC) and the books of NVM behind it.'''

    MC_STATUS_OFFLINE = 0
    MC_STATUS_ACTIVE = 1

    def __init__(self, STRorCID=None, enc=None, node=None, ordMC=None,
                 module_size_books=0):
        self.module_size_books = module_size_books
        if STRorCID is not None:
            '''Break apart an encoded string, or just return integer'''
            try:
                enc, node, ordMC = STRorCID.split(':')
                enc = int(enc)
                node = int(node)
                ordMC = int(ordMC)
            except Exception as e:
                # Values in a raw CID are zero based
                enc = (((STRorCID >> 9) & 0x7) + 1)   # 3 bits
                node = (((STRorCID >> 4) & 0xF) + 1)  # 4 bits
                ordMC = STRorCID & 0x3                # 2 LSB in FRD
        else:
            assert enc is not None and node is not None and ordMC is not None

        assert 1 <= enc <= 8, 'Bad enclosure value'
        assert 1 <= node <= 10, 'Bad node value'
        assert 0 <= ordMC <= 3, 'Bad ordMC value'
        self.rack = 1   # FRD, just practicing math elsewhere
        self.enc = enc
        self.node = node
        self.ordMC = ordMC
        self.rawCID = (enc - 1) << 9 | (node - 1) << 4 | (8 + ordMC)

    def __str__(self):
        return 'node_id %2d: %d:%d:%d (%d books)' % (
            self.node_id, self.enc, self.node, self.ordMC,
            self.module_size_books)

    def __repr__(self):
        return self.__str__()

    def __sub__(self, other):
        '''Return the NGMI hop count between two media controllers'''
        if self.enc != other.enc:
            return 5

        # same enclosure
        if self.node != other.node:
            return 3

        # same enclosure, same node
        assert self.ordMC != other.ordMC, 'Psychotic self-talk'
        return 1

#--------------------------------------------------------------------------


class MCCIDlist(object):
    '''Better than a plain list becauase of __repr__ and __str__'''

    def __init__(self, rawCIDlist=None, module_size_books=0):
        if rawCIDlist is None:
            self.mediaControllers = [ ]
            return
        assert len(rawCIDlist) <= 8, 'CID list element count > 8'
        self.mediaControllers = [
            FRDFAModule(STRorCID=c, module_size_books=module_size_books)
                     for c in rawCIDlist
        ]

    def __repr__(self):
        return str([ '0x%x' % cid.rawCID for cid in self.mediaControllers ])

    def __str__(self):
        tmp = [ str(cid) for cid in self.mediaControllers ]
        return str(tmp)

    # Duck-type a list

    def __getitem__(self, index):
        return self.mediaControllers[index]

    def __iter__(self):
        return iter(self.mediaControllers)

    def __len__(self):
        return len(self.mediaControllers)

    def append(self, newFAModule):
        assert len(self.mediaControllers) < 8, 'Max length exceeded'
        self.mediaControllers.append(newFAModule)

#--------------------------------------------------------------------------


class FRDnode(FRDnodeID):

    SOC_STATUS_OFFLINE = 0
    SOC_STATUS_ACTIVE = 1
    SOC_HEARTBEAT_SECS = 300.0

    def __init__(self, node, enc=None, module_size_books=0, autoMCs=True):
        node_id = None
        if enc is None:
            try:
                # encoded string
                rack, enc, node = node.split(':')
                rack = int(rack)
                enc = int(enc)
                node = int(node)
            except Exception as e:
                # old school, node is enumerator 1-80
                assert 1 <= node <= 80, 'Bad node enumeration value'
                node_id = node
                n = node - 1   # modulus math
                node = (n % 10) + 1
                enc = ((n % 80) // 10) + 1
                rack = (n // 80) + 1    # always 1
        else:
            rack = 1    # FRD: 1 rack

        assert 1 == rack, 'Bad rack value'
        assert 1 <= enc <= 8, 'Bad enclosure value'
        assert 1 <= node <= 10, 'Bad node value'
        self.node = node
        self.enc = enc
        self.rack = rack
        if node_id is not None:     # property check
            assert self.node_id == node_id

        # Duck-type spoof the objects generated from a JSON TMCF.  A better
        # long-term approach is to meld this file's objects into tmconfig.py.

        self.coordinate = 'node/%d' % (self.node)
        self.serialNumber = self.physloc
        self.soc = GenericObject(
            macAddress='52:54:00:%02d:%02d:%02d' % (
                self.node_id, self.node_id, self.node_id),
            coordinate='soc_board/1/soc/1',
            tlsPublicCertificate='NotToday'
        )

        # Media controllers. If not auto-generated (see the chipset ERS)
        # there's probably a custom module_size_books in the INI file.
        if not autoMCs:
            self.mediaControllers = []
            return

        # Could also loop on (enc=, node=, ordMC=) constructor but this
        # is how it will appear in INI file for [InterleaveGroups] section.
        self.mediaControllers = MCCIDlist(
            [ '%d:%d:%d' % (enc, node, ordMC) for ordMC in range(4) ],
            module_size_books
        )

        # Finish the JSON spoof, do a partial here and complete it in caller
        for mc in self.mediaControllers:
            mc.coordinate = 'enclosure/%d/node/%d/memory_board/1/media_controller/%d' % (
                mc.enc, mc.node, mc.ordMC + 1)

    @property
    def physloc(self):
        return '%(rack)d:%(enc)d:%(node)d' % self.__dict__

    def __str__(self):
        return 'node_id %2d: %-6s %s' % (self.node_id, self.physloc,
        self.mediaControllers)

    def __repr__(self):
        return self.__str__()

#--------------------------------------------------------------------------
# Interleave groups, using the abbreviation from the chipset ERS.


class FRDintlv_group(object):

    def __init__(self, groupId, mediaControllers):
        assert 0 <= groupId < 128, 'intlv_group ID out of range 0-127'
        self.groupId = groupId
        self.mediaControllers = mediaControllers
        # Real machine hardware only has 13 bits of book number in an LZA
        assert self.total_books <= 8192, 'book count too large'

    def __str__(self):
        return '%-3s %s' % (self.groupId, self.mediaControllers)

    def __repr__(self):
        return self.__str__()

    @property
    def total_books(self):
        '''Supersedes ig_gap calculations for flat-space emulations'''
        total_books = 0
        for mc in self.mediaControllers:
            total_books += mc.module_size_books
        return total_books
