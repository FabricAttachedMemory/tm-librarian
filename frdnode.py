#!/usr/bin/python3 -tt

# Full Rack Demo (FRD) for June 2016
# Max of one rack of eight enclosures of ten nodes: exclude rack.
# FRD has 4T per node, or 512 books per node, or 128 books per media
# controller.  See chipset ERS 2.02 section 2.21.3.1 option 1 for CIDs.

from pdb import set_trace

#--------------------------------------------------------------------------


class FRDFAModule(object):
    '''One Media Controller (MC) and the books of NVM behind it.'''

    def __init__(self, raw=None, enc=None, node=None, ordMC=None,
                       module_size_books=0):
        self.module_size_books = module_size_books
        if raw is not None:
            '''Break apart an encoded string, or just return integer'''
            try:
                enc, node, ordMC = raw.split(':')
                enc = int(enc)
                node = int(node)
                ordMC = int(ordMC)
            except Exception as e:
                enc =  (raw >> 8) & 0x7  # 3 bits
                node = (raw >> 4) & 0xF  # 4 bits
                ordMC = raw & 0x3       # 2 LSB cuz
        else:
            assert enc is not None and node is not None and ordMC is not None

        assert 1 <= enc <= 8, 'Bad enclosure value'
        assert 1 <= node <= 10, 'Bad node value'
        assert 0 <= ordMC <= 3, 'Bad ordMC value'
        self.enc = enc
        self.node = node
        self.ordMC = ordMC
        self.value = enc << 8 | node << 4 | (8 + ordMC)

    def __str__(self):
        return '%d:%d:%d' % (self.enc, self.node, self.ordMC)

    def __sub__(self, other):
        '''Return the NGMI hop count between two MCs'''
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
            self.MCs = [ ]
            return
        assert 1 <= len(rawCIDlist) <= 8, 'IG element count out of range 1-8'
        self.MCs = [ FRDFAModule(raw=c, module_size_books=module_size_books)
            for c in rawCIDlist ]

    def __repr__(self):
        return str([ '0x%x' % cid.value for cid in self.MCs ])

    def __str__(self):
        tmp = [ str(cid) for cid in self.MCs ]
        return str(tmp)

    # Duck-type a list

    def __getitem__(self, index):
        return self.MCs[index]

    def __iter__(self):
        return iter(self.MCs)

    def append(self, newFAModule):
        assert len(self.MCs) < 8, 'Max length exceeded'
        self.MCs.append(newFAModule)

#--------------------------------------------------------------------------


class FRDnode(object):

    def __init__(self, node, enc=None, MAC=None, module_size_books=0,
                             autoMCs=True):
        if enc is None: # old school, node is enumerator 1-80
            assert 1 <= node <= 80, 'Bad node enumeration value'
            n = node - 1   # modulus math
            node = (n % 10) + 1
            enc = ((n % 80) // 10) + 1
            rack = (n // 80) + 1
        else:
            assert 1 <= enc <= 8, 'Bad enclosure value'
            assert 1 <= node <= 10, 'Bad node value'
            rack = 1
        self.node = node
        self.enc = enc
        self.rack = rack
        self.MAC = MAC
        if not autoMCs: # Done later, probably custom module_size_books
            self.MCs = []
            return

        # Could also loop on (enc=, node=, ordMC=) constructor but this
        # is how it will appear in INI file for [InterleaveGroups] section.
        self.MCs = MCCIDlist(
            [ '%d:%d:%d' % (enc, node, ordMC) for ordMC in range(4) ],
            module_size_books
        )

    def __str__(self):
        physloc = '%(rack)d:%(enc)d:%(node)d' % self.__dict__
        return '%-6s %s' % (physloc, self.MCs)

    def __eq__(self, other):
        return (self.rack == other.rack and
                self.enc == other.enc and
                self.node == other.node)

#--------------------------------------------------------------------------


class FRDIG(object):

    def __init__(self, num, MCs):
        assert 0 <= num < 128, 'IGnum out of range 0-127'
        self.num = num
        self.MCs = MCs

#--------------------------------------------------------------------------
# Match the automatic extrapolation mode of book_register.py, working
# only from a node count (80)

if __name__ == '__main__':
    MSB = 128
    FRDnodes = [ FRDnode(n + 1, module_size_books=MSB) for n in range(80) ]
    IGs = [ FRDIG(i + 1, node.MCs) for i, node in enumerate(FRDnodes) ]
    assert IGs[15].MCs[2].module_size_books == MSB
    assert IGs[15].MCs[2] - IGs[15].MCs[3] == 1
    assert IGs[15].MCs[2] - IGs[16].MCs[3] == 3
    assert IGs[15].MCs[2] - IGs[26].MCs[3] == 5
    set_trace()
    pass
