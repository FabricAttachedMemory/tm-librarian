#!/usr/bin/python3 -tt

class Chain():
    ''' A chain of objects representing functinos to be applied in order '''
    _chain = []

    def __init__(self):
        pass

    def __del__(self):
        pass

    def add_link(self, link):
        self._chain.append(link)

    def forward_traverse(self, in_obj):
        result = in_obj
        for link in self._chain:
            result = link.apply(result)
        return result

    def reverse_traverse(self, in_obj):
        result = in_obj
        for link in list(reversed(self._chain)):
            result = link.unapply(result)
        return result

class Link():
    ''' The objects representing the function in a chain (aka links).  Please
    implement both function if nothing needs tobe done fill in apply or unapply
    with pass appropriatly. '''
    def __init__(self):
        pass

    def __del__(self):
        pass

    def apply(self):
        raise NotImplementedError

    def unapply(self):
        raise NotImplementedError

class Identity_Link(Link):

    def apply(self, obj):
        return obj

    def unapply(self, obj):
        return obj

class Identity_Chain(Chain):

    def __init__(self):
        super().add_link(Identity_Link())
