""" Module to ease the repeated applying of an arbitrary number of functions to
object.  Useful when doing conversion betwix things when you don't want to
chain the API """

class BadChainForward(Exception):
    pass

class BadChainReverse(Exception):
    pass

class Chain(object):
    """ A list of objects representing functions to be applied in order """

    def __init__(self):
        self.fwdlinks = []
        self.revlinks = []

    def append(self, newlink):
        """ Add a link to the end of the forward chain

        Args:
            link: the Link object to be added to the chain
        """
        assert isinstance(newlink, Link), 'New item is not of type "Link"'
        self.fwdlinks.append(newlink)
        self.revlinks.insert(0, newlink)

    def forward_traverse(self, in_obj):
        """ Traverse the chain forward (in "append" order) calling the
            forward() method of each successive Link of the chain
            startig with in_obj.

        Args:
            in_obj: Object to be converted. Will be passed in order to the
            apply() functions in the chain.

        Returns:
            The same object as the most recently added Link's apply() function
            returns.

        """
        result = in_obj
        try:
            for i, link in enumerate(self.fwdlinks):
                result = link.forward(result)
            return result
        except Exception as e:
            raise BadChainForward('Error in link index %d' % i)

    def reverse_traverse(self, in_obj):
        """ Traverse the chain in reverse calling the reverse method of
        each Link starting with in_obj.

        Args:
            in_obj: Object to be converted. Will be passed in reverse order
            to the unapply() functions int he chain.

        Returns:
            A converted object of the type as the least recently added Link's
            unapply() function returns.
        """
        result = in_obj
        try:
            for i, link in enumerate(self.revlinks):
                result = link.reverse(result)
            return result
        except Exception as e:
            raise BadChainReverse('Error in link index %d' % i)

class Link(object):
    ''' The object representing the function in a chain (Links).  Please
    implement both functions if nothing needs tobe done fill in forward or reverse
    with pass appropriately. '''

    def forward(self, obj):
        """ Calls function implemented here to obj
        Args:
            obj: the object to be modified

        Returns:
            The modified object
        """
        raise BadChainForward('Subclass this and write your own')

    def reverse(self, obj):
        """ Calls function implemented here to obj
        Args:
            obj: the object to be modified

        Returns:
            The modified object
        """
        raise BadChainReverse('Subclass this and write your own')

class IdentityLink(Link):
    """ a link whose forward/reverse functions simply return the same object
    useful for debugging or using an API that requires a chain """

    def forward(self, obj):
        """
        Args: obj

        Returns obj
        """
        return obj

    def reverse(self, obj):
        """
        Args: obj

        Returns obj
        """
        return obj

class IdentityChain(Chain):
    """ Identity chain class, only contains an Identity_Link """

    def __init__(self):
        self.append(IdentityLink())
