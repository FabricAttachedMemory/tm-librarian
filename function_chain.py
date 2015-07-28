""" Module to ease the repeated applying of an arbitrary number of functions to
object.  Useful when doing conversion betwix things when you don't want to
chain the API """

class Chain():
    """ A chain of objects representing functions to be applied in order """
    _chain = []

    def __init__(self):
        pass

    def __del__(self):
        pass

    def add_link(self, link):
        """ Add a link to the chain

        Args:
            link: the Link object to be added to the chain
        """
        self._chain.append(link)

    def forward_traverse(self, in_obj):
        """ Traverse the chain forward this calls the apply function of
        each successive Link of the chain on in_obj.

        Args:
            in_obj: Object to be converted. Will be passed in order to the
            apply() functions in the chain.

        Returns:
            The same object as the most recently added Link's apply() function
            returns.

        """
        result = in_obj
        for link in self._chain:
            result = link.apply(result)
        return result

    def reverse_traverse(self, in_obj):
        """ Traverse the chain in reverse this calls the unapply function of
        each successive Link of the chain on in_obj.

        Args:
            in_obj: Object to be converted. Will be passed in reverse order
            to the unapply() functions int he chain.

        Returns:
            A converted object of the type as the least recently added Link's
            unapply() function returns.
        """
        result = in_obj
        for link in list(reversed(self._chain)):
            result = link.unapply(result)
        return result

class Link():
    ''' The object representing the function in a chain (Links).  Please
    implement both functions if nothing needs tobe done fill in apply or unapply
    with pass appropriately. '''
    def __init__(self):
        pass

    def __del__(self):
        pass

    def apply(self, obj):
        """ Calls function implemented here to obj
        Args:
            obj: the object to be modified

        Returns:
            The modified object
        """
        raise NotImplementedError

    def unapply(self, obj):
        """ Calls function implemented here to obj
        Args:
            obj: the object to be modified

        Returns:
            The modified object
        """
        raise NotImplementedError

class IdentityLink(Link):
    """ a link whose apply and unapply functions simply return the same object
    useful for debugging or using an API that requires a chain """

    def apply(self, obj):
        """
        Args: obj

        Returns obj
        """
        return obj

    def unapply(self, obj):
        """
        Args: obj

        Returns obj
        """
        return obj

class IdentityChain(Chain):
    """ Identity chain class, only contains an Identity_Link """

    def __init__(self):
        super().__init__()
        super().add_link(IdentityLink())
