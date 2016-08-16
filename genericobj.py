"""Provide object<-->dict flexibility, along with a __str__ that's
   really useful in debugging.  Probably also workable as a mixin.
   __init__ is left as an exercise to the reader, keep reading."""

from pdb import set_trace


class objstr(object):

    def __str__(self):
        s = [ ]
        for k in sorted(self.__dict__.keys()):
            v = self.__dict__[k]
            if isinstance(v, dict):
                s.append("%s{len=%d}" % (k, len(v)))
            elif isinstance(v, list):
                s.append("%s[len=%d]" % (k, len(v)))
            elif isinstance(v, tuple):
                s.append("%s(len=%d)" % (k, len(v)))
            elif isinstance(v, int):
                s.append("%s=%d" % (k, v))
            elif isinstance(v, float):
                s.append("%s=%.3f" % (k, v))
            elif isinstance(v, str):
                s.append("%s='%s'" % (k, v))
            elif isinstance(v, objstr):     # Avoid infinite loop to here
                s.append("%s=%s" % (k, type(v)))
            else:
                s.append("%s='%s'" % (k, v))
        return "; ".join(s)

    def __repr__(self):
        return str(self)

    def __len__(self):  # fields that were added by setattr
        return len(self.__dict__)

    def __getitem__(self, key):
        """Duck typing, now I'm a dict, useful for string convolution.
           Properties fail the __dict__ lookup but they're more common."""
        try:
            return self.__dict__[key]
        except KeyError as e:
            return getattr(self, e[0])      # let an error here bubble up

    def get(self, key, default=None):
        return self.__dict__.get(key, default)

    def __setitem__(self, key, value):      # Symmetry
        self.__dict__[key] = value

    def __eq__notyet(self, other):
        for k, v in self.__dict__.iteritems():
            if other[k] != v:
                return False
            return True

    def __ne__notright(self, other):    # yin and yang
        for k, v in self.iteritems():
            if other[k] == v:
                return True
            return False

    # for (re)conversion to send back across the wire
    @property
    def dict(self):
        return self.__dict__

###########################################################################
# If you just want an object.


class GenericObject(objstr):
    """Takes a dict and/or a set of kwargs, create an object."""

    def __init__(self, asdict=None, **kwargs):
        if asdict is not None:
            self.__dict__.update(asdict)
        self.__dict__.update(kwargs)
