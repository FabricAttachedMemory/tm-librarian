'''Define the set of commands exchanged between a client and server
   to perform Librarian functions.  This module is imported into
   everything else and should be considered authoritative.  Supply
   a method to construct a valid command dictionary from a variety
   of input parameter conventions.'''

from collections import OrderedDict
from pdb import set_trace
from pprint import pprint

from genericobj import GenericObject as GO;

class LibrarianCommandProtocol(object):
    '''__call__ will take a full tuple, kwargs, or a dict.'''

    _commands = {

        'version':  GO(
            doc='query librarian for version',
            parms=None,
        ),
        'create_shelf': GO(
            doc='create new shelf',
            parms = ('name', ),
        ),
        'list_shelf': GO(
            doc='list shelf details by shelf name',
            parms=('name', ),
        ),
        'list_shelves': GO(
            doc='list all shelf names',
            parms=None,
        ),
        'list_open_shelves': GO(
            doc='show all open shelves',
            parms=None,
        ),
        'open_shelf': GO(
            doc='open shelf and setup node access',
            parms = ('name', )
        ),
        'resize_shelf': GO(
            doc='resize shelf to given size',
            parms = ('name', 'id', 'size_bytes' ),
        ),
        'close_shelf': GO(
            doc='close shelf and tear down node access',
            parms=('id', ),
        ),
        'destroy_shelf': GO(
            doc='destroy shelf and free reserved books',
            parms=('name', ),
        ),
        'list_book': GO(
            doc='list book details by book id',
            parms=('id', ),
        ),

    }   # _commands

    # Helper routine.  It used to have more callers.
    @staticmethod
    def _emitparms(cmd, go):
        respdict = OrderedDict((('command', cmd), ))
        respdict['parms'] = go.parms
        return respdict

    def __call__(self, command, *args, **kwargs):
        '''Accept additional parameters as positional args, keywords,
           or a dictionary.'''
        go = self._commands[command]    # natural keyerror is fine here

        # stub for now
        try:
            auth = kwargs['auth']
            del kwargs['auth']
            assert isinstance(auth, dict), 'auth object is not a dictionary'
        except KeyError:    # let assertion through
            auth = None

        assert not (args and kwargs), 'Pos/keyword args are mutually exclusive'
        respdict = OrderedDict((('command', command), ))

        # Polymorphism.  Careful: passing a sring makes args[0] a tuple
        if args and isinstance(args[0], dict):
            kwargs = args[0]
            args = None

        try:
            if args:    # Gotta have them all, unless...
                if args[0] == 'help':
                    return self._emitparms(command, go)
                assert len(args) == len(go.parms), 'Argument count mismatch'
                for item in zip(go.parms, args):
                    respdict[item[0]] = item[1]
            elif kwargs:
                keys = sorted(kwargs.keys())
                assert set(keys) == set(go.parms), 'Argument field mismatch'
                for key in keys:
                    respdict[key] = kwargs[key]
            else:
                assert not len(go.parms), 'Missing parameter(s)'

            if auth is not None:
                respdict.update(auth)
            return respdict

        except AssertionError as e:
            msg = str(e)
        except Exception as e:
            msg = 'INTERNAL ERROR: ' + str(e)
        raise RuntimeError(msg)

    @property
    def commandset(self):
        return tuple(sorted(self._commands.keys()))

    @property
    def help(self):
        docs = []
        for name in self.commandset:
            go = self._commands[name]
            if go.parms is not None:
                docstr = '{}{}: {}'.format(name, go.parms, go.doc)
            else:
                docstr = '{}: {}'.format(name, go.doc)
            docs.append(docstr)
        return '\n'.join(docs)

if __name__ == '__main__':

    lcp = LibrarianCommandProtocol()

    # General assistance
    print(lcp.commandset)
    print(lcp.help)
    try:
        print(lcp['NoSuchCommand'])
    except Exception as e:
        print('Caught error on unknown command')
    print()

    # Individual command assistance
    tmp = lcp('resize_shelf', 'help')
    print('resize_shelf needs ' + str(tmp['parms']))

    # filled in by tuple, you need all the fields in the above order.
    # Used by repl_client.py
    junk = lcp('resize_shelf', 'shelf1', 31178, 27)
    pprint(junk)

    # Most (engine) code uses one of these two methods
    # filled in by keywords.
    junk = lcp('resize_shelf', size_bytes=28, name='shelf2', id=31178)
    pprint(junk)

    # filled in by dict
    junk = lcp('resize_shelf',
               { 'size_bytes': 42, 'id': 31178, 'name': 'shelf3' })
    pprint(junk)

    raise SystemExit(0)
