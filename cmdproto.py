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
        'resize_shelf': GO(
            doc='resize shelf to given size',
            parms = ('name', 'size_bytes' ),
        ),
        'open_shelf': GO(
            doc='open shelf and setup node access',
            parms = ('name', )
        ),
        'list_shelves': GO(
            doc='list all shelf names',
            parms=None,
        ),
        'list_open_shelves': GO(
            doc='show all open shelves',
            parms=None,
        ),
        'list_shelf': GO(
            doc='list shelf details by shelf name',
            parms=('name', ),
        ),
        'close_shelf': GO(
            doc='close shelf and tear down node access',
            parms=('name', ),
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

    # Helper routine
    @staticmethod
    def _nulldict(cmd, go):
        respdict = OrderedDict((('command', cmd), ))
        for p in go.parms:
            respdict[p] = None
        return respdict

    def __getitem__(self, item):
        go = self._commands[item]   # native KeyError is fine
        return self._nulldict(item, go)

    def __getattr__(self, attr):
        try:
            go = self._commands[attr]
            return self._nulldict(attr, go)
        except KeyError as e:
            raise AttributeError(attr)

    def __call__(self, command, *args, **kwargs):
        go = self._commands[command]    # natural keyerror is fine
        assert not (args and kwargs), 'Pos/keyword args are mutually exclusive'
        respdict = OrderedDict((('command', command), ))

        # Polymorphism.  Careful: passing a sring makes args[0] a tuple
        if args and isinstance(args[0], dict):
            kwargs = args[0]
            args = None
        try:
            if args:
                assert len(args) == len(go.parms), 'Argument count mismatch'
                for item in zip(go.parms, args):
                    respdict[item[0]] = item[1]
            if kwargs:
                keys = sorted(kwargs.keys())
                assert set(keys) == set(go.parms), 'Argument field mismatch'
                for key in keys:
                    respdict[key] = kwargs[key]
            return respdict
        except AssertionError as e:
            msg = str(e)
        except Exception as e:
            msg = 'Internal error: ' + str(e)
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

    print(lcp.help)

    # two forms of null or template responses
    pprint(lcp.resize_shelf)
    pprint(lcp['resize_shelf'])

    # filled in by tuple
    junk = lcp('resize_shelf', 'shelf1', 42)
    pprint(junk)

    # filled in by keywords
    junk = lcp('resize_shelf', size_bytes=84, name='shelf2')
    pprint(junk)

    # filled in by dict
    junk = lcp('resize_shelf', { 'size_bytes': 27, 'name': 'shelf3' })
    pprint(junk)

    raise SystemExit(0)
