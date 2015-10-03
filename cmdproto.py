'''Define the set of commands exchanged between a client and server
   to perform Librarian functions.  This module is imported into
   everything else and should be considered authoritative.  Supply
   a method to construct a valid command dictionary from a variety
   of input parameter conventions.'''

import sys
from collections import OrderedDict
from pdb import set_trace
from pprint import pprint

from genericobj import GenericObject as GO


class LibrarianCommandProtocol(object):
    '''__call__ will take a full tuple, kwargs, or a dict.'''

    _commands = {

        'version':  GO(
            doc='query librarian for version',
            parms=None,
        ),
        'get_fs_stats':  GO(
            doc='query global FS stats',
            parms=None,
        ),
        'create_shelf': GO(
            doc='create new shelf',
            parms=('name', ),
        ),
        'get_shelf': GO(
            doc='get shelf details by shelf name',
            parms=('name', ),
        ),
        'list_shelf_books': GO(
            doc='list books on a shelf',
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
            parms=('name', )
        ),
        'resize_shelf': GO(
            doc='resize shelf to given size',
            parms=('name', 'id', 'size_bytes' ),
        ),
        'close_shelf': GO(
            doc='close shelf and end node access',
            parms=('id', 'open_handle'),
        ),
        'destroy_shelf': GO(
            doc='destroy shelf and free reserved books',
            parms=('name', ),
        ),
        'get_book': GO(
            doc='get book details by book id',
            parms=('id', ),
        ),
        'get_xattr': GO(
            doc='get extended attribute for a shelf',
            parms=('name', 'xattr'),
        ),
        'list_xattrs': GO(
            doc='get current extended attribute names for a shelf',
            parms=('name',),
        ),
        'set_xattr': GO(
            doc='set extended attribute for a shelf',
            parms=('name', 'xattr', 'value'),
        ),
        'remove_xattr': GO(
            doc='remove an extended attribute for a shelf',
            parms=('name', 'xattr'),
        ),
        'set_am_time': GO(
            doc='set access/modified times on a shelf',
            parms=('name', 'atime', 'mtime'),
        ),
        'send_OOB': GO(
            doc='send an out-of-band message to all connected clients',
            parms=('msg', ),
        ),
        'get_book_all': GO(
            doc='get all books in database sorted by LZA',
            parms=None,
        ),

        # repl_client and demos only

        'kill_zombie_books': GO(
            doc='Clean up zombie books, returning them to the FREE state ',
            parms=None,
         ),

        'get_book_alloc': GO(
            doc='Return allocation state of each book in a node',
            parms=('node_id', ),
         ),

    }   # _commands

    # "Context" title is from FuSE.  In the C FuSE libary, the context is
    # (uid, gid, pid, umask, private_data) where private_data is the return
    # from the FuSE "init" call (NOT Python's __init__).  For the librarian,
    # add node_id to that list.

    def __init__(self, context):
        self._context = context
        self._context['seq'] = 0

    def __call__(self, command, *args, **kwargs):
        '''Accept additional parameters as positional args, keywords,
           or a dictionary.'''
        go = self._commands[command]    # natural keyerror is fine here

        assert not (args and kwargs), 'Pos/keyword args are mutually exclusive'
        self._context['seq'] += 1
        respdict = OrderedDict((
            ('command', command),
            ('context', self._context),
        ))

        # Polymorphism.  Careful: passing a sring makes args[0] a tuple
        if args and isinstance(args[0], dict):
            kwargs = args[0]
            args = None

        try:
            if args:    # Gotta have them all, unless...
                arg0 = args[0]
                if str(arg0) == 'help':
                    respdict = OrderedDict((('command', command), ))
                    respdict['parms'] = go.parms
                    return respdict

                # It might be an object.  Try duck typing first.
                try:
                    for p in go.parms:
                        respdict[p] = getattr(arg0, p)
                except AttributeError as e:
                    assert len(args) == len(go.parms), 'Arg count mismatch'
                    for item in zip(go.parms, args):
                        respdict[item[0]] = item[1]
            elif kwargs:
                keys = sorted(kwargs.keys())
                assert set(keys) == set(go.parms), 'Arg field mismatch'
                for key in keys:
                    respdict[key] = kwargs[key]
            else:
                assert go.parms is None, 'Missing parameter(s)'

            return respdict

        except AssertionError as e:
            msg = str(e)
        except Exception as e:
            msg = 'INTERNAL ERROR @ %s[%d]: %s' % (
                self.__class__.__name__, sys.exc_info()[2].tb_lineno, str(e))
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

    from book_shelf_bos import TMShelf

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

    # high-level
    shelf = TMShelf(name='shelf4')
    shelf.size_bytes = 42
    junk = lcp('resize_shelf', shelf)
    pprint(junk)

    raise SystemExit(0)
