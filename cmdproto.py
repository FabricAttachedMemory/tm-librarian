'''Define the set of commands exchanged between a client and server
   to perform Librarian functions.  This module is imported into
   everything else and should be considered authoritative.  Supply
   a method to construct a valid command dictionary from a variety
   of input parameter conventions.'''

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

import sys
import threading
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
            parms=('path', 'mode'),
        ),
        'get_shelf': GO(
            doc='get shelf details by shelf name',
            parms=('path', ),
        ),
        'list_shelf_books': GO(
            doc='list books on a shelf',
            parms=('path', ),
        ),
        'list_shelves': GO(
            doc='list shelf names',
            parms=('path', ),
        ),
        'list_open_shelves': GO(
            doc='show all open shelves',
            parms=None,
        ),
        'open_shelf': GO(
            doc='open shelf and setup node access',
            parms=('path', )
        ),
        'resize_shelf': GO(
            doc='resize shelf to given size in bytes',
            parms=('path', 'id', 'size_bytes', 'zero_enabled'),
        ),
        'rename_shelf': GO(
            doc='rename shelf',
            parms=('path', 'id', 'newpath' ),
        ),
        'close_shelf': GO(
            doc='close shelf and end node access',
            parms=('id', 'open_handle'),
        ),
        'destroy_shelf': GO(
            doc='destroy shelf and free reserved books',
            parms=('path', ),
        ),
        'get_book': GO(
            doc='get book details by book id',
            parms=('id', ),
        ),
        'get_xattr': GO(
            doc='get extended attribute for a shelf',
            parms=('path', 'xattr'),
        ),
        'list_xattrs': GO(
            doc='get current extended attribute names for a shelf',
            parms=('path',),
        ),
        'set_xattr': GO(
            doc='set extended attribute for a shelf',
            parms=('path', 'xattr', 'value'),
        ),
        'remove_xattr': GO(
            doc='remove an extended attribute for a shelf',
            parms=('path', 'xattr'),
        ),
        'set_am_time': GO(
            doc='set access/modified times on a shelf',
            parms=('path', 'atime', 'mtime'),
        ),
        'send_OOB': GO(
            doc='send an out-of-band message to all connected clients',
            parms=('msg', ),
        ),
        'get_book_all': GO(
            doc='get all books in database sorted by LZA',
            parms=None,
        ),
        'mkdir': GO(
            doc='create new directory',
            parms=('path', 'mode'),
        ),
        'rmdir': GO(
            doc='remove empty directory',
            parms=('path', ),
        ),
        'get_shelf_path': GO(
            doc='retrieve shelf path from name and parent_id',
            parms=('name', 'parent_id'),
        ),
        'symlink': GO(
            doc='create symbolic link at path, pointing to file target',
            parms=('path', 'target'),
        ),
        'readlink': GO(
            doc='find path to actual file through a symbolic link',
            parms=('path', ),
        ),
        'update_node_soc_status': GO(
            doc='update the status and heartbeat of an SOC on a given node',
            parms=('status', 'cpu_percent', 'rootfs_percent', 'network_in',
                   'network_out', 'mem_percent'),
        ),
        'update_node_mc_status': GO(
            doc='update the status for each media controller on a given node',
            parms=('status',),
        ),

        # repl_client and demos only

        'kill_zombie_books': GO(
            doc='Clean up zombie books, returning them to the FREE state ',
            parms=None,
        ),

        'get_book_ig': GO(
            doc='get all books in an interleave group',
            parms=('intlv_group', ),
        ),

        'get_book_info_all': GO(
            doc='get all books for an interleave group joined with shelf information',
            parms=('intlv_group', ),
        ),

    }   # _commands

    # "Context" title is from FuSE.  In the C FuSE libary, the context is
    # (uid, gid, pid, umask, private_data) where private_data is the return
    # from the FuSE "init" call (NOT Python's __init__).  For the librarian,
    # add node_id to that list.

    def __init__(self, context):
        self._context = context
        self._context['seq'] = 0
        self._seq_lock = threading.Lock()

    def __call__(self, command, *args, **kwargs):
        '''Accept additional parameters as positional args, keywords,
           or a dictionary.'''
        go = self._commands[command]    # natural keyerror is fine here

        assert not (args and kwargs), 'Pos/keyword args are mutually exclusive'
        self._seq_lock.acquire()
        self._context['seq'] += 1
        self._seq_lock.release()
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
