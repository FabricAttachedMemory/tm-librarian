class LibrarianCommandProtocol(object):

    def _CP_version(self):
        '''- query Librarian for current version'''
        return self._kw2dict()

    def _CP_list_book(self):
        '''<rowid> - list book details by book rowid'''
        return self._kw2dict(kw=('rowid', ) )

    def _CP_list_shelves(self):
        '''- list all shelf names'''
        return self._kw2dict()

    def _CP_list_open_shelves(self):
        '''- show all shelves'''
        return self._kw2dict()

    def _CP_list_shelf(self):
        '''- list shelf details by shelf name'''
        return self._kw2dict(kw=('shelf_name', ) )

    def _CP_create_shelf(self):
        '''<shelf_name> <node_id> <pid> <uid> <gid> - create new shelf'''
        return self._kw2dict(kw=( 'shelf_name',
            'node_id', 'pid', 'uid', 'gid') )

    def _CP_open_shelf(self):
        '''<shelf_name>  <res_owner> - open shelf and setup node access'''
        return self._kw2dict(kw=('shelf_name',
            'node_id', 'pid', 'uid', 'gid') )

    def _CP_resize_shelf(self):
        '''<shelf_name> <size_in_bytes> <node_id> <pid> <uid> <gid> - resize shelf to given size '''
        return self._kw2dict(kw=('shelf_name', 'size_bytes',
            'node_id', 'pid', 'uid', 'gid') )

    def _CP_close_shelf(self):
        '''<shelf_name> <res_owner> - close shelf and tear down node access'''
        return self._kw2dict(kw=('shelf_name',
            'node_id', 'pid', 'uid', 'gid') )

    def _CP_destroy_shelf(self):
        '''<shelf_name> <node_id> <pid> <uid> <gid> - destroy shelf and free reserved books'''
        return self._kw2dict(kw=('shelf_name',
            'node_id', 'pid', 'uid', 'gid') )

    def _kw2dict(self, kw=None):
        '''kw is a dict that aligns with self._values'''
        command_dict = { 'command': self._command }
        if kw is None:
            assert not self._values, 'Extraneous values %s' % self._values
        else:
            assert len(self._values) == len(kw), 'Keywords<->values mismatch'
            command_dict.update(dict(zip(kw, self._values)))
        return command_dict

    _handlers = { }

    _help = None

    def __init__(self):
        # Skip '_CP_' prefix
        tmp = dict( [ (name[4:], func)
                    for (name, func) in self.__class__.__dict__.items() if
                        name.startswith('_CP_')
                    ]
        )
        self._handlers.update(tmp)

        docs = [ name + ' ' + func.__doc__ for (name, func) in
            list(self._handlers.items()) ]
        assert None not in docs, 'Missing one or more CP docstrings'
        self.__class__._help = '\n'.join(sorted(docs))

    @property
    def help(self):
        return self._help

    @property
    def commandset(self):
        return tuple(sorted(self._handlers.keys()))

    def __call__(self, command, *args):
        self._command = command
        try:
            if args is None:
                self._values = tuple()
            else:
                assert isinstance(args[0], list), 'Supplied arg is not a list'
                self._values = args[0]
            handler = self._handlers[command]
            return handler(self)
        except KeyError as e:
            msg = 'No such command "%s"' % command
        except AssertionError as e:
            msg = str(e)
        except Exception as e:
            msg = 'Internal error: ' + str(e)
        raise RuntimeError(msg)

