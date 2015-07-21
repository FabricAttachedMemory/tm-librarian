class LibrarianCommandProtocol(object):

    def _CP_version(self):
        '''- query Librarian for current version'''
        return self._kw2dict()

    def _CP_book_list(self):
        '''<rowid> - list book details by book rowid'''
        return self._kw2dict(kw=('rowid', ) )

    def _CP_shelf_list(self):
        '''- list shelf details by shelf name'''
        return self._kw2dict(kw=('shelf_name', ) )

    def _CP_shelf_listall(self):
        '''- list shelf details for all shelves in database'''
        self._command = 'shelf_reservation_list'
        return self._kw2dict()

    def _CP_shelf_create(self):
        '''<shelf_name> <shelf_owner> - create new shelf'''
        return self._kw2dict(kw=('shelf_name', 'shelf_owner') )

    def _CP_shelf_resize(self):
        '''<shelf_name> <size_in_bytes> - resize shelf to given size '''
        return self._kw2dict(kw=('shelf_name', 'size_bytes') )

    def _CP_shelf_destroy(self):
        '''<shelf_name> - destroy shelf and free reserved books'''
        return self._kw2dict(kw=('shelf_name', ) )

    def _CP_shelf_open(self):
        '''<shelf_name>  <res_owner> - open shelf and setup node access'''
        return self._kw2dict(kw=('shelf_name', 'res_owner') )

    def _CP_shelf_close(self):
        '''<shelf_name> <res_owner> - close shelf and tear down node access'''
        return self._kw2dict(kw=('shelf_name', 'res_owner') )

    def _CP_shelf_reservation_list(self):
        '''- show all shelves'''
        return self._kw2dict()

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

