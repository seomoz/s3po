'''Patching in our mocks'''

import inspect
from .connection import S3Connection


class Mock(object):
    '''Mock a connection object'''
    def __init__(self, conn):
        self._conn = conn
        self._mocked = S3Connection()
        self._original = {}

    def start(self):
        '''Mock all the elements of the connection'''
        callables = [(k, v) for k, v in inspect.getmembers(self._conn) if (
            not k.startswith('__') and callable(v)
        )]
        for key, value in callables:
            self._original[key] = value
            if hasattr(self._mocked, key):
                setattr(self._conn, key, getattr(self._mocked, key))
            else:
                # We need to mask that function
                def error(*args, **kwargs):
                    '''Raise an error about an attribute'''
                    raise NotImplementedError(
                        'mocks3 does not implement %s' % key)
                setattr(self._conn, key, error)

    def stop(self):
        '''Replace all the functions with their originals'''
        for key in self._original.keys():
            setattr(self._conn, key, self._original.pop(key))

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, typ, val, trace):
        self.stop()
        if typ:
            raise typ, val, trace
