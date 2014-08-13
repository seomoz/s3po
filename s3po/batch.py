'''Batching with gevent'''

import sys
if 'threading' in sys.modules:
    del sys.modules['threading']
from gevent import monkey
monkey.patch_all()
from gevent.pool import Pool


class Proxy(object):
    '''A proxy that will run a function on a new connection in a gevent pool'''
    def __init__(self, pool, func):
        self.pool = pool
        self.func = func
        self._greenlet = None

    def run(self, *args, **kwargs):
        '''Invoke our function with arguments'''
        callback = kwargs.pop('callback', None)
        if callback:
            return callback(self.func(*args, **kwargs))
        return self.func(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        self._greenlet = self.pool.spawn(self.run, *args, **kwargs)
        return self._greenlet

    def __getattr__(self, attr):
        return getattr(self._greenlet, attr)


class Batch(object):
    '''For uploading batches of objects in parallel. Implements the same
    interface as Connection, but just does it all in parallel. Can be used as a
    ContextManager'''

    def __init__(self, connection, poolsize):
        # Save a copy of connection, and the pool size
        self.conn = connection
        self.pool = Pool(poolsize)
        self.proxies = []

    def __getattr__(self, attr):
        # Return a proxy object that will perform the same action in a pool
        proxy = Proxy(self.pool, getattr(self.conn, attr))
        self.proxies.append(proxy)
        return proxy

    def __enter__(self):
        return self

    def __exit__(self, typ, val, trace):
        # Wait for all the jobs in the pool to complete
        self.wait()
        if typ:   # pragma: no cover
            raise typ, val, trace

    def wait(self):
        '''Wait until all our jobs are done'''
        self.pool.join()

    def success(self):
        '''Return whether or not everything finished successfully'''
        return not any(not p.successful() for p in self.proxies)

    def results(self):
        '''Get the results of each request, in original order'''
        return list(p.value for p in self.proxies)
