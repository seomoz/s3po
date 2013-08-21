'''Batching with gevent'''

from gevent import monkey
monkey.patch_all()
from gevent.pool import Pool


class Proxy(object):
    '''A proxy that will run a function on a new connection in a gevent pool'''
    def __init__(self, pool, func):
        self.pool = pool
        self.func = func

    def run(self, *args, **kwargs):
        '''Invoke our function with arguments'''
        callback = kwargs.pop('callback', None)
        if callback:
            return callback(self.func(*args, **kwargs))
        return self.func(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self.pool.spawn(self.run, *args, **kwargs)


class Batch(object):
    '''For uploading batches of objects in parallel. Implements the same
    interface as Connection, but just does it all in parallel. Can be used as a
    ContextManager'''

    def __init__(self, connection, poolsize):
        # Save a copy of connection, and the pool size
        self.conn = connection
        self.pool = Pool(poolsize)

    def __getattr__(self, attr):
        # Return a proxy object that will perform the same action in a pool
        return Proxy(self.pool, getattr(self.conn, attr))

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
