#! /usr/vin/env python

from gevent import monkey; monkey.patch_all()
from gevent.pool import Pool

from boto.s3.connection import S3Connection
from . import Connection

class Proxy(object):
    '''A proxy that will run a function on a new connection in a gevent pool'''
    def __init__(self, pool, f):
        self.pool = pool
        self.f    = f
    
    def run(self, *args, **kwargs):
        cb = kwargs.pop('callback', None)
        if cb:
            return cb(self.f(*args, **kwargs))
        return self.f(*args, **kwargs)
    
    def __call__(self, *args, **kwargs):
        return self.pool.spawn(self.run, *args, **kwargs)

class Batch(object):
    '''For uploading batches of objects in parallel. Implements the same 
    interface as Connection, but just does it all in parallel. Can be used as a
    ContextManager'''
    
    def __init__(self, connection, poolsize):
        # Save a copy of connection, and the pool size
        self.connection = connection
        self.pool = Pool(poolsize)
    
    def __getattr__(self, item):
        # Return a proxy object that will perform the same action with a new 
        # connection
        conn = Connection(
            async    = False,
            delete   = self.connection.delete,
            tempdir  = self.connection.tempdir,
            provider = self.connection.conn.provider)
        return Proxy(self.pool, getattr(conn, item))
    
    def __enter__(self):
        return self
    
    def __exit__(self, t, v, tb):
        # Wait for all the jobs in the pool to complete
        self.pool.join()
    
    def wait(self):
        # Wait until all our jobs are done
        self.pool.join()