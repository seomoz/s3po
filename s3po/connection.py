'''Deal with object storage.'''

import contextlib
import os
from cStringIO import StringIO

# Internal imports
from .util import retry, logger
from .backends.s3 import S3
from .backends.swift import Swift
from .backends.memory import Memory


class Connection(object):
    '''Our connection to S3'''

    @classmethod
    def s3(cls, *args, **kwargs):  # pragma: no cover
        '''Create a connection using S3.'''
        return cls(S3(*args, **kwargs))

    @classmethod
    def swift(cls, *args, **kwargs):  # pragma: no cover
        '''Create a connection using Swift.'''
        return cls(Swift(*args, **kwargs))

    @classmethod
    def memory(cls):
        '''Create a connection using the in-memory backend.'''
        return cls(Memory())

    def __init__(self, backend):
        self.backend = backend

    def batch(self, poolsize=20):
        from .batch import Batch
        return Batch(self, poolsize)

    @contextlib.contextmanager
    def mock(self):
        '''Return a context-manager for managing our mocking'''
        original = self.backend
        try:
            self.backend = Memory()
            yield self
        finally:
            self.backend = original

    def upload(self, bucket, key, obj_or_data, headers=None, retries=3):
        '''Upload the provided string or file object to bucket/key'''
        logger.info('Uploading to %s / %s', bucket, key)
        if isinstance(obj_or_data, basestring):
            return self.backend.upload(
                bucket, key, StringIO(obj_or_data), retries, headers)
        else:
            return self.backend.upload(bucket, key, obj_or_data, retries, headers)

    def upload_file(self, bucket, key, path, headers=None, retries=3):
        '''Upload the file at path to bucket/key. This method is important for
        use in batch mode, so that the file object can be used with the right
        context management'''
        with open(os.path.abspath(path)) as fobj:
            return self.upload(bucket, key, fobj, headers, retries)

    def download(self, bucket, key, obj=None, headers=None, retries=3):
        '''Download to either the object or return a string'''
        logger.info('Downloading %s / %s', bucket, key)
        if obj:
            return self.backend.download(bucket, key, obj, retries, headers)
        obj = StringIO()
        self.backend.download(bucket, key, obj, retries, headers)
        return obj.getvalue()

    def download_file(self, bucket, key, path, headers=None, retries=3, mode='w'):
        '''Download the item at bucket/key to a file at path. This method is
        important for us in batch mode so that the file object can be used with
        the right context management'''
        with open(os.path.abspath(path), mode) as fout:
            return self.download(bucket, key, fout, headers, retries)

    def list(self, bucket, prefix=None, delimiter=None, retries=3, headers=None):
        '''List the contents of the bucket, optionally specifying a prefix.'''
        return self.backend.list(bucket, prefix, delimiter, retries, headers)

    def delete(self, bucket, key, headers=None, retries=3):
        '''Delete the bucket/key'''
        logger.info('Deleting %s / %s', bucket, key)
        return self.backend.delete(bucket, key, retries, headers)
