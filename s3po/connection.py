'''Connecting to and talking to S3'''

import os
import warnings
from cStringIO import StringIO
from boto.s3.connection import S3Connection

# Internal imports
from .mock import Mock
from .util import retry
from .util import CountFile, logger
from .exceptions import UploadException, DownloadException


class Connection(object):
    '''Our connection to S3'''
    # How big must a file get before it's multiparted. Also how big the chunks
    # are that we'll read
    multipart_chunk = 50 * 1024 * 1204

    def __init__(self, *args, **kwargs):
        self.conn = S3Connection(*args, **kwargs)

    def batch(self, poolsize=20):
        from .batch import Batch
        return Batch(self, poolsize)

    def mock(self):
        '''Return a context-manager for managing our mocking'''
        return Mock(self.conn)

    def _download(self, bucket, key, fobj, retries):
        '''Download the contents of bucket/key to fobj'''
        bucket = self.conn.get_bucket(bucket)
        # Make a file that we'll write into
        fobj = CountFile(fobj)
        obj = bucket.get_key(key)
        if not obj:
            raise DownloadException('Key %s does not exist in %s' % (
                key, bucket.name))

        # Get its original location so we can go back to it if need be
        offset = fobj.tell()

        @retry(retries)
        def func():
            '''The bit that we want to retry'''
            fobj.seek(offset)
            obj.get_contents_to_file(fobj)
            # Ensure it was downloaded completely
            logger.info(
                'Downloaded %s bytes out of %s' % (fobj.count, obj.size))
            if obj.size != fobj.count:
                raise DownloadException('Downloaded only %i of %i bytes' % (
                    fobj.count, obj.size or 0))
        # With our wrapped function defined, we'll go ahead an invoke it.
        func()

    def _upload(self, bucket, key, fobj, retries, headers=None):
        '''Upload the contents of fobj to bucket/key with headers'''
        # Make our headers object
        headers = headers or {}
        bucket = self.conn.get_bucket(bucket)
        # We'll read in some data, and if the file appears small enough, we'll
        # upload it in a single go. In order for it to be a valid multipart
        # upload, it needs at least two parts, so we will make sure there are
        # at least enough for two parts before we commit to multipart
        data = fobj.read(2 * self.multipart_chunk)
        if len(data) < (2 * self.multipart_chunk):
            key = bucket.new_key(key)

            @retry(retries)
            def func():
                '''The bit that we want to retry'''
                key.set_contents_from_string(data, headers=headers)
                if key.size != len(data):
                    raise UploadException('Uploaded only %i for %i bytes' % (
                        key.size, len(data)))
                return True
            return func()
        else:
            logger.info('Multipart')
            # Otherwise, it's a large-enough file that we should multipart
            # upload it. There's a restriction that all parts of a multipart
            # upload must be at least 5MB. Therefore, we should keep uploading
            # chunks as long as the remaining data is 5MB greater than our chunk
            # size. That way we avoid the case where we have a remainder less
            # than this limit
            multi = bucket.initiate_multipart_upload(key, headers=headers)
            count = 1
            while len(data) >= (self.multipart_chunk + (5 * 1024 * 1024)):
                part = data[0:self.multipart_chunk]
                retry(
                    retries)(multi.upload_part_from_file)(StringIO(part), count)
                data = (
                    data[self.multipart_chunk:] +
                    fobj.read(self.multipart_chunk))
                count += 1
            # And finally, the last part
            multi.upload_part_from_file(StringIO(data), count)
            multi.complete_upload()
            return True

    def uploadFile(self, bucket, key, path, headers=None, compress=None,
        retries=3, async=None, delete=None, silent=False):  # pragma: no cover
        '''Upload the file at path to bucket/key'''
        warnings.warn('Deprecation: use `upload` instead of `uploadFile`',
            DeprecationWarning)
        with open(os.path.abspath(path)) as fobj:
            return self.upload(bucket, key, fobj, headers, retries)

    def uploadString(self, bucket, key, string, headers=None, compress=None,
        retries=3, async=None, silent=False):  # pragma: no cover
        '''Upload the string to bucket/key'''
        warnings.warn('Deprecation: use `upload` instead of `uploadString`',
            DeprecationWarning)
        return self.upload(bucket, key, string, headers, retries)

    def downloadFile(self, bucket, key, filename, retries=3): # pragma: no cover
        '''Download bucket/key to the provided filename'''
        warnings.warn('Deprecation: use `download` instead of `downloadFile`',
            DeprecationWarning)
        with open(filename, 'w') as fout:
            return self.download(bucket, key, fout, retries)

    def downloadString(self, bucket, key, retries=3): # pragma: no cover
        '''Download bucket/key and return the string'''
        warnings.warn('Deprecation: use `download` instead of `downloadString`',
            DeprecationWarning)
        return self.download(bucket, key, retries=retries)

    def upload(self, bucket, key, obj_or_data, headers=None, retries=3):
        '''Upload the provided string or file object to bucket/key'''
        logger.info('Uploading to %s / %s' % (bucket, key))
        if isinstance(obj_or_data, basestring):
            return self._upload(
                bucket, key, StringIO(obj_or_data), retries, headers)
        else:
            return self._upload(bucket, key, obj_or_data, retries, headers)

    def upload_file(self, bucket, key, path, headers=None, retries=3):
        '''Upload the file at path to bucket/key. This method is important for
        use in batch mode, so that the file object can be used with the right
        context management'''
        with open(os.path.abspath(path)) as fobj:
            return self.upload(bucket, key, fobj, headers, retries)

    def download(self, bucket, key, obj=None, retries=3):
        '''Download to either the object or return a string'''
        if obj:
            return self._download(bucket, key, obj, retries)
        obj = StringIO()
        self._download(bucket, key, obj, retries)
        return obj.getvalue()

    def download_file(self, bucket, key, path, retries=3, mode='w'):
        '''Download the item at bucket/key to a file at path. This method is
        important for us in batch mode so that the file object can be used with
        the right context management'''
        with open(os.path.abspath(path), mode) as fout:
            return self.download(bucket, key, fout, retries)
