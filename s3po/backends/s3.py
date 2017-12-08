'''Deal with S3.'''


from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError, BotoServerError, BotoClientError
from cStringIO import StringIO

from ..util import CountFile, retry, logger
from ..exceptions import UploadException, DownloadException, DeleteException


class S3(object):
    '''Our connection to S3'''
    # How big must a file get before it's multiparted. Also how big the chunks
    # are that we'll read
    chunk_size = 50 * 1024 * 1204
    min_chunk_size = 5 * 1024 * 1024

    def __init__(self, *args, **kwargs):
        self.conn = S3Connection(*args, **kwargs)

    def get_bucket(self, bucket):
        return self.conn.get_bucket(bucket, validate=False)

    def download(self, bucket, key, destination, retries, headers=None):
        '''Download the contents of bucket/key to destination'''
        bucket = self.get_bucket(bucket)
        # Make a file that we'll write into
        destination = CountFile(destination)
        obj = bucket.get_key(key, validate=False)

        # Get its original location so we can go back to it if need be
        offset = destination.tell()

        @retry(retries)
        def func():
            '''The bit that we want to retry'''
            destination.seek(offset)
            try:
                obj.get_contents_to_file(destination, headers=headers)
            except S3ResponseError as exc:
                raise DownloadException('Failed to download: %s' % exc.message)
            # Ensure it was downloaded completely
            logger.info(
                'Downloaded %s bytes out of %s' % (destination.count, obj.size))
            if obj.size != destination.count:
                raise DownloadException('Downloaded only %i of %i bytes' % (
                    destination.count, obj.size or 0))
        # With our wrapped function defined, we'll go ahead an invoke it.
        func()

    def upload(self, bucket, key, source, retries, headers=None):
        '''Upload the contents of source to bucket/key with headers'''
        # Make our headers object
        headers = headers or {}
        bucket = self.get_bucket(bucket)
        # We'll read in some data, and if the file appears small enough, we'll
        # upload it in a single go. In order for it to be a valid multipart
        # upload, it needs at least two parts, so we will make sure there are
        # at least enough for two parts before we commit to multipart
        data = source.read(2 * self.chunk_size)
        if len(data) < (2 * self.chunk_size):
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
            logger.info('Performing multipart upload')
            # Otherwise, it's a large-enough file that we should multipart
            # upload it. There's a restriction that all parts of a multipart
            # upload must be at least 5MB. Therefore, we should keep uploading
            # chunks as long as the remaining data is 5MB greater than our chunk
            # size. That way we avoid the case where we have a remainder less
            # than this limit
            multi = bucket.initiate_multipart_upload(key, headers=headers)
            count = 1
            while len(data) >= (self.chunk_size + self.min_chunk_size):
                logger.info('Uploading chunk %s', count)
                part = data[0:self.chunk_size]
                retry(
                    retries)(multi.upload_part_from_file)(StringIO(part), count)
                data = (
                    data[self.chunk_size:] +
                    source.read(self.chunk_size))
                count += 1
            # And finally, the last part
            multi.upload_part_from_file(StringIO(data), count)
            multi.complete_upload()
            return True

    def _list_retry(self, retries, bucket, *args, **kwargs):
        @retry(retries)
        def func():
            return bucket.list(*args, **kwargs)
        return func()

    def list(self, bucket, prefix=None, delimiter=None, retries=3, headers=None):
        '''List the bucket, possibly limiting the search with a prefix.'''
        bucket = self.conn.get_bucket(bucket)
        # consume iterator to make a list to keep parity with Swift backend
        return (key.name for key in self._list_retry(retries, bucket,
                                          prefix, delimiter, headers=headers))

    def delete(self, bucket, key, retries, headers=None):
        '''Delete bucket/key with headers'''
        # Make our headers object
        headers = headers or {}
        bucket = self.get_bucket(bucket)
        key = bucket.new_key(key)

        @retry(retries)
        def func():
            '''The bit that we want to retry'''
            try:
                key.delete(headers=headers)
            except (BotoServerError, BotoClientError) as exc:
                raise DeleteException('Failed to delete %s/%s: %s(%s)' %
                    (bucket, key, exc.__class__.__name__, exc.message))

        return func()
