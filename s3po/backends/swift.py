'''Deal with Swift.'''

import swiftclient.client
from swiftclient.exceptions import ClientException

from ..util import CountFile, retry, logger
from ..exceptions import UploadException, DownloadException, DeleteException


class Swift(object):
    '''Our connection to S3'''
    # The size of the chunk to download / upload
    chunk_size = 1024 * 1024

    def __init__(self, *args, **kwargs):
        # We explicitly disable retries so that we can manage that directly
        kwargs['retries'] = 0
        self.conn = swiftclient.client.Connection(*args, **kwargs)

    def download(self, bucket, key, fobj, retries, headers=None):
        '''Download the contents of bucket/key to fobj'''
        # Make a file that we'll write into
        fobj = CountFile(fobj)

        # Get its original location so we can go back to it if need be
        offset = fobj.tell()

        @retry(retries)
        def func():
            '''The bit that we want to retry'''
            try:
                resp_headers, response = self.conn.get_object(
                    bucket, key, resp_chunk_size=self.chunk_size, headers=headers)

                fobj.seek(offset)
                for chunk in response:
                    fobj.write(chunk)

                length = resp_headers.get('content-length')
                if not length:
                    logger.warn('No content-length provided -- cannot detect truncation.')
                elif fobj.count != int(length):
                    raise DownloadException('Downloaded only %i of %i bytes' % (
                        fobj.count, int(length)))

            except ClientException as exc:
                raise DownloadException('Key not found: ' + str(exc))

        # Invoke the download
        func()

    def upload(self, bucket, key, fobj, retries, headers=None):
        '''Upload the contents of fobj to bucket/key with headers'''
        # Make our headers object
        headers = headers or {}

        @retry(retries)
        def func():
            try:
                self.conn.put_object(
                    bucket, key, fobj, chunk_size=self.chunk_size, headers=headers)
            except ClientException:
                raise UploadException('Failed to upload %s' % key)

        func()

    def _get_container_retry(self, retries, *args, **kwargs):
        '''Wrap Swift's get_container with retries.'''
        @retry(retries)
        def func():
            return self.conn.get_container(*args, **kwargs)
        return func()

    def list(self, bucket, prefix=None, delimiter=None, retries=3,
                   headers=None, chunksize=100):
        '''List the bucket, possibly limiting the search with a prefix.'''
        listing =  self._get_container_retry(retries,
                                           bucket,
                                           prefix=prefix,
                                           delimiter=delimiter,
                                           limit=chunksize)[1]
        while listing:
            for result in listing:
                yield result['name']
            # out of results in current listing, get more starting at the end
            marker = listing[-1]['name']
            listing = self._get_container_retry(retries,
                                              bucket,
                                              prefix=prefix,
                                              delimiter=delimiter,
                                              marker=marker,
                                              limit=chunksize)[1]

    def delete(self, bucket, key, retries, headers=None):
        '''Delete bucket/key with headers'''
        # Make our headers object
        headers = headers or {}

        @retry(retries)
        def func():
            try:
                self.conn.delete_object(bucket, key, headers=headers)
            except ClientException:
                raise DeleteException('Failed to delete %s/%s' % (bucket, key))

        func()
