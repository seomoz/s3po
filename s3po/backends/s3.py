'''Deal with S3.'''


import boto3
from boto3.s3.transfer import TransferConfig
from boto3.exceptions import Boto3Error
from botocore.exceptions import BotoCoreError, ClientError

from ..util import retry
from ..exceptions import DeleteException, DownloadException, UploadException


class S3(object):
    '''Our connection to S3'''
    # How big must a file get before it's multiparted.
    multipart_threshold = 5 * 1024 * 1204 * 1024
    # Size of chunks for multipart uploads
    multipart_chunk_size = 50 * 1024 * 1204

    def __init__(self, *args, **kwargs):
        self.conn = boto3.resource('s3', *args, **kwargs)

    def get_bucket(self, bucket):
        return self.conn.Bucket(bucket)

    def download(self, bucket, key, destination, retries, extra=None):
        '''Download the contents of bucket/key to destination'''
        bucket = self.get_bucket(bucket)

        key = bucket.Object(key)
        config = TransferConfig(
            multipart_threshold=self.multipart_threshold,
            multipart_chunksize=self.multipart_chunk_size,
            use_threads=False,
            num_download_attempts=retries)

        try:
            key.download_fileobj(destination, Config=config, ExtraArgs=extra)
        except (ClientError, BotoCoreError, Boto3Error) as exc:
            raise DownloadException('Failed to download s3://{}/{}: {}'.format(
                bucket, key, exc))

    def upload(self, bucket, key, source, retries, extra=None):
        '''Upload the contents of source to bucket/key'''
        bucket = self.get_bucket(bucket)

        key = bucket.Object(key)
        config = TransferConfig(
            multipart_threshold=self.multipart_threshold,
            multipart_chunksize=self.multipart_chunk_size,
            use_threads=False,
            num_download_attempts=retries)
        try:
            key.upload_fileobj(source, Config=config, ExtraArgs=extra)
            return True
        except (ClientError, BotoCoreError, Boto3Error) as ex:
            raise UploadException('Failed to upload s3://{}/{}: {}'.format(
                bucket, key, ex))

    def _list_retry(self, retries, bucket, **kwargs):
        @retry(retries)
        def func():
            return bucket.objects.filter(**kwargs)
        return func()

    def list(self, bucket, prefix=None, delimiter=None, retries=3, extra=None):
        '''List the bucket, possibly limiting the search with a prefix.'''
        bucket = self.get_bucket(bucket)
        opts = {}
        if prefix:
            opts['Prefix'] = prefix
        if delimiter:
            opts['Delimiter'] = delimiter
        if extra:
            opts['ExtraArgs'] = extra
        # consume iterator to make a list to keep parity with Swift backend
        return (
            key.key for key in self._list_retry(retries, bucket, **opts)
        )

    def delete(self, bucket, key, retries):
        '''Delete bucket/key'''
        bucket = self.get_bucket(bucket)
        key = bucket.Object(key)

        @retry(retries)
        def func():
            '''The bit that we want to retry'''
            try:
                key.delete()
            except (ClientError, BotoCoreError, Boto3Error) as exc:
                raise DeleteException(
                    'Failed to delete %s/%s: %s(%s)' %
                    (bucket, key, exc.__class__.__name__, exc))

        return func()
