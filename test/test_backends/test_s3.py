'''Talk to S3'''

from six import StringIO
from botocore.exceptions import BotoCoreError, ClientError

import mock

from test.base import BaseTest

from s3po.backends.s3 import S3
from s3po.exceptions import UploadException, DownloadException, DeleteException


class S3BackendTest(BaseTest):
    '''We can talk to S3 as expected.'''

    def setUp(self):
        BaseTest.setUp(self)
        self.bucket = Bucket('bucket')
        self.backend = S3(
            aws_access_key_id='not', aws_secret_access_key='a real key')
        self.bucket_patcher = mock.patch.object(
            self.backend.conn, 'Bucket', return_value=self.bucket)
        self.bucket_patcher.start()

    def tearDown(self):
        self.bucket_patcher.stop()

    def test_round_trip(self):
        '''Can round-trip an object.'''
        result = StringIO()
        self.backend.upload('bucket', 'key', StringIO('content'), 1)
        self.backend.download('bucket', 'key', result, 1)
        self.assertEqual(result.getvalue(), 'content')

    def test_download_missing(self):
        '''Downloading a missing object gives us failure.'''
        key = self.bucket.Object('key')
        error = ClientError(
            {'Error': {'Message': 'Not Found', 'Code': '404'}},
            'HeadObject'
        )
        with mock.patch.object(key, 'download_fileobj', side_effect=error):
            with self.assertRaises(DownloadException):
                self.backend.download('bucket', 'key', StringIO(), 1)

    def test_upload_exception(self):
        '''Throws an upload exception if an error occured during upload'''
        key = self.bucket.Object('key')
        error = ClientError(
            {'Error': {'Message': 'Service Unavailable', 'Code': '503'}},
            'PutObject'
        )
        with mock.patch.object(key, 'upload_fileobj', side_effect=error):
            self.assertRaises(
                UploadException,
                self.backend.upload, 'bucket', 'key', StringIO('content'), 1)

    def test_multipart(self):
        '''Can perform a multipart upload.'''
        # Make sure we trigger multipart uploads
        self.backend.chunk_size = self.backend.min_chunk_size = 10
        result = StringIO()
        data = 'some very long content ' * 100
        self.backend.upload('bucket', 'key', StringIO(data), 1)
        self.backend.download('bucket', 'key', result, 1)
        self.assertEqual(result.getvalue(), data)

    def test_list(self):
        '''Can list a bucket'''
        self.bucket.Object('abc')
        self.assertEqual(list(self.backend.list('bucket')), ['abc'])

    def test_list_prefix(self):
        '''Can list a bucket limited by prefix'''
        self.bucket.Object('abc')
        self.bucket.Object('starts_with_something_else')
        self.assertEqual(list(self.backend.list('bucket', prefix='a')),
                         ['abc'])

    def test_delete(self):
        '''Can delete a key'''
        self.bucket.Object('abc')
        self.backend.delete('bucket', 'abc', 1)
        self.assertEqual(list(self.backend.list('bucket', prefix='a')), [])

    def test_deletion_error(self):
        '''Raises DeleteException on key.delete() error'''
        key = self.bucket.Object('key')
        error = BotoCoreError()
        with mock.patch.object(key, 'delete', side_effect=error):
            with self.assertRaises(DeleteException):
                self.backend.delete('bucket', 'key', 1)


class Bucket(object):
    '''A mock bucket.'''

    def __init__(self, name):
        self.name = name
        self.keys = {}

        class Filterable(object):
            '''Provides the filter method on the objects field'''
            def __init__(self, bucket):
                self.bucket = bucket

            def filter(self, Prefix=None, Delimiter=None, ExtraArgs=None):
                prefix = Prefix or ''

                class Item(object):
                    '''Provides the Object method for mock list results'''
                    def __init__(self, bucket, key):
                        self.bucket = bucket
                        self.key = key

                    def Object(self):
                        return self.bucket.Object(key)

                return [
                    Item(self.bucket, key) for key in self.bucket.keys
                    if key.startswith(prefix)
                ]

        self.objects = Filterable(self)

    def Object(self, key):
        return self._new_key(key)

    def _new_key(self, key):
        new_key = self.keys.get(key)
        if not new_key:
            new_key = self.keys[key] = Key(self, key)
        return new_key

    def delete_key(self, key):
        del self.keys[key]


class Key(object):
    '''S3 key'''
    def __init__(self, bucket, key):
        self.data = ''
        self.bucket = bucket
        self.key = key

    def download_fileobj(self, fobj, Config, ExtraArgs=None):
        fobj.write(self.data)

    def upload_fileobj(self, fobj, Config, ExtraArgs=None):
        self.data = fobj.read()

    def delete(self):
        self.bucket.delete_key(self.key)
