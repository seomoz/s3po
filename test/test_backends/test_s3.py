'''Talk to S3'''

from cStringIO import StringIO

import mock
from collections import namedtuple

from base import BaseTest

from s3po.backends.s3 import S3
from s3po.exceptions import UploadException, DownloadException


class S3BackendTest(BaseTest):
    '''We can talk to S3 as expected.'''

    def setUp(self):
        BaseTest.setUp(self)
        self.bucket = Bucket('bucket')
        self.backend = S3(aws_access_key_id='not', aws_secret_access_key='a real key')
        mock.patch.object(self.backend, 'get_bucket', return_value=self.bucket).start()

    def test_round_trip(self):
        '''Can round-trip an object.'''
        result = StringIO()
        self.backend.upload('bucket', 'key', StringIO('content'), 1)
        self.backend.download('bucket', 'key', result, 1)
        self.assertEqual(result.getvalue(), 'content')

    def test_download_missing(self):
        '''Downloading a missing object gives us failure.'''
        self.assertRaises(
            DownloadException, self.backend.download, 'bucket', 'key', StringIO(), 1)

    def test_download_size_mismatch(self):
        '''If we've downloaded less than expected, throws an exception.'''
        self.backend.upload('bucket', 'key', StringIO('content'), 1)
        key = self.bucket.get_key('key')
        with mock.patch.object(key, 'get_contents_to_file'):
            self.assertRaises(
                DownloadException, self.backend.download, 'bucket', 'key', StringIO(), 1)

    def test_upload_partial(self):
        '''Throws an exception if we only partially upload'''
        key = self.bucket.new_key('key')
        with mock.patch.object(key, 'set_contents_from_string'):
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
        self.bucket.new_key('key')
        with mock.patch.object(self.backend.conn, 'get_bucket', return_value=self.bucket):
            self.assertEqual(self.backend.list('bucket'),
                             ['key'])

    def test_list_prefix(self):
        '''Can list a bucket limited by prefix'''
        self.bucket.new_key('key')
        self.bucket.new_key('starts_with_something_else')
        with mock.patch.object(self.backend.conn, 'get_bucket', return_value=self.bucket):
            self.assertEqual(self.backend.list('bucket', prefix='k'),
                             ['key'])


class Bucket(object):
    '''A mock bucket.'''

    def __init__(self, name):
        self.name = name
        self.keys = {}

    def get_key(self, key):
        return self.keys.get(key)

    def new_key(self, key):
        if key not in self.keys:
            self.keys[key] = Key()
        return self.keys[key]

    def list(self, prefix, delimiter, headers=None):
        prefix = prefix or ''
        Key = namedtuple('Key', ['name'])
        return [Key(key) for key in self.keys if key.startswith(prefix)]

    def initiate_multipart_upload(self, key, headers=None):
        return Multi(self, key, headers)


class Key(object):
    '''S3 key'''
    def __init__(self):
        self.data = ''
        self.headers = {}

    @property
    def size(self):
        return len(self.data)

    def set_contents_from_string(self, data, headers=None):
        self.data = data
        self.header = headers or dict()

    def get_contents_to_file(self, fobj, headers=None):
        fobj.write(self.data)


class Multi(object):
    '''Multipart upload.'''
    def __init__(self, bucket, key, headers=None):
        self.bucket = bucket
        self.key = key
        self.headers = headers or {}
        self.parts = {}

    def upload_part_from_file(self, fobj, count):
        self.parts[count] = fobj.read()

    def complete_upload(self):
        # Join data together in sorted order. And yes, we can skip indexes
        data = ''.join(v for _, v in sorted(self.parts.items()))
        self.bucket.new_key(self.key).set_contents_from_string(data, self.headers)
