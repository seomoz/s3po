'''Test our Connection'''

import unittest
from cStringIO import StringIO

from s3po import Connection
from s3po.exceptions import UploadException, DownloadException


class ConnectionTest(unittest.TestCase):
    '''Test our connection's functionality'''
    def setUp(self):
        self.s3po = Connection()
        self.mock = self.s3po.mock()
        self.mock.start()
        self.s3po.conn.create_bucket('s3po')

    def tearDown(self):
        self.mock.stop()

    def test_upload(self):
        '''We should be able to upload a small key'''
        self.assertRaises(DownloadException, self.s3po.download, 's3po', 'foo')
        self.s3po.upload('s3po', 'foo', 'hello')
        self.assertEqual(self.s3po.download('s3po', 'foo'), 'hello')

    def test_multipart(self):
        '''If the key is large enough, we should do this as multipart'''
        self.s3po.multipart_chunk = 5 * 1024 * 1024
        data = 'hello' * self.s3po.multipart_chunk
        self.s3po.upload('s3po', 'foo', data)
        self.assertEqual(len(self.s3po.download('s3po', 'foo')), len(data))

    def test_short_download(self):
        '''If there is too little downloaded, throws an error'''
        self.s3po.upload('s3po', 'foo', 'bar')
        self.s3po.conn.get_bucket('s3po').get_key('foo')._size = 5
        self.assertRaises(DownloadException, self.s3po.download, 's3po', 'foo',
            retries=0)

    def test_short_upload(self):
        '''If we uploaded too little data, throws an error'''
        self.s3po.upload('s3po', 'foo', 'bar')
        self.s3po.conn.get_bucket('s3po').get_key('foo')._size = 5
        self.assertRaises(
            UploadException, self.s3po.upload, 's3po', 'foo', 'bar', retries=0)

    def test_upload_file(self):
        '''We should be able to upload a file'''
        self.s3po.upload('s3po', 'foo', StringIO('hello'))
        self.assertEqual(self.s3po.download('s3po', 'foo'), 'hello')

    def test_download_file(self):
        '''We should be able to download to a file'''
        self.s3po.upload('s3po', 'foo', 'hello')
        obj = StringIO()
        self.s3po.download('s3po', 'foo', obj)
        self.assertEqual(obj.getvalue(), 'hello')
