'''Test our Connection'''

import os
import shutil
import unittest
from cStringIO import StringIO

from s3po import Connection
from s3po.exceptions import UploadException, DownloadException


class ConnectionTest(unittest.TestCase):
    '''Test our connection's functionality'''
    tmpdir = 'test/tmp'

    def setUp(self):
        self.s3po = Connection()
        self.mock = self.s3po.mock()
        self.mock.start()
        self.s3po.conn.create_bucket('s3po')
        if not os.path.exists(self.tmpdir):
            os.mkdir(self.tmpdir)
        self.assertEqual(os.listdir(self.tmpdir), [])

    def tearDown(self):
        self.mock.stop()
        for path in os.listdir(self.tmpdir):
            path = os.path.join(self.tmpdir, path)
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)

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

    def test_download_filename(self):
        '''We should be able to download to a file'''
        path = os.path.join(self.tmpdir, 'foo')
        self.s3po.upload('s3po', 'foo', 'hello')
        self.s3po.download_file('s3po', 'foo', path)
        with open(path) as fin:
            self.assertEqual(fin.read(), 'hello')

    def test_upload_filename(self):
        '''We should be able to upload a file'''
        path = os.path.join(self.tmpdir, 'foo')
        with open(path, 'w+') as fout:
            fout.write('hello')
        self.s3po.upload_file('s3po', 'foo', path)
        self.assertEqual(self.s3po.download('s3po', 'foo'), 'hello')
