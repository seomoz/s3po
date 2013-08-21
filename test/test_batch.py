'''Test all of our batching utilities'''

import unittest

from s3po.connection import Connection
from s3po.exceptions import DownloadException


class BatchTest(unittest.TestCase):
    '''We should be able to batch some requests out'''
    def setUp(self):
        self.s3po = Connection()
        self.mock = self.s3po.mock()
        self.mock.start()
        self.s3po.conn.create_bucket('s3po')

    def tearDown(self):
        self.mock.stop()

    def test_upload(self):
        '''We should be able to upload'''
        names = ['foo', 'bar', 'baz', 'whiz']
        with self.s3po.batch() as batch:
            for name in names:
                batch.upload('s3po', name, 'foo')
                self.assertRaises(
                    DownloadException, self.s3po.download, 's3po', name)
        for name in names:
            self.assertEqual(self.s3po.download('s3po', name), 'foo')

    def test_callback(self):
        '''We can optionally provide a callback'''
        names = ['foo', 'bar', 'baz', 'whiz']
        with self.s3po.batch() as batch:
            for name in names:
                batch.upload('s3po', name, name)

        results = []
        with self.s3po.batch() as batch:
            for name in names:
                batch.download(
                    's3po', name, callback=lambda r: results.append(r))
        self.assertEqual(set(results), set(names))
