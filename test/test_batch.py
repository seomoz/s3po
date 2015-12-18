'''Test all of our batching utilities'''

import unittest

from s3po.connection import Connection
from s3po.exceptions import DownloadException


class BatchTest(unittest.TestCase):
    '''We should be able to batch some requests out'''
    def setUp(self):
        self.conn = Connection.memory()

    def test_upload(self):
        '''We should be able to upload'''
        keys = ['foo', 'bar', 'baz', 'whiz']
        with self.conn.batch() as batch:
            for key in keys:
                batch.upload('bucket', key, 'foo')

        for key in keys:
            self.assertEqual(self.conn.download('bucket', key), 'foo')

    def test_callback(self):
        '''We can optionally provide a callback'''
        keys = ['foo', 'bar', 'baz', 'whiz']
        with self.conn.batch() as batch:
            for key in keys:
                batch.upload('bucket', key, key)

        results = []
        with self.conn.batch() as batch:
            for key in keys:
                batch.download(
                    'bucket', key, callback=lambda r: results.append(r))
        self.assertEqual(set(results), set(keys))

    def test_results(self):
        '''We can get the results of our batch operations'''
        keys = ['foo', 'bar', 'baz', 'whiz']
        with self.conn.batch() as batch:
            for key in keys:
                batch.upload('bucket', key, key)

        with self.conn.batch() as batch:
            for key in keys:
                batch.download('bucket', key)

        self.assertEqual(set(batch.results()), set(keys))

    def test_success(self):
        '''We can get confirmation of a successful batch.'''
        keys = ['foo', 'bar', 'baz', 'whiz']
        with self.conn.batch() as batch:
            for key in keys:
                batch.upload('bucket', key, key)

        self.assertTrue(batch.success())

    def test_failed(self):
        '''We can get confirmation of a failed batch.'''
        keys = ['foo', 'bar', 'baz', 'whiz']
        with self.conn.batch() as batch:
            for key in keys:
                batch.download('bucket', key)

        self.assertFalse(batch.success())
