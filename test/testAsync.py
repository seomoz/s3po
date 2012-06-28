#! /usr/bin/env python

import s3po
import unittest

# Logging stuff
import logging
from s3po import logger
logger.setLevel(logging.DEBUG)

class TestCompress(unittest.TestCase):
    def setUp(self):
        self.bucket = 'blogscape'
        self.keys   = ['testing-%i' % i for i in range(10)]
        self.connection = s3po.Connection(
            'access_id',
            'secret_key',
            async=False)
        self.count = 0
    
    def tearDown(self):
        bucket = self.connection.conn.get_bucket(self.bucket)
        for key in self.keys:
            bucket.delete_key(key)
    
    def test_async(self):
        conn = s3po.Connection()
        
        # Make sure that the default behavior comes through when the second
        # argument isn't set
        self.assertTrue(    conn._should(None , None))
        self.assertTrue(    conn._should(True , None))
        self.assertTrue(not conn._should(False, None))
        
        # And make sure that the second argument is honored, despite what the
        # default is
        self.assertTrue(not conn._should(None  , False))
        self.assertTrue(not conn._should(True  , False))
        self.assertTrue(not conn._should(False , False))
        
        self.assertTrue(    conn._should(None  , True))
        self.assertTrue(    conn._should(True  , True))
        self.assertTrue(    conn._should(False , True))
    
    def test_batch_upload(self):
        # Make sure that we can upload in parallel
        # Make sure the keys don't exist ahead of time
        bucket = self.connection.conn.get_bucket(self.bucket)
        for key in self.keys:
            self.assertEqual(bucket.get_key(key), None)
        
        string = 'hello'
        with self.connection.batch(10) as b:
            for key in self.keys:
                b.uploadString(self.bucket, key, string)
        
        # Now, make sure that they exist
        bucket = self.connection.conn.get_bucket(self.bucket)
        for key in self.keys:
            self.assertNotEqual(bucket.get_key(key), None)
    
    def test_batch_download(self):
        # Make sure that we can download in parallel
        with self.connection.batch(10) as b:
            for key in self.keys:
                b.uploadString(self.bucket, key, 'hello')
        
        # Now download them
        results = {}
        with self.connection.batch(10) as b:
            for key in self.keys:
                results[key] = b.downloadString(self.bucket, key)
        
        # results has a bunch of greenlets for values
        for k, v in results.items():
            self.assertEqual(v.value, 'hello')
    
    def callback(self, string):
        self.assertEqual(string, 'hello')
        self.count += 1
    
    def test_callbacks(self):
        # We should be able to specify callbacks for batch operations
        with self.connection.batch(10) as b:
            for key in self.keys:
                b.uploadString(self.bucket, key, 'hello')
        
        # Now download them, with our provided callback
        with self.connection.batch(10) as b:
            for key in self.keys:
                b.downloadString(self.bucket, key, callback=self.callback)
        
        # Now make sure it was called 10 times
        self.assertEqual(self.count, 10)
    
    def test_background(self):
        # Outside the context of a batch job, we should be able to upload
        pass

unittest.main()
