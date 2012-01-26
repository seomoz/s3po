#! /usr/bin/env python

import os
import time
import s3po
import unittest
from cStringIO import StringIO

# Logging stuff
import logging
from s3po import logger
logger.setLevel(logging.DEBUG)

class TestCrawled(unittest.TestCase):
    def setUp(self):
        logger.info('Setting up')
        # This is 1MB, and should not be uploaded with multipart uploads automatically
        self.small  = '0' * 1024 * 1024
        # This is 11MB, and should be uploaded with multipart uploads automatically
        self.large  = '01234567890' * 1024 * 1024
        self.bucket = 'pr1-store'
        self.key    = '000000000-testing-%i' % int(time.time())
        self.connection = s3po.Connection(async=False)
    
    def tearDown(self):
        logger.info('Tearing down...')
        try:
            b = self.connection.conn.get_bucket(self.bucket)
            k = b.get_key(key)
            k.delete()
        except:
            pass
    
    def test_upload(self):
        # Upload a string, and then download it, to make sure that we get everything back
        self.assertTrue(self.connection.uploadString(self.bucket, self.key, self.small))
        self.assertEqual(self.connection.downloadString(self.bucket, self.key), self.small)
        
        # Now upload the big one
        self.assertTrue(self.connection.uploadString(self.bucket, self.key, self.large))
        self.assertEqual(self.connection.downloadString(self.bucket, self.key), self.large)
    
unittest.main()
