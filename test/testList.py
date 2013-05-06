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

class TestList(unittest.TestCase):
    def setUp(self):
        logger.info('Setting up')
        self.payload  = '0' * 1024
        self.nkeys = 4
        self.bucket = 'freshscape'
        self.prefix = 'test/key'
        self.keys = [self.prefix + repr(i) for i in range(self.nkeys)]
        self.connection = s3po.Connection(
            'access_id',
            'secret_key',
            async=False)
    
    def tearDown(self):
        logger.info('Tearing down...')
        try:
            b = self.connection.conn.get_bucket(self.bucket)
            for key in self.keys:
                k = b.get_key(key)
                k.delete()
        except:
            pass
    
    def test_list(self):
        # Upload a string a few times, then make sure they appear
        # in the list.
        for key in self.keys:
            self.connection.uploadString(self.bucket, key, self.payload)

        actualKeys = self.connection.listNames(self.bucket, self.prefix)
        self.assertEqual(actualKeys, self.keys)

if __name__ == '__main__':
    unittest.main()
