#! /usr/bin/env python

import s3po
import unittest

# Logging stuff
import logging
from s3po import logger
logger.setLevel(logging.DEBUG)

class TestCompress(unittest.TestCase):
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

unittest.main()
