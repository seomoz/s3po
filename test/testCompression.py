#! /usr/bin/env python

import os
import time
import unittest
import s3po.util
from cStringIO import StringIO

# Logging stuff
import logging
from s3po import logger
logger.setLevel(logging.CRITICAL)

class TestCompress(unittest.TestCase):
    def setUp(self):
        self.data     = 'Hello, how are all you all today?' * 1024
        self.stringio = StringIO(self.data)
    
    def test_zlib(self):
        # First, compress, and make sure that it's not the same as original
        compressed = StringIO()
        s3po.util.compressToFile(self.stringio, compressed, 'zlib')
        self.assertNotEqual(self.data, compressed.getvalue(), 'Compressed content same as input')
         
        # Now, decompress, make sure it equals the original
        decompressed = StringIO()
        s3po.util.decompressToFile(compressed, decompressed, 'zlib')
        self.assertEqual(self.data, decompressed.getvalue(), 'Decompressed content does not equal original.')
         
        # Now try to decompress it as a different format, make sure it throws
        decompressed = StringIO()
        self.assertRaises(Exception, s3po.util.decompressToFile, (compressed, decompressed, 'gzip'))
    
    def test_zlib_file(self):
        # First, write this out to a file, and then we'll try to read it and see.
        import tempfile
        fd, path = tempfile.mkstemp()
        with os.fdopen(fd, 'w+') as f:
            f.write(self.data)
        # With that written out, let's try to compress it, and read back in the
        # contents.
        newpath = s3po.util.compressFile(path, 'zlib')
        with file(newpath) as f:
            self.assertNotEqual(self.data, f.read(), 'Compressed content same as input')
            self.assertEqual(newpath, path + '.zlib')
        
        # Alright, now let's try to decompress the file
        newpath = s3po.util.decompressFile(newpath, 'zlib')
        with file(newpath) as f:
            self.assertEqual(self.data, f.read(), 'Decompressed content does not equal original.')
            self.assertTrue(not newpath.endswith('.zlib'))
        
    def test_gzip(self):
        # First, compress, and make sure that it's not the same as original
        compressed = StringIO()
        s3po.util.compressToFile(self.stringio, compressed, 'gzip')
        self.assertNotEqual(self.data, compressed.getvalue(), 'Compressed content same as input')
         
        # Now, decompress, make sure it equals the original
        decompressed = StringIO()
        s3po.util.decompressToFile(compressed, decompressed, 'gzip')
        self.assertEqual(self.data, decompressed.getvalue(), 'Decompressed content does not equal original.')
         
        # Now try to decompress it as a different format, make sure it throws
        decompressed = StringIO()
        self.assertRaises(Exception, s3po.util.decompressToFile, (compressed, decompressed, 'zlib'))
    
    def test_gzip_file(self):
        # First, write this out to a file, and then we'll try to read it and see.
        import tempfile
        fd, path = tempfile.mkstemp()
        with os.fdopen(fd, 'w+') as f:
            f.write(self.data)
        # With that written out, let's try to compress it, and read back in the
        # contents.
        newpath = s3po.util.compressFile(path, 'gzip')
        with file(newpath) as f:
            self.assertNotEqual(self.data, f.read(), 'Compressed content same as input')
            self.assertEqual(newpath, path + '.gz')
        
        # Alright, now let's try to decompress the file
        newpath = s3po.util.decompressFile(newpath, 'gzip')
        with file(newpath) as f:
            self.assertEqual(self.data, f.read(), 'Decompressed content does not equal original.')
            self.assertTrue(not newpath.endswith('.gz'))

unittest.main()
