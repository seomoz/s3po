'''Base for our tests'''

import os
import shutil
import unittest

from s3po import Connection


class BaseTest(unittest.TestCase):
    '''Test our connection's functionality'''
    tmpdir = 'test/tmp'

    def setUp(self):
        self.conn = Connection.memory()
        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        self.assertEqual(os.listdir(self.tmpdir), [], 'Empty out the tmp directory')

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def tmpfile(self, *parts):
        '''Get a path to a temp file whose directory exists.'''
        path = os.path.join(self.tmpdir, *parts)
        base = os.path.dirname(path)
        if not os.path.exists(base):
            os.makedirs(base)
        return path
