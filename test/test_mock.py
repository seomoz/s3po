'''Make sure our connection class works as expected'''

import unittest
from cStringIO import StringIO

from s3po.mock import Mock
from s3po.mock.key import Key, Prefix
from s3po.mock.connection import S3Connection
from s3po.mock.exceptions import S3ResponseError


class ConnectionTest(unittest.TestCase):
    '''Test our connection'''
    def setUp(self):
        self.connection = S3Connection()

    def test_lookup(self):
        '''Lookup should give us no bucket for an empty account'''
        self.assertEqual(self.connection.lookup('foo'), None)

    def test_get_bucket(self):
        '''Getting a non-existent bucket raises an error'''
        self.assertRaises(S3ResponseError, self.connection.get_bucket, 'foo')
        self.connection.create_bucket('foo')
        self.assertEqual(self.connection.get_bucket('foo').name, 'foo')

    def test_create_bucket(self):
        '''We should be able to create a bucket'''
        self.assertEqual(self.connection.lookup('foo'), None)
        self.connection.create_bucket('foo')
        self.assertEqual(self.connection.lookup('foo').name, 'foo')
        self.connection.create_bucket('foo')

    def test_get_all_buckets(self):
        '''We should be able to find all buckets'''
        self.assertEqual(self.connection.get_all_buckets(), [])
        names = ['foo', 'bar', 'whiz']
        for name in names:
            self.connection.create_bucket(name)
        self.assertEqual(set(names),
            set(b.name for b in self.connection.get_all_buckets()))

    def test_delete_bucket(self):
        '''We should be able to delete an empty bucket'''
        self.connection.create_bucket('foo')
        self.assertEqual(self.connection.get_bucket('foo').name, 'foo')
        self.connection.delete_bucket('foo')
        self.assertEqual(self.connection.lookup('foo'), None)

    def test_delete_nonempty_bucket(self):
        '''We can't delete a bucket that has keys'''
        self.connection.create_bucket('foo')
        self.connection.get_bucket('foo').new_key('bar')
        self.assertRaises(S3ResponseError, self.connection.delete_bucket, 'foo')


class BucketTest(unittest.TestCase):
    '''Test our Buckets'''
    def setUp(self):
        self.connection = S3Connection()
        self.connection.create_bucket('foo')
        self.bucket = self.connection.get_bucket('foo')

    def test_create_key(self):
        '''We should be able to make a key'''
        self.assertEqual(self.bucket.get_key('foo'), None)
        self.bucket.new_key('foo')
        self.assertEqual(self.bucket.get_key('foo').name, 'foo')

    def test_get_all_keys(self):
        '''We should be able to see all the keys'''
        self.assertEqual(self.bucket.get_all_keys(), [])
        names = ['foo', 'bar', 'whiz']
        for name in names:
            self.bucket.new_key(name)
        self.assertEqual(set(names),
            set(k.name for k in self.bucket.get_all_keys()))

    def test_delete_key(self):
        '''We should be able to delete a key'''
        self.bucket.new_key('foo')
        self.assertEqual(self.bucket.get_key('foo').name, 'foo')
        self.bucket.delete_key('foo')
        self.assertEqual(self.bucket.get_key('foo'), None)

    def test_list(self):
        '''We should be able to list all keys of a given prefix'''
        names = ['hello/foo', 'hello/bar', 'howdy/foo']
        for name in names:
            self.bucket.new_key(name)
        # All the keys with the 'hello' prefix
        self.assertEqual(set(n for n in names if n.startswith('hello')),
            set(k.name for k in self.bucket.list('hello')))
        # And now for no prefix
        self.assertEqual(set(names),
            set(k.name for k in self.bucket.list()))

    def test_delimiter_list(self):
        '''Make sure we can get keys up to a delimiter'''
        # In particular, we want to exercise that:
        #   - the delimiter appears in the prefix
        #   - a key exists before the delimiter
        #   - a key exists after the delimiter
        names = [
            'hello/howdy',
            'hello/how/are/you/today',
            'hello/how/are/you/tomorrow']
        for name in names:
            self.bucket.new_key(name)
        found = dict((k.name, k) for k in self.bucket.list('hello/', '/'))
        self.assertIsInstance(found['hello/howdy'], Key)
        self.assertIsInstance(found['hello/how'], Prefix)

    def test_missing_parts(self):
        '''Missing parts of a multi-part upload is a bad thing'''
        multi = self.bucket.initiate_multipart_upload('foo')
        multi.upload_part_from_file(StringIO('hello'), 1)
        multi.upload_part_from_file(StringIO('hello'), 2)
        multi.upload_part_from_file(StringIO('hello'), 10)
        self.assertRaises(S3ResponseError, multi.complete_upload)


class KeyTest(unittest.TestCase):
    '''Test our Key'''
    def setUp(self):
        self.connection = S3Connection()
        self.connection.create_bucket('foo')
        self.bucket = self.connection.get_bucket('foo')
        self.key = self.bucket.new_key('bar')

    def test_exists_delete(self):
        '''We should be able to verify that a key exists and delete it'''
        self.assertTrue(self.key.exists())
        self.key.delete()
        self.assertFalse(self.key.exists())

    def test_set_get_contents_as_string(self):
        '''We can set it as a string'''
        self.assertEqual(self.key.get_contents_as_string(), None)
        self.key.set_contents_from_string('hello')
        self.assertEqual(self.key.get_contents_as_string(), 'hello')

    def test_set_get_contents_as_file(self):
        '''We can set it and get it as a file'''
        self.key.set_contents_from_file(StringIO('hello'))
        obj = StringIO()
        self.key.get_contents_to_file(obj)
        self.assertEqual(obj.getvalue(), 'hello')


class FakeConn(object):
    '''An object with attributes that will be re-set when done'''
    def __init__(self):
        self.bar = 5

    def foo(self):
        '''Dummy method'''
        return 5

    def create_bucket(self, name):
        '''Create a bucket'''
        pass

    def get_bucket(self, name):
        '''Get a bucket, perhaps'''
        pass


class PatchTest(unittest.TestCase):
    '''Make sure our patching works as expected'''
    def setUp(self):
        self.conn = FakeConn()

    def test_basic(self):
        '''Make sure that it switches around all the attributes we expect'''
        self.assertEqual(self.conn.foo(), 5)
        with Mock(self.conn):
            self.assertRaises(NotImplementedError, self.conn.foo)
            self.conn.create_bucket('foo')
            self.assertEqual(self.conn.get_bucket('foo').name, 'foo')
        self.assertEqual(self.conn.foo(), 5)

    def test_exception(self):
        '''Make sure it raises an exception when appropriate'''
        self.assertEqual(self.conn.foo(), 5)

        def func():
            '''Raise an exception in the block'''
            with Mock(self.conn):
                self.assertRaises(NotImplementedError, self.conn.foo)
                self.conn.create_bucket('foo')
                self.assertEqual(self.conn.get_bucket('foo').name, 'foo')
                raise ValueError('Something went wrong')

        self.assertRaises(ValueError, func)
        self.assertEqual(self.conn.foo(), 5)
