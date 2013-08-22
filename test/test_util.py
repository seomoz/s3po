'''Make sure our utitilies work as advertised'''

import unittest

from s3po.util import retry


class UtilTest(unittest.TestCase):
    '''Test our utilities'''
    def test_retry(self):
        '''We should be able to retry functions'''
        @retry(10, sleep=lambda _: 0)
        def func(obj):
            '''Counting function'''
            obj['count'] += 1
            raise ValueError('foo')

        obj = {'count': 0}
        self.assertRaises(ValueError, func, obj)
        self.assertEqual(obj['count'], 10)

    def test_exceptions(self):
        '''We should be able to limit the exceptions we retry on'''
        @retry(10, exceptions=(TypeError))
        def func(obj):
            '''Raise a ValueError'''
            obj['count'] += 1
            raise ValueError('foo')

        obj = {'count': 0}
        self.assertRaises(ValueError, func, obj)
        self.assertEqual(obj['count'], 1)
