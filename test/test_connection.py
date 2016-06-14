'''Test our Connection'''

from base import BaseTest
from cStringIO import StringIO

from s3po.exceptions import DownloadException, DeleteException


class ConnectionTest(BaseTest):
    '''Test our connection's functionality'''

    def test_round_trip(self):
        '''Can upload a string.'''
        self.conn.upload('bucket', 'key', 'content')
        self.assertEqual(self.conn.download('bucket', 'key'), 'content')

    def test_upload_file(self):
        '''Upload from a file object.'''
        self.conn.upload('bucket', 'key', StringIO('content'))
        self.assertEqual(self.conn.download('bucket', 'key'), 'content')

    def test_upload_filename(self):
        '''Upload by filename.'''
        path = self.tmpfile('upload')
        with open(path, 'w+') as fout:
            fout.write('content')

        self.conn.upload_file('bucket', 'key', path)
        self.assertEqual(self.conn.download('bucket', 'key'), 'content')

    def test_download_file(self):
        '''Can download to a file.'''
        fobj = StringIO()
        self.conn.upload('bucket', 'key', 'content')
        self.conn.download('bucket', 'key', fobj)
        self.assertEqual(fobj.getvalue(), 'content')

    def test_download_filename(self):
        '''Download by filename.'''
        path = self.tmpfile('download')
        self.conn.upload('bucket', 'key', 'content')
        self.conn.download_file('bucket','key', path)
        with open(path) as fin:
            self.assertEqual(fin.read(), 'content')

    def test_download_missing(self):
        '''Raises an exception downloading a missing key.'''
        self.assertRaises(DownloadException, self.conn.download, 'bucket', 'key')

    def test_mock(self):
        '''Mocking swaps out the backend.'''
        original = self.conn.backend
        try:
            with self.conn.mock():
                self.assertNotEqual(original, self.conn.backend)
                raise ValueError('Explode')
        except ValueError:
            pass

        self.assertEqual(original, self.conn.backend)

    def test_list(self):
        self.conn.upload('bucket', 'key', StringIO('content'))
        self.assertEqual(list(self.conn.list('bucket')),
                         ['key'])

    def test_list_prefix(self):
        self.conn.upload('bucket', 'key', StringIO('content'))
        self.conn.upload('bucket', 'something_else', StringIO('content'))
        self.assertEqual(list(self.conn.list('bucket', prefix='k')),
                         ['key'])

    def test_list_delimiter(self):
        self.conn.upload('bucket', 'a.1', StringIO('content'))
        self.conn.upload('bucket', 'a.2', StringIO('content'))
        self.assertEqual(list(self.conn.list('bucket', delimiter='.')),
                         ['a'])

    def test_delete_file(self):
        '''Can delete.'''
        self.conn.upload('bucket', 'key', 'content')
        self.conn.delete('bucket', 'key')
        self.assertEqual(list(self.conn.list('bucket', prefix='k')), [])

    def test_delete_unexisting_key(self):
        '''Delete raises when key not found.'''
        with self.assertRaises(DeleteException):
            self.conn.delete('bucket', 'key')
