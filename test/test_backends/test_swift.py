'''Talk to Swift'''

from cStringIO import StringIO
from swiftclient.exceptions import ClientException

import mock
from base import BaseTest

from s3po.backends.swift import Swift
from s3po.exceptions import UploadException, DownloadException


class SwiftBackendTest(BaseTest):
    '''We can talk to Swift as expected.'''

    def setUp(self):
        BaseTest.setUp(self)
        self.backend = Swift()
        self.conn = mock.Mock()
        mock.patch.object(self.backend, 'conn', self.conn).start()

    def test_download_missing(self):
        '''Downloading a missing object gives us failure.'''
        self.conn.get_object.side_effect = ClientException('Missing')
        self.assertRaises(
            DownloadException, self.backend.download, 'bucket', 'key', StringIO(), 1)

    def test_download_success(self):
        '''Can successfully download an object.'''
        data = 'content'
        headers = {
            'content-length': len(data)
        }
        self.conn.get_object.return_value = (headers, list(data))
        result = StringIO()
        self.backend.download('bucket', 'key', result, 1)
        self.assertEqual(result.getvalue(), data)

    def test_download_partial(self):
        '''Throws an exception if we read too few bytes.'''
        data = 'content'
        headers = {
            'content-length': len(data) * 10
        }
        self.conn.get_object.return_value = (headers, list(data))
        self.assertRaises(
            DownloadException, self.backend.download, 'bucket', 'key', StringIO(), 1)

    def test_missing_content_length(self):
        '''Handles the case where no content length is provided.'''
        data = 'content'
        headers = {}
        self.conn.get_object.return_value = (headers, list(data))
        result = StringIO()
        self.backend.download('bucket', 'key', result, 1)
        self.assertEqual(result.getvalue(), data)

    def test_upload_exception(self):
        '''Throws UploadException when uploads fail.'''
        self.conn.put_object.side_effect = ClientException('Failed to upload')
        self.assertRaises(UploadException,
            self.backend.upload, 'bucket', 'key', StringIO('content'), 1)

    def test_list(self):
        self.conn.get_container.side_effect = [(None, [{'name':'key'}]),
                                               (None, [])]
        self.assertEqual(list(self.backend.list('bucket')),
                         ['key'])
