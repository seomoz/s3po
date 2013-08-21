'''A Mock Key'''

from .exceptions import S3ResponseError


class Key(object):
    '''A Key in a Bucket'''
    def __init__(self, bucket, name, headers=None):
        self.bucket = bucket
        self.name = name
        self.content = None
        self.content_encoding = None
        self.headers = headers or {}
        self._size = None

    @property
    def size(self):
        '''Get the size of the content'''
        if self._size:
            return self._size
        if self.content:
            return len(self.content)
        return 0  # pragma: no cover

    def delete(self):
        '''Delete this key from S3'''
        self.bucket.delete_key(self.name)

    def exists(self):
        '''Check if this key exists'''
        return self.bucket.get_key(self.name) != None

    def get_contents_as_string(self):
        '''Get the key's contents'''
        return self.content

    def get_contents_to_file(self, fobj):
        '''Write the content out to the provided file'''
        fobj.write(self.content)

    def set_contents_from_file(self, fobj, *args, **kwargs):
        '''Set the contents based on the the file'''
        self.content = fobj.read()

    def set_contents_from_string(self, string, *args, **kwargs):
        '''Set the contents from the string'''
        self.content = string


class Prefix(object):
    '''A Prefix in a Bucket'''
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name


class MultiPart(object):
    '''A multipart key'''
    def __init__(self, bucket, name, headers):
        self.bucket = bucket
        self.name = name
        self.headers = headers
        self.parts = {}

    def upload_part_from_file(self, fobj, part_num):
        '''Upload another part to this file'''
        self.parts[part_num] = fobj.read()

    def complete_upload(self):
        '''Complete this as a multipart upload'''
        try:
            limits = xrange(1, len(self.parts) + 1)
            value = ''.join(self.parts[num] for num in limits)
            self.bucket.new_key(self.name).set_contents_from_string(value)
        except KeyError:
            raise S3ResponseError('Missing part from multipart upload')
