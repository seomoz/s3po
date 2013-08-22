'''Mock s3 access with the local filesystem'''

import os
import shutil


class Mock(object):
    '''Mocking base'''
    atts = []

    def __init__(self, obj):
        self._obj = obj
        self._original = {}

    def mock(self, obj, atts):
        '''Mock the object's attributes with this object's attributes'''
        for att in atts:
            self._original[att] = getattr(obj, att)
            setattr(obj, att, getattr(self, att))

    def unmock(self, obj, atts):
        '''Stop the mockery'''
        for att in atts:
            setattr(obj, att, self._original.pop(att))

    def start(self):
        '''Start mocking'''
        self.mock(self._obj, self.atts)

    def stop(self):
        '''Stop mocking'''
        self.unmock(self._obj, self.atts)

    def __enter__(self):
        self.start()

    def __exit__(self, typ, val, trace):
        self.unmock(self._obj, self.atts)
        if typ:
            raise typ, val, trace


class S3Key(object):
    '''Mock a key'''
    def __init__(self, bucket, name):
        self._bucket = bucket
        self._name = name
        self.path = os.path.join(bucket.path, name.rstrip('/'))

    def exists(self):
        '''Return true if this key exists'''
        return os.path.exists(self.path)

    @staticmethod
    def mkdir(path):
        '''Make the path and return it'''
        try:
            os.makedirs(os.path.dirname(path))
        except:
            pass
        return path

    def set_contents_from_string(self, string):
        '''Set the file's content from a string'''
        with open(self.mkdir(self.path), 'w+') as fout:
            fout.write(string)

    def get_contents_as_string(self):
        '''Get the contents as a string'''
        with open(self.path) as fin:
            return fin.read()


class S3Prefix(object):
    '''A Prefix'''
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name


class S3Bucket(object):
    '''Mock a bucket'''
    def __init__(self, base, name):
        self._base = base
        self._name = name
        self.path = os.path.join(base, name)

    def get_key(self, name):
        '''Get a key in this bucket, if it exists. Otherwise, None'''
        key = S3Key(self, name)
        if not key.exists():
            return None
        return key

    def list(self, prefix, separator=None):
        '''Iterate over keys beginning with this prefix, until separator'''
        paths = [(self._base, '')]
        found = []
        while paths:
            path, relative = paths.pop()
            if os.path.isdir(path):
                paths.extend(
                    (p, os.path.join(relative, p)) for p in os.listdir(path))
            else:
                found.append(path)

        paths = []
        for _, relative in found:
            if not relative.startswith(prefix):
                continue
            if separator:
                location = relative.find(separator)
                if location == -1:
                    paths.append(relative)
                else:
                    paths.append(relative[:location])
            else:
                paths.append(relative)

        # Now, uniquify them and turn them to objects
        paths = sorted(list(set(paths)))
        results = []
        for path in paths:
            if os.path.exists(os.path.join(self._base, path)):
                results.append(S3Key(self, path))
            else:
                results.append(S3Prefix(self, path))
        return results


class S3Connection(Mock):
    '''Mock the S3Connection for an S3Connection'''
    atts = ['get_bucket']

    def __init__(self, base, conn):
        Mock.__init__(self, conn)
        self._base = base

    def get_bucket(self, bucket):
        '''Get a bucket object'''
        return S3Bucket(self, bucket)


class S3po(Mock):
    '''Mock is a context manager that mocks s3 operations'''
    # These are the methods that this mocks on s3po
    atts = [
        'uploadString', 'uploadFile', 'downloadString', 'downloadFile', 'conn']

    def __init__(self, base, s3obj):
        Mock.__init__(self, s3obj)
        self._base = os.path.abspath(base)
        self.conn = S3Connection(self._base, s3obj.conn)

    def stop(self):
        '''Stop mocking'''
        # Stop the mocking for us and our connection
        Mock.stop(self)
        self.conn.stop()
        if os.path.exists(self._base):
            shutil.rmtree(self._base)

    ##########################################
    # Actual methods that it's mocking
    ##########################################
    def uploadString(self, bucket, key, string, *args, **kwargs):
        '''Upload the provided string'''
        S3Key(
            self.conn.get_bucket(bucket), key).set_contents_from_string(string)

    def uploadFile(self, bucket, key, path, *args, **kwargs):
        '''Upload the provided file'''
        with open(path) as fin:
            self.uploadString(bucket, key, fin.read())

    def downloadString(self, bucket, key, *args, **kwargs):
        '''Download the provided string'''
        return S3Key(self.conn.get_bucket(bucket), key).get_contents_as_string()

    def downloadFile(self, bucket, key, filename=None, *args, **kwargs):
        '''Download the provided file'''
        path = filename or os.path.dirname(key)
        with open(path, 'w+') as fout:
            fout.write(self.downloadString(bucket, key))
