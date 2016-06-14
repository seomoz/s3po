'''An in-memory backend'''

import collections

from ..exceptions import DownloadException, DeleteException


class Memory(object):
    '''An in-memory backend'''

    def __init__(self):
        self.buckets = collections.defaultdict(dict)

    def download(self, bucket, key, fobj, retries, headers=None):
        '''Download the contents of bucket/key to fobj'''
        obj = self.buckets[bucket].get(key)
        if not obj:
            raise DownloadException('%s / %s not found' % (bucket, key))
        else:
            fobj.write(obj)

    def upload(self, bucket, key, fobj, retries, headers=None):
        '''Upload the contents of fobj to bucket/key with headers'''
        self.buckets[bucket][key] = fobj.read()

    def list(self, bucket, prefix=None, delimiter=None, retries=None, headers=None):
        '''List the contents of a bucket.'''
        if prefix is None:
            prefix = ''
        keys = (key for key in self.buckets[bucket].keys() if key.startswith(prefix))
        if delimiter:
            return (prefix for prefix in set(key.split(delimiter, 1)[0] for key in keys))
        else:
            return keys

    def delete(self, bucket, key, retries, headers=None):
        '''Delete bucket/key with headers'''
        if key in self.buckets[bucket]:
            del self.buckets[bucket][key]
        else:
            raise DeleteException('Failed to delete %s/%s' % (bucket, key))
