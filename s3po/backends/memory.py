'''An in-memory backend'''

from ..exceptions import DownloadException


class Memory(object):
    '''An in-memory backend'''

    def __init__(self):
        self.buckets = {}

    def download(self, bucket, key, fobj, retries, headers=None):
        '''Download the contents of bucket/key to fobj'''
        obj = self.buckets.get(bucket, {}).get(key)
        if not obj:
            raise DownloadException('%s / %s not fount' % (bucket, key))
        else:
            fobj.write(obj)

    def upload(self, bucket, key, fobj, retries, headers=None):
        '''Upload the contents of fobj to bucket/key with headers'''
        if bucket not in self.buckets:
            self.buckets[bucket] = {}

        self.buckets[bucket][key] = fobj.read()
