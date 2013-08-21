'''A Mock S3 Connection'''


from .bucket import Bucket
from .exceptions import S3ResponseError


class S3Connection(object):
    '''An S3 Connection. With buckets and everything'''
    def __init__(self):
        self._buckets = {}

    def create_bucket(self, bucket):
        '''Create a bucket'''
        if not bucket in self._buckets:
            self._buckets[bucket] = Bucket(bucket)

    def delete_bucket(self, bucket):
        '''Delete a bucket'''
        obj = self._buckets.get(bucket)
        if obj and not obj.empty():
            raise S3ResponseError('Bucket %s not empty' % bucket)
        self._buckets.pop(bucket, None)

    def get_bucket(self, bucket):
        '''Get a bucket'''
        obj = self._buckets.get(bucket)
        if not obj:
            raise S3ResponseError('Bucket %s does not exist' % bucket)
        return obj

    def get_all_buckets(self):
        '''Get ALL the buckets'''
        return self._buckets.values()

    def lookup(self, bucket):
        '''Get a bucket, or None if it doesn't exist'''
        return self._buckets.get(bucket, None)
