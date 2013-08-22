'''A Mock Bucket'''

from .key import Key, Prefix, MultiPart


class Bucket(object):
    '''A Bucket in an account'''
    def __init__(self, name):
        self.name = name
        self._keys = {}

    def empty(self):
        '''Return whether or not the bucket is empty'''
        return len(self._keys) == 0

    def delete_key(self, key):
        '''Delete the provided key'''
        self._keys.pop(key, None)

    def get_all_keys(self):
        '''Get all the keys!'''
        return self._keys.values()

    def get_key(self, key):
        '''Get the provided key'''
        return self._keys.get(key.lstrip('/'), None)

    def list(self, prefix='', delimiter=''):
        '''Iterate over all the keys'''
        found = set()
        for key, value in self._keys.items():
            if key.startswith(prefix):
                # If a delimiter was provided, we should find the first
                # occurrence of it /after/ the prefix
                if delimiter:
                    location = key.find(delimiter, len(prefix) + 1)
                    if location == -1:
                        # It's a full key
                        yield value
                    else:
                        name = key[:location]
                        if name not in found:
                            found.add(name)
                            yield Prefix(self, name)
                else:
                    yield value

    def new_key(self, name):
        '''Create a new key'''
        self._keys[name] = self.get_key(name) or Key(self, name)
        return self._keys[name]

    def initiate_multipart_upload(self, name, headers=None):
        '''Start a multipart upload'''
        return MultiPart(self, name, headers)
