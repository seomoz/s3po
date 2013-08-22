'''Various utilities'''

import time

# Logging
import logging
logger = logging.getLogger('s3po')
formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s:%(funcName)s@%(lineno)s => %(message)s')
handler = logging.StreamHandler()
handler.setLevel(logging.WARNING)
handler.setFormatter(formatter)
logger.addHandler(handler)


class CountFile(object):
    '''A file-like object that counts how much data's written to it'''
    def __init__(self, fobj):
        self._fobj = fobj
        self._start = fobj.tell()

    @property
    def count(self):
        '''How many bytes we've written'''
        return self._fobj.tell() - self._start

    def __getattr__(self, attr):
        return getattr(self._fobj, attr)


class Backoff(object):
    '''Various backoff policies'''
    @staticmethod
    def exponential(factor, base):
        '''Exponential backoff of the form factor * (base ^ x)'''
        def func(num):
            '''Exponential backoff of the form factor * (base ^ x)'''
            return factor * (base ** num)
        return func


def retry(count,
    exceptions=(Exception), sleep=None, policy=Backoff.exponential(30, 2)):
    '''Decorator for retrying a function count times'''
    sleep = sleep or time.sleep

    def _retry(func):
        '''The actual decorator'''
        def new_func(*args, **kwargs):
            '''The decorated function'''
            for attempt in xrange(count + 1):  # pragma: no branch
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    if attempt >= (count - 1):
                        raise
                    interval = policy(attempt)
                    logger.exception(
                        'Sleeping %is after attempt %i' % (interval, attempt))
                    sleep(interval)
        return new_func
    return _retry
