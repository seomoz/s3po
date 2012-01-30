#! /usr/bin/env python

# Copyright (c) 2011 SEOmoz
# 
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
# 
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import os
import time
import tempfile

# Threading stuff
import Queue
import threading

# Compression stuff
from cStringIO import StringIO

# Boto
from boto import s3
from boto.s3.connection import S3Connection

# Logging
# If you would like to get s3po's logging in your logger,
# then you can monkey-patch it by simply setting:
#   s3po.logger = myLogger
import logging
from logging import handlers
logger = logging.getLogger('s3po')
formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s:%(funcName)s@%(lineno)s => %(message)s')
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)

# This represents a request to upload a file, for asynchronous use
def uploadRequest(bucket, key, path, headers=None, compress=None, retries=3, delete=None, pingback=None):
    '''Returns a dictionary representative of the request'''
    return {
        # The bucket and key where we should upload
        'bucket'  : bucket,
        'key'     : key,
        # The local absolute path of the file to upload
        'path'    : os.path.abspath(path),
        # Any headers you want set on the S3 object's response headers
        'headers' : headers or {},
        # Should we automatically compress this object?
        # Accepted:
        #   None
        #   'gzip'
        #   'zlib'
        'compress': compress,
        # How many times should we retry this upload?
        'retries' : retries,
        # Any url to ping with a POST of the json blob?
        'pingback': pingback,
        # State of the upload
        # Valid values:
        #   'queued'   (waiting for an upload)
        #   'errored'  (failed uploading)
        #   'uploaded' (completed successfully)
        'state'   : 'queued'
    }

class Connection(object):
    '''This class helps out with uploading and downloading files to and from S3'''
    # This is the user's access_id
    access_id  = None
    # And secret
    secret_key = None
    # Connection
    conn       = None
    # The upload queue
    queue      = None
    # The thread in which the uploader is running
    thread     = None
    # Should the upload loop be running?
    running    = False
    # This is the interval to use for sleeping between checking to
    # see if there are new requests for uploading
    interval   = 60
    # Should request to upload a file be treated as asynchronous?
    async      = True
    # Should we clean up files after uploading them?
    delete     = True
    # Where we should store temporary files
    tempdir    = None
    
    def __init__(self, access_id=None, secret_key=None, queue=None, async=True, delete=True, tempdir=None, *args, **kwargs):
        '''Initialize this object, very much in the same way you initialize an S3 connection in boto'''
        self.conn = S3Connection(access_id, secret_key, *args, **kwargs)
        # Set the queue this draws from
        if queue:
            self.queue = queue
        else:
            self.queue = Queue.Queue()
        # Set whether or not this is asynchronous
        self.async = async
        self.tempdir = tempdir
    
    def runLoop(self, threaded=True):
        '''Run the upload loop. If threaded is False, it runs in the main thread'''
        if threaded:
            if self.thread:
                logger.warn('Threaded runLoop called again on Connection');
            else:
                self.thread = threading.Thread(target=self.runLoop, kwargs={'threaded': False})
                self.thread.start()
        else:
            self.thread = threading.current_thread()
            # Mark that we should be running
            self.running = True
            # If we should stop uploading, then another thread
            # will have set self.running to False
            while self.running:
                # Go through a loop to upload each of the pieces of work
                # in the specified queue, and upload them
                while len(self.queue):
                    next = self.queue.pop()
                # After uploading any files that might be there, we should
                # sleep for a little while
                time.sleep(self.interval)
            self.thread = None
    
    def stopLoop(self):
        '''Stops the upload loop from running'''
        self.stopped = True
    
    def _download(self, bucket, key, retries=3):
        b = self.conn.get_bucket(bucket)
        # Make a file that we'll write into
        fd, fname = tempfile.mkstemp(dir=self.tempdir)
        for i in range(retries):
            try:
                with os.fdopen(fd, 'w+') as f:
                    # Explicitly truncate the file
                    f.truncate(0)
                    # Get the boto key, save it to the file, and make sure it's
                    # had all its data written out to disk
                    k = s3.key.Key(b, key)
                    k.get_contents_to_file(f)
                    f.flush()
                    # Now check and make sure that we've downloaded everything
                    # that S3 reports is there.
                    size = os.fstat(fd).st_size
                    if k.size != size:
                        raise Exception('Download incomplete: only %i of %i bytes' % (int(k.size or 0), size))
                    elif k.content_encoding:
                        return util.decompressFile(fname, k.content_encoding)
                    else:
                        return fname
            except Exception as e:
                if i < retries - 1:
                    logger.exception('Download failed...')
                    util.backoff(i)
                else:
                    raise e
        return False

    def _upload(self, bucket, key, fp, size, headers=None, compress=None, retries=3, silent=False):
        # Make our headers object
        headers = headers or {}
        try:
            # First, get the bucket we're going to upload to, and then make
            # the key that we'll be writing to. Also, set aside the headers
            # we'll be using
            b = self.conn.get_bucket(bucket)
            
            if compress:
                headers['Content-Encoding'] = compress
                if size < 50 * 1024 * 1024:
                    f = StringIO()
                    util.compressToFile(fp, f, compress)
                    size = len(f.getvalue())
                    f = StringIO(f.getvalue())
                else:
                    f = tempfile.TemporaryFile(dir=self.tempdir)
                    util.compressToFile(fp, f, compress)
                    f.flush()
                    size = os.fstat(f.fileno()).st_size
            else:
                f = fp
            
            for i in range(retries):
                f.seek(0)
                try:
                    if size < 10 * 1024 * 1024:
                        # If the upload is less than 10MB, upload it the conventional way
                        k = s3.key.Key(b, key)
                        k.set_contents_from_file(f, headers=headers)
                        if k.size != size:
                            raise Exception('Incomplete upload: only %i of %i bytes' % (int(k.size or 0), size))
                        else:
                            return True
                    else:
                        # Otherwise, try to do a multipart upload
                        if self._mupload(bucket, key, fp, size, retries=retries, headers=headers):
                            return True
                        raise Exception('Failed multipart upload')
                except Exception as e:
                    if i < retries - 1:
                        logger.exception('Incomplete upload')
                        util.backoff(i)
                    elif silent:
                        logger.exception('Failing silently for %s' % key)
                    else:
                        raise e
            return False
        except Exception as e:
            if silent:
                return False
            raise e
    
    def _mupload(self, bucket, key, fp, size=None, chunk=10 * 1024 * 1024, retries=3, silent=False, **kwargs):
        # This method works very much like the upload function, except that it
        # starts and completes a multi-part upload. NOTE: the file pointer must
        # be seekable
        
        # This is the minimum chunk size
        limit = 5 * 1024 * 1024
        chunk = max(limit, chunk)
        
        b = self.conn.get_bucket(bucket)
        mp = b.initiate_multipart_upload(key, **kwargs)
        
        # This holds the last data that we wrote
        last = ''
        # And this holds the data we're about to write
        next = ''
        # This is the part number we're using
        part = 2
        # This is the range that we'll be reading from
        start = 0
        while True:
            # Shift the current chunk of data to be the last
            last = next
            next = ''
            fp.seek(start)
            
            # If we were fortunate enough to be told how big the file is,
            # then we should make use of that information to see if this
            # is the last chunk or not. If we couldn't upload this chunk
            # and still have the next chunk be the appropriate size, then
            # we should make this chunk all the way to the end.
            if size and (size - start) < (chunk + limit):
                chunk = size - start
            # Read chunk bytes from the file
            d = fp.read(chunk)
            while len(d):
                next += d
                d = fp.read(len(next) - chunk)
            
            # logger.debug('Next chunk is %i bytes starting at %i' % (len(next), start))
            
            if len(next) == 0:
                # If we reach this point, then the last chunk was big enough
                # to satisfy the size limit, and to let's not re-upload
                # logger.debug('Last chunk met last time.')
                mp.complete_upload()
                return mp
            if len(next) < limit:
                logger.debug('Reuploaging...')
                # If this next part is not sufficently large for an upload,
                # then we should re-upload the last part with the combination
                # of the last data read and the current chunk to write
                next = last + next
                # logger.debug('Last chunk met. Uploading %i bytes' % len(next))
                mp.upload_part_from_file(StringIO(next), part - 1)
                mp.complete_upload()
                return mp
            else:
                mp.upload_part_from_file(StringIO(next), part)
            
            # Update the part number
            part += 1
            # Update the range
            start += chunk
        return False
    
    def _should(self, globl, loc):
        # Given the global and local configuration, decide whether we should
        # use the default behavior
        # Returns whether or not to delete a file given the configuration
        return (globl == False and loc == True) or (globl != False and loc != False)
        
    # =========================
    # Helper niceness functions
    # =========================
    def uploadFile(self, bucket, key, path, headers=None, compress=None, retries=3, async=None, delete=None, silent=False):
        # If we're doing this asynchronously, then we should go ahead and
        # just push a request on and immediately return
        if self._should(self.async, async):
            logger.debug('Using the async mechanism')
            delete = self._should(self.delete, delete)
            try:
                return self.queue.put(uploadRequest(
                    bucket, key, path, headers, compress, retries, delete))
            except AttributeError:
                return self.queue.push(uploadRequest(
                    bucket, key, path, headers, compress, retries, delete))
        # Make sure that the base is an absolute one
        path = os.path.abspath(path)
        # As little as I like to do this, it must be done. Unfortunately the 
        # gzip module in python is extremely slow (by almost an order of 
        # magnitude). So, we're just going to shell out to the gzip command
        # until there exists an efficient implementation
        if compress:
            logger.debug('Compress')
            # Ensure headers is at least a dictionary
            headers = headers or {}
            headers['Content-Encoding'] = compress
            # Now compress it
            path = util.compressFile(path, compress)
            if not path:
                return False
        
        # And upload
        size = os.stat(path).st_size
        logger.debug('Uploading')
        with file(path) as f:
            if self._upload(bucket, key, f, size, headers, None, retries, silent):
                if self._should(self.delete, delete):
                    os.remove(path)
                return True
            return False

    def uploadString(self, bucket, key, s, headers=None, compress=None, retries=3, async=None, silent=False):
        # Asynchronous string uploads don't... really work in all circumstances.
        # As such, in that case, we'll just write the string to a temp file
        if self._should(self.async, async):
            fd, path = tempfile.mkstemp(dir=self.tempdir)
            with os.fdopen(fd, 'w+') as f:
                f.write(s)
            return self.uploadFile(bucket, key, path, headers, compress, retries, async, True, silent)
        else:
            size = len(s)
            f = StringIO(s)
            return self._upload(bucket, key, f, size, headers, compress, retries, silent=silent)
    
    def downloadFile(self, bucket, key, retries=3, filename=None):
        path = self._download(bucket, key, retries)
        if not filename:
            return path
        else:
            os.rename(path, filename)
            return filename
    
    def downloadString(self, bucket, key, retries=3):
        fname = self._download(bucket, key, retries)
        if fname:
            with file(fname) as f:
                data = f.read()
            os.remove(fname)
            return data
        return None

# Have to do this one at the end
import util