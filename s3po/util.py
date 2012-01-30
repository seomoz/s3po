#! /usr/bin/env python

import os
import time
import gzip
import zlib
import tempfile
import subprocess
from s3po import logger
from cStringIO import StringIO

# This is a utility class for all sorts of little things
def decompressFile(path, compression, tempdir=None):
    if compression == 'gzip':
        if not path.endswith('.gz'):
            newpath = '%s.gz' % path
            os.rename(path, newpath)
            subprocess.check_call(['gunzip', '-f', newpath])
            return path
        else:
            subprocess.check_call(['gunzip', '-f', path])
            return path[:-3]
    else:
        fd, newpath = tempfile.mkstemp(dir=tempdir)
        with os.fdopen(fd, 'w+') as outf:
            with file(path) as inf:
                if decompressToFile(inf, outf, compression):
                    # Remove the compressed file
                    os.remove(path)
                    # Strip off the extension if it exists
                    extension = '.%s' % compression
                    if path.endswith(extension):
                        path = path[:-len(extension)]
                    # Rename this file to the original file name, with
                    # the extension (if any) stipped.
                    os.rename(newpath, path)
                    return path
                else:
                    return False

def decompressToFile(inf, outf, compression):
    # Given an input stream that's compressed, and the compression type,
    # read in the compressed format, and write out the decompressed content
    inf.seek(0)
    if compression == 'gzip':
        logger.info('Decompressing gzip content...')
        # Make a gzip file reader, and then write its decompressed
        # contents out to the file
        tmp = gzip.GzipFile(fileobj=inf, mode='r')
        outf.writelines(tmp)
    elif compression == 'deflate' or compression == 'zlib':
        logger.info('Decompressing zlib/deflate content...')
        # Prepare to read compressed content
        tmp = zlib.decompressobj()
        # Read the result in 1MB chunks, decompress, and write to the output
        data = inf.read(1024 * 1024)
        while data:
            outf.write(tmp.decompress(data))
            data = inf.read(1024 * 1024)
        outf.write(tmp.flush())
    else:
        outf.writelines(inf)
    return True

def compressFile(path, compression):
    if compression == 'gzip':
        try:
            subprocess.check_call(['gzip', '-f', path])
            return '%s.gz' % path
        except subprocess.CalledProcessError:
            return False
    else:
        newpath = '%s.zlib' % path
        with file(newpath, 'w+') as outf:
            with file(path) as inf:
                if compressToFile(inf, outf, compression):
                    # Remove the uncompressed file
                    os.remove(path)
                    return newpath
                else:
                    return False

def compressToFile(inf, outf, compression):
    # Given an input stream that is uncompressed, write its compressed
    # contenst out to the provided file
    inf.seek(0)
    if compression == 'gzip':
        logger.info('Compressing as gzip...')
        gzip.GzipFile(fileobj=outf, mode='wb').writelines(inf)
    elif compression == 'zlib' or compression == 'deflate':
        logger.info('Compressing as zlib/deflate...')
        # Prepare to read uncompressed content
        tmp = zlib.compressobj()
        # Read in 1MB chunks
        data = inf.read(1024 * 1024)
        while data:
            outf.write(tmp.compress(data))
            data = inf.read(1024 * 1024)
        outf.write(tmp.flush())
    else:
        outf.writelines(inf)
    return True

def backoff(attempt):
    # How much should we backoff? Exponential with a 
    # base of 30. Return the number of seconds to wait
    sleep = 30 * (2 ** attempt)
    logger.info('Sleeping %is after attempt %i' % (sleep, attempt))
    time.sleep(sleep)

def pathFromKey(key):
    return key.rpartition('')[0]

def filenameFromKey(key):
    return key.rpartition('/')[-1]
