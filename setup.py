#!/usr/bin/env python

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

try:
    from setuptools import setup
    extra = {
        'install_requires' : ['boto', 'gevent']
    }
except ImportError:
    from distutils.core import setup
    extra = {
        'dependencies' : ['boto', 'gevent']
    }

setup(
    name             = 's3po',
    version          = '0.2.0',
    description      = 'An uploading daemon for S3',
    long_description = '''Boto is a wonderful library. Sometimes, though, I just 
        want to describe a file that I want to upload, and then have it uploaded 
        asynchronously. I don't want to worry about whether or not I should use 
        a multi-part upload, or worry about having to back off from S3. I should 
        just be able to enqueue a file to be uploaded and another agent should 
        take care of the rest.''',
    author           = 'Dan Lecocq',
    author_email     = 'dan@seomoz.org',
    url              = 'http://github.com/seomoz/s3po',
    packages         = ['s3po'],
    license          = 'MIT',
    platforms        = 'Posix; MacOS X',
    classifiers      = [
        'License :: OSI Approved :: MIT License',
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Topic :: Internet :: WWW/HTTP'],
    **extra
)