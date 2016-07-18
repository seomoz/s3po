#!/usr/bin/env python

# Copyright (c) 2011, 2013 SEOmoz, Inc
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

from setuptools import setup

setup(
    name             = 's3po',
    version          = '0.4.7',
    description      = 'An uploading daemon for S3',
    long_description = '''Boto is a wonderful library. This is just a little
        help for dealing with multipart uploads, batch uploading with gevent
        and getting some help when mocking''',
    author           = 'Dan Lecocq',
    author_email     = 'dan@moz.com',
    url              = 'http://github.com/seomoz/s3po',
    packages         = ['s3po', 's3po.backends'],
    license          = 'MIT',
    platforms        = 'Posix; MacOS X',
    install_requires = [
        'boto',
        'coverage',
        'gevent',
        'mock',
        'nose',
        'python_swiftclient'
    ],
    classifiers      = [
        'License :: OSI Approved :: MIT License',
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Topic :: Internet :: WWW/HTTP'
    ]
)
