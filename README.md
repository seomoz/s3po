s3po
====
Your friendly neighborhood S3 helper

Installation
------------
This module requires that you have `boto` installed. After that:

    python setup.py install

Synchronous Use
===============
`s3po` can be used __synchronously__ to help you automatically compress or 
decompress files as they're uploaded or downloaded, or to easily perform
multi-part uploads, or even just to automatically retry:

    import s3po
    conn = s3po.Connection()
    # Upload this string setting the Content-Encoding header to `gzip` and 
    # compressing it with gzip.
    conn.uploadString('bucket', 'key', ..., compress='gzip')
    # When downloading, it's also aware that it was compressed with `gzip` and 
    # will automatically decompress it for you
    results = conn.downloadString('bucket', 'key')

Asynchronous Use
================
It can also help you to upload files or strings in the background:

    conn.uploadString('bucket', 'key', ..., async=True)

Alternatively, if you have a batch of uploads or downloads, you can use a batch 
object (which incidentally works with `with`). It has the same interface as the
connection object, but it will run everything in a pool of greenlets. Each 
function you'd call on `conn` returns instead a `gevent` greenlet, which can be
used to access the normal return value after the fact. So one way to make use of
this:

    # We've got a lot to download:
    keys = [...]
    # Use 30 greenlets
    results = {}
    with conn.batch(30) as batch:
        for key in keys:
            results[key] = batch.downloadString('bucket', key)
    
    # By the time we reach this line, all the downloads have completed
    for k, greenlet in results.items():
        print 'Downloaded %s from %s' % (greenlet.value, k)

If you'd prefer, you can also provide a callback:

    # Our callback
    def foo(results):
        print results
    
    with conn.batch(30) as batch:
        for key in keys:
            batch.downloadString('bucket', key, callback=foo)

This is extremely useful with `functools.partial` to help you bind arguments to 
the callback:

    # Our callback
    def foo(key, results):
        print 'Downloaded %s from %s' % (results, key)
    
    # We're going to need to bind arguments
    from functools import partial
    
    with conn.batch(30) as batch:
        for key in keys:
            # Bind the `key` argument of our callback
            cb = partial(foo, key=key)
            batch.downloadString('bucket', key, callback=cb)

Other Options and Features
==========================

Multipart Uploads
-----------------
By default, anything over 10MB is uploaded as multi-part.

Deleting Uploaded Files
-----------------------
When performing uploads, you're sometimes interested in deleting the file 
afterwards. If that's the case, you can provide `delete=True`:

    # Delete this file once it's done uploading
    conn.uploadFile('bucket', 'key', '/some/path', async=True, delete=True)

Changing Retries
----------------
If you want an operation to use more or fewer retries than the default 3, then 
the `retries` argument is for you:

    # Retry up to 10 times
    conn.uploadFile('bucket', 'key', '/some/path', async=True, retries=10)

Headers
-------
It can be useful to set additional headers to be included as metadata with the 
key. For example, if you want to describe the content with a MIME type:

    # Upload a JSON file
    conn.uploadString('bucket', 'key', ..., headers={
        'Content-Encoding': 'application/json'
    })

When you ask for the key to be compressed as it's uploaded, then the
`Content-Encoding` header is automatically set appropriately.
