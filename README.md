S3-PO
=====

This is a utility designed to facilitate uploads to S3, synchronous or otherwise.

Installation
------------

This module requires that you have `boto` installed. After that:

	python setup.py install

Use
===

S3-PO can be used in two ways: synchronously and asynchronously. The synchronous
usage just provides robust uploads and downloads with optional retries, backoff, 
optional silent failure, and automatic multipart upload support.

In the asynchronous way, you request that certain files be uploaded to certain
buckets and keys, and then s3po will take care of it at some point. Read the section
on Asynchronous Use for more information.

In order to get going, you must first make an `s3po.Connection` object, given your
AWS credentials. Since it uses `boto`, the normal authentication rules apply, so
if you have your credentials stored in `~/.boto`, then you don't have to provide
those details.

	import s3po
	conn = s3po.Connection()

Auto Deletion
-------------

By default, the connection will delete files it uploads after it _successfully_
uploads them. For the connection-wide default to be set to _not_ delete such files,
then set the `delete=False` flag when creating the connection.

	conn = s3po.Connection(delete=False)

This can be overridden on a per-request basis, too. It's designed to allow you to
set the default behavior to be the case most common for you. If you have the default
to not delete uploaded files, but would like to delete a _specific_ file after it's
uploaded:

	# Now by default we won't delete uploaded files
	conn = s3po.Connection(delete=False)
	# This won't get deleted
	conn.uploadFile('my-bucket', 'my-key', 'foo.txt')
	# But this one will!
	conn.uploadFile('my-bucket', 'my-key', 'bar.txt', delete=True)

Like the delete behavior, you can set the default asynchronous behavior. The default
is to make all upload requests asynchronous, but you can override this to use all
synchronous requests (unless specifically specified):

	# Now upload requests are synchronous by default
	conn = s3po.Connection(async=False)
	# This will wait until the file has uploaded
	conn.uploadFile('my-bucket', 'my-key', 'foo.txt')
	# This will return right away
	conn.uploadFile('my-bucket', 'my-key', 'bar.txt', async=True)

All download requests are synchronous. The reason for that is that we don't necessarily
want to make the presumption for the user theat they want to use a threaded environment.
This is likely to change in the user to provide easy access to this use, but while
still maintaining the flexibility to use other asynchronous execution mechanisms.

Retries
-------

You can also adjust the number of retries made on an upload or download, with the `retries`
argument. It defaults to 3 retries, backing off exponentially between failures (first
1 minute, then 2, and then 4). If you'd like to retry more or less:

	conn.uploadFile('my-bucket', 'my-key', 'foo.txt', retries=10)

Response Headers and Compression
--------------------------------

Users commonly want to set the reponse headers on S3 objects. To do so, simply provide
a dictionary for the response headers you'd like included

	conn.uploadFile('my-bucket', 'my-key', 'foo.txt', headers={
		'Content-Type': 'text/plain'
	})

You can also ask that the content be compressed before it's uploaded. Currently, `s3po`
knows about 'gzip' and `zlib` as they're among the most commonly-used compressions 
in use with HTTP. Setting the `compress` opotion also automatically sets the 
`Content-Encoding` header. Unfortunately, python's gzip module is extremely slow, and
so until such a time as that can be fixed (seriously, it's 10x slower than the 
command-line utility in 2.7), it shells out to the `gzip` and `gunzip` command-line
utilities.

	conn.uploadFile('my-bucket', 'my-key', 'foo.txt', compress='gzip')

Asynchronous Use
----------------

S3-PO support asynchronous use in two ways:

1. Separate daemon process consuming a local redis queue of json blobs that describe
	where to upload files, and where those files live
2. As a background thread in the current process. This isn't recommended, as it doesn't
	store any state for the requested uploads, and doesn't persist between stopping 
	your program.

At some point in the near future, I'd like to add support to give it an API endpoint,
to which users can post JSON blobs to request the upload of a given file, but it's not
currently a priority.

If you choose to use the threaded version, you must call `runLoop` on your `Connection`
object:

	import s3po
	
	# Make a connection object
	connection = s3po.Connection(async=True)
	# Now it will go off in the background and upload files
	connection.runLoop()
	
	# Add requests to upload files; each of these returns 
	# immediately, and s3po will upload them when it can
	connection.uploadFile('my-bucket', 'my-key', 'foo.txt')
	connection.uploadFile('my-bucket', 'my-key', 'bar.txt')
	connection.uploadFile('my-bucket', 'my-key', 'yes.txt')

If you choose to use the separate daemon process, launch the included s3po daemon, `s3pod`,
optionally providing the access-id and secret-key you wish you use. In order to use this,
you should be running a local redis instance.