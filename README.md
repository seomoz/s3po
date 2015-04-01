s3po
====
![Status: Production](https://img.shields.io/badge/status-production-green.svg?style=flat)
![Team: Big Data](https://img.shields.io/badge/team-big_data-green.svg?style=flat)
![Scope: External](https://img.shields.io/badge/scope-external-green.svg?style=flat)
![Open Source: MIT](https://img.shields.io/badge/open_source-MIT-green.svg?style=flat)
![Critical: Yes](https://img.shields.io/badge/critical-yes-red.svg?style=flat)

Your friendly neighborhood S3 helper. It knows just a few tricks, but hopefully
they'll help you out.

- __Automatic retries__ -- with exponential backoff to make your life easier
- __Parallel batch operations__ -- using __gevent__ to make it hella fast
- __Built-in S3 Mocking__ -- to make your testing easier
- __Automatic multipart uploading__ -- and it can retry each piece individually

Installation
------------
This module requires that you have `boto` installed. After that:

```bash
sudo python setup.py install
```

Basic Use
=========
`s3po` knows a few tricks, but at its core, you'll use two methods: `upload`
and `download`:

```python
import s3po
conn = s3po.Connection()

# Upload with a string
conn.upload('bucket', 'key', 'howdy')
# Upload a file
with open('foo.txt') as fin:
    conn.upload('bucket', 'key', fin)

# Download as a string
print conn.download('bucket', 'key')
# Or into a file
with open('foo.txt', 'w') as fout:
    conn.download('bucket', 'key', fout)
```

Batch Use
=========
The batch object works like a context manager that provide the same interface
as the connection object. The only difference is that all the requests run in
a `gevent` pool. When the context is closed, it waits for all functions to
finish.

```python
# Upload these in a gevent pool
keys = ['key.%i' for key in range(1000)]
with conn.batch(50) as batch:
    for key in keys:
        batch.upload('bucket', key, 'hello')

# And now they're all uploaded
```

Callbacks
---------
When you want to take some action with the result, all the functions have an
additional `callback` optional parameter to you can grab the result. This is
particularly useful when doing bulk downloads:

```python
from functools import partial

def func(key, value):
    print 'Downloaded %s from %s' % (key, value)

with conn.batch(50) as batch:
    for key in keys:
        batch.download('bucket', key, callback=partial(func, key))
```

Multipart
=========
If the provided data is sufficiently large, it will automatically run the upload
as a multipart upload rather than a single upload. The advantage here is that
for any part upload that fails, it will retry just that part.

Mocking
=======
You can turn on mocking to get the same functionality of `s3po` that you'd
expect but without ever having to touch S3. Use it as a context manager:

```python
# This doesn't touch S3 at all
with conn.mock():
    conn.conn.create_bucket('foo')
    conn.upload('foo', 'bar', 'hello')
```

If you're writing tests, this is a common pattern:

```python
class MyTest(unittest.TestCase):
    def setUp(self):
        self.s3po = Connectino()
        self.mock = self.s3po.mock()
        self.mock.start()
        self.s3po.conn.create_bucket('bucket')

    def tearDown(self):
        self.mock.stop()
```

Development
===========
A `Vagrantfile` is provided for development:

```bash
# On the host OS
vagrant up
vagrant ssh

# On the vagrant instance
cd /vagrant
make test
```
