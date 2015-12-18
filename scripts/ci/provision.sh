#! /bin/bash

set -e

echo '[Credentials]
aws_access_key_id = not-a-real-id
aws_secret_access_key = not-a-real-key' > tee ~/.boto
