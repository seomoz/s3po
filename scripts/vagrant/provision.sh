#! /bin/bash

set -e

sudo apt-get update
sudo apt-get install -y python-pip python-dev python3-pip python3-dev

echo '[Credentials]
aws_access_key_id = not-a-real-id
aws_secret_access_key = not-a-real-key' > ~/.boto

(
    cd /vagrant
    sudo pip install -r requirements.txt
    sudo pip3 install -r requirements.txt
)

echo $'\ncd /vagrant' >> ~/.profile
