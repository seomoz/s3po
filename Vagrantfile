# Encoding: utf-8
# -*- mode: ruby -*-
# vi: set ft=ruby :

ENV['VAGRANT_DEFAULT_PROVIDER'] = 'virtualbox'

# http://docs.vagrantup.com/v2/
Vagrant.configure('2') do |config|
  config.vm.box = 'ubuntu/trusty64'
  config.vm.hostname = 's3po'
  config.ssh.forward_agent = true

  config.vm.provision :shell, path: 'scripts/vagrant/provision.sh', privileged: false
end
