# Encoding: utf-8
# -*- mode: ruby -*-
# vi: set ft=ruby :

# http://docs.vagrantup.com/v2/
Vagrant.configure('2') do |config|
  config.vm.box = 'ubuntu/trusty64'
  config.vm.hostname = 's3po'
  config.ssh.forward_agent = true

  config.vm.provision :shell, inline:
    'echo \'Defaults env_keep += "SSH_AUTH_SOCK"\' > /etc/sudoers.d/ssh-auth-sock; ' +
    'chmod 0440 /etc/sudoers.d/ssh-auth-sock'

  config.vm.provision :shell, path: 'scripts/vagrant/provision.sh', privileged: false
end
