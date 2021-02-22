#!/bin/bash
# Generates an SSH config file for connections if a config var exists.

if [ "$GIT_SSH_KEY" != "" ]; then
  echo "Detected SSH key for git. Adding SSH config" >&1
  echo "" >&1

  # Ensure we have an ssh folder
  if [ ! -d ~/.ssh ]; then
    mkdir -p ~/.ssh
    chmod 700 ~/.ssh
  fi

  # Load the private key into a file.
  echo $GIT_SSH_KEY | base64 --decode >~/.ssh/deploy_key

  # Change the permissions on the file to
  # be read-only for this user.
  chmod o-w ~/
  chmod 700 ~/.ssh
  chmod 600 ~/.ssh/deploy_key

  # Setup the ssh config file.
  echo -e "Host github.com\n" \
    " IdentityFile ~/.ssh/deploy_key\n" \
    " HostName github.com\n" \
    " IdentitiesOnly yes\n" \
    " UserKnownHostsFile=/dev/null\n" \
    " StrictHostKeyChecking no" \
    >~/.ssh/config

  echo "eval $(ssh-agent -s)"
  eval $(ssh-agent -s)

  echo "ssh-add -l"
  ssh-add -l

  echo "ssh-add ~/.ssh/deploy_key"
  ssh-add ~/.ssh/deploy_key

  # uncomment to check that everything works just fine
  ssh -v git@bitbucket.org
fi
