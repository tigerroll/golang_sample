#!/usr/bin/env bash

dnf group install development-tools -y
dnf install procps-ng curl file git awk vim the_silver_searcher htop btop -y

[[ -s /home/linuxbrew/.linuxbrew/bin/brew ]] || {
  NONINTERACTIVE=1 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"
  echo >> $HOME/.bashrc
  echo 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"' >> $HOME/.bashrc
}

brew install gcc goenv go-task

[[ -s $(goenv which go) ]] || {
  goenv install -f latest
  goenv global latest
  echo 'export GOENV_ROOT=$HOME/.goenv' >> $HOME/.bashrc
  echo 'export PATH=$GOENV_ROOT/bin:$PATH' >> $HOME/.bashrc
  echo 'eval "$(goenv init -)"' >> $HOME/.bashrc
  source $HOME/.bashrc
}
