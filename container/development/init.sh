#!/usr/bin/env bash

dnf group install development-tools -y
dnf install procps-ng curl file nc git awk vim tree nkf bind-utils the_silver_searcher htop btop -y

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

[[ -s $(type mysql) ]] || {
  brew install mysql-client
  echo 'export PATH="/home/linuxbrew/.linuxbrew/opt/mysql-client/bin:$PATH"' >> $HOME/.bashrc
  echo 'export LDFLAGS="-L/home/linuxbrew/.linuxbrew/opt/mysql-client/lib"' >> $HOME/.bashrc
  echo 'export CPPFLAGS="-I/home/linuxbrew/.linuxbrew/opt/mysql-client/include"' >> $HOME/.bashrc
  echo 'export PKG_CONFIG_PATH="/home/linuxbrew/.linuxbrew/opt/mysql-client/lib/pkgconfig"' >> $HOME/.bashrc
}

[[ -s $(type psql) ]] || {
  brew install libpq
  echo 'export PATH="/home/linuxbrew/.linuxbrew/opt/libpq/bin:$PATH"' >> $HOME/.bashrc
  echo 'export LDFLAGS="-L/home/linuxbrew/.linuxbrew/opt/libpq/lib"' >> $HOME/.bashrc
  echo 'export CPPFLAGS="-I/home/linuxbrew/.linuxbrew/opt/libpq/include"' >> $HOME/.bashrc
  echo 'export PKG_CONFIG_PATH="/home/linuxbrew/.linuxbrew/opt/libpq/lib/pkgconfig"' >> $HOME/.bashrc
}
