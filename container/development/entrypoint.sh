#!/usr/bin/env bash
set -e

function install_package_groups() {
  echo "Installing package groups..."
  # pckage group difinitons.
  local package_groups+=("development-tools")

  # install package groups.
  dnf group install "${package_groups[@]}" -y
  return $?
}

function install_packages() {
  echo "Installing individual packages..."
  # pckage difinitons.
  local packages+=("procps-ng") 
  local packages+=("curl")
  local packages+=("file") 
  local packages+=("nc") 
  local packages+=("git")
  local packages+=("awk")
  local packages+=("vim")
  local packages+=("tree") 
  local packages+=("nkf")
  local packages+=("bind-utils") 
  local packages+=("the_silver_searcher")
  local packages+=("htop")
  local packages+=("btop")

  # install packages.
  dnf install "${packages[@]}" -y
  return $?
}

function install_homebrew() {
  echo "Checking Homebrew installation..."
  local code=0
  [[ -s /home/linuxbrew/.linuxbrew/bin/brew ]] || {
    NONINTERACTIVE=1 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)" && \
    eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)" && \
    echo >> $HOME/.bashrc && \
    echo 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"' >> $HOME/.bashrc
    code=$?
  }
  return $code
}

function install_complete() {
  echo "Checking bash complete installation..."
  local code=0
  [[ -s /home/linuxbrew/.linuxbrew/bin/brew ]] || {
    code=1
  } && {
    [[ -r "/home/linuxbrew/.linuxbrew/etc/profile.d/bash_completion.sh" ]] || {
      brew install bash-completion@2 && \
      echo '[[ -r "/home/linuxbrew/.linuxbrew/etc/profile.d/bash_completion.sh" ]] && . "/home/linuxbrew/.linuxbrew/etc/profile.d/bash_completion.sh"' >> $HOME/.bashrc && \
      source $HOME/.bashrc
      code=$?
    }
  }
  return $code
}

function install_goenv() {
  echo "Checking goenv installation..."
  local code=0
  [[ -s /home/linuxbrew/.linuxbrew/bin/brew ]] || {
    code=1
  } && {
    [[ -s $(goenv which go) ]] || {
      brew install gcc goenv go-task && \
      goenv install -f latest && \
      goenv global latest && \
      echo 'export GOENV_ROOT=$HOME/.goenv' >> $HOME/.bashrc && \
      echo 'export PATH=$GOENV_ROOT/bin:$PATH' >> $HOME/.bashrc && \
      echo 'eval "$(goenv init -)"' >> $HOME/.bashrc && \
      source $HOME/.bashrc
      code=$?
    }
  }
  return $code
}

function install_mysql_client() {
  local code=0
  [[ -s /home/linuxbrew/.linuxbrew/bin/brew ]] || {
    code=1
  } && {
    [[ -s $(type mysql) ]] || {
      brew install mysql-client && \
      echo 'export PATH="/home/linuxbrew/.linuxbrew/opt/mysql-client/bin:$PATH"' >> $HOME/.bashrc && \
      echo 'export LDFLAGS="-L/home/linuxbrew/.linuxbrew/opt/mysql-client/lib"' >> $HOME/.bashrc && \
      echo 'export CPPFLAGS="-I/home/linuxbrew/.linuxbrew/opt/mysql-client/include"' >> $HOME/.bashrc && \
      echo 'export PKG_CONFIG_PATH="/home/linuxbrew/.linuxbrew/opt/mysql-client/lib/pkgconfig"' >> $HOME/.bashrc
      code=$?
    }
  }
  return $code
}

function install_posgres_client() {
  local code=0
  [[ -s /home/linuxbrew/.linuxbrew/bin/brew ]] || {
    code=1
  } && {
    [[ -s $(type psql) ]] || {
      brew install libpq && \
      echo 'export PATH="/home/linuxbrew/.linuxbrew/opt/libpq/bin:$PATH"' >> $HOME/.bashrc && \
      echo 'export LDFLAGS="-L/home/linuxbrew/.linuxbrew/opt/libpq/lib"' >> $HOME/.bashrc && \
      echo 'export CPPFLAGS="-I/home/linuxbrew/.linuxbrew/opt/libpq/include"' >> $HOME/.bashrc && \
      echo 'export PKG_CONFIG_PATH="/home/linuxbrew/.linuxbrew/opt/libpq/lib/pkgconfig"' >> $HOME/.bashrc
      code=$?
    }
  }
  return $code
}

function install_aider() {
  local code=0
  [[ -s /home/linuxbrew/.linuxbrew/bin/brew ]] || {
    code=1
  } && {
    [[ -s $(type aider) ]] || {
      brew install aider
    code=$?
    }
  }
  return $code
}

function initialize() {
  echo "Starting initialization..."
  install_package_groups || exit 1
  install_packages || exit 1
  install_homebrew || exit 1
  install_complete || exit 1
  install_goenv || exit 1
  install_mysql_client || exit 1
  install_posgres_client || exit 1
  install_aider || exit 1
  echo "Initialization finished."
}

function entrypoint() {
  [[ -f /home/linuxbrew/.linuxbrew/bin/brew ]] && {
    eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"
  }

  [[ -d "$HOME/.goenv" ]] && {
    export GOENV_ROOT="$HOME/.goenv"
    export PATH="$GOENV_ROOT/bin:$PATH"
    [[ $(command -v goenv &>/dev/null) ]] && {
      eval "$(goenv init - bash)"
    }
  }

  [[ -d 'home/linuxbrew/.linuxbrew/opt/mysql-client/bin' ]] && {
    export PATH="/home/linuxbrew/.linuxbrew/opt/mysql-client/bin:$PATH"
  }

  [[ -d '/home/linuxbrew/.linuxbrew/opt/libpq/bin' ]] && {
    export PATH="/home/linuxbrew/.linuxbrew/opt/libpq/bin:$PATH"
  }
}

case ${1} in
  "init")
    initialize;;
  *)
    entrypoint && exec "$@";;
esac
