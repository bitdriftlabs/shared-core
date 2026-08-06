#!/usr/bin/env bash
# shared-core - bitdrift's common client/server libraries
# Copyright Bitdrift, Inc. All rights reserved.
#
# Use of this source code is governed by a source available license that can be found in the
# LICENSE file or at:
# https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

set -euo pipefail

if [[ "$(uname -s)" != Linux ]]; then
  printf 'This setup script must run inside a Linux VM.\n' >&2
  exit 1
fi

if ! command -v apt-get >/dev/null; then
  printf 'This setup script currently supports Debian and Ubuntu VMs.\n' >&2
  exit 1
fi

sudo apt-get update
sudo apt-get install --yes \
  build-essential \
  ca-certificates \
  clang \
  cmake \
  curl \
  flatbuffers-compiler \
  git \
  kcachegrind \
  libssl-dev \
  lld \
  openssh-server \
  pkg-config \
  protobuf-compiler \
  valgrind \
  x11-apps \
  xauth

# OrbStack's proxy SSH endpoint does not support X11 forwarding. Configure the VM's OpenSSH socket
# on port 2222, which OrbStack exposes to macOS at 127.0.0.1:2222 after the VM is restarted.
sudo install -d -m 755 /etc/ssh/sshd_config.d
sudo tee /etc/ssh/sshd_config.d/90-x11-forwarding.conf >/dev/null <<'EOF'
X11Forwarding yes
X11UseLocalhost yes
EOF
sudo install -d -m 755 /etc/systemd/system/ssh.socket.d
sudo tee /etc/systemd/system/ssh.socket.d/90-orbstack-x11.conf >/dev/null <<'EOF'
[Socket]
ListenStream=
ListenStream=2222
EOF
sudo sshd -t
sudo systemctl daemon-reload
sudo systemctl disable --now ssh.service || true
sudo systemctl enable --now ssh.socket
sudo systemctl restart ssh.socket

if ! command -v rustup >/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
fi

export PATH="$HOME/.cargo/bin:$PATH"
rustup toolchain install stable nightly

if ! command -v cargo-nextest >/dev/null; then
  cargo install cargo-nextest --locked
fi

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
git -C "$repo_root" submodule update --init --recursive

printf '\nOrbStack Linux VM profiling prerequisites are ready.\n'
printf 'Open a new shell (or run: source "$HOME/.cargo/env") before benchmarking.\n'
printf 'Restart the VM from macOS, then connect with: ssh -Y -p 2222 127.0.0.1\n'
