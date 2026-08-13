# Benchmarks

We use [Gungraun](https://github.com/gungraun/gungraun) to run benchmarks. This is a wrapper around
Valgrind's Callgrind tool which provides cycle accurate profiling of benchmark functions. This means
that the benchmarks only run on platforms that fully support Valgrind, which does not include Mac.
In order to run the benchmarks you will need to run them on Linux. An aarch64 Linux VM on Mac will
work just fine.

To view profiling data it is recommended to use
[KCacheGrind](https://kcachegrind.sourceforge.net/html/Home.html) which is a visual wrapper around
the Callgrind profiles.

## OrbStack Linux VM setup (macOS)

Run KCachegrind in the Linux VM for proper source mappings. The following one-time setup is
specific to an OrbStack VM on macOS: it installs the Linux profiling dependencies and configures
the VM's native SSH endpoint for X11 forwarding. Other VM providers can use their equivalent X11
forwarding setup.

First, install and start XQuartz on the Mac. Set `DISPLAY` in the macOS terminal that will run SSH
if it is not already set:

```sh
brew install --cask xquartz
open -a XQuartz
export DISPLAY=:0
```

OrbStack's `ssh orb` proxy does not support X11 forwarding. Copy a regular macOS SSH public key
through the proxy, enter the VM, then run the setup command from the repository root:

```sh
cat "$HOME/.ssh/id_ed25519.pub" |
  ssh orb 'umask 077; mkdir -p ~/.ssh; cat >> ~/.ssh/authorized_keys'
ssh orb

# In the OrbStack VM:
./scripts/setup-orbstack-linux-vm.sh
source "$HOME/.cargo/env"
exit

# Back on macOS:
orbctl restart ubuntu
```

Open a new macOS terminal and use the VM's native SSH server on port 2222. `xclock` confirms that
X11 forwarding works before opening a Callgrind profile:

```sh
ssh -Y -p 2222 127.0.0.1
echo "$DISPLAY" # Expected: localhost:10.0 (or similar)
xclock
kcachegrind /path/to/callgrind.out
```

To run the benchmarks on linux use for example:

```
cargo bench -p bd-workflows
```

`bd-workflow-bench` provides the same two modes for end-to-end workflow replay. Its default
checked-in fixture is appropriate for repeatable Criterion wall-time benchmarks and Callgrind
instruction profiles; both wrappers also accept local config and log paths for a live corpus. See
[`bd-workflow-bench/README.md`](bd-workflow-bench/README.md) for commands and measurement scope.

The callgrind output which can be opened in KCacheGrind will show up in:

```
ll target/gungraun/
```

## On-demand pull request benchmarks

Maintainers can run a benchmark comparison for a pull request by posting a command comment:

```
/benchmark workflows
```

`workflows` runs `bd-workflow-bench/criterion.sh` with the checked-in replay fixture. The workflow
runs the merge base first and saves it as the Criterion baseline, then runs the pull request head
and posts the result and HTML report artifact on the pull request. A newer benchmark command for
the same pull request cancels the in-progress run.
