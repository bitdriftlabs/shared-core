# shared-core

This repo contains bitdrift code shared between the client and server.

## Development

Tests are run with `cargo test`. If you run into file descriptor exhaustion, consider increasing
the ulimit size or target specific crates. Alternatively, consider using nextest which seems less
prone to exhaustion:

```bash
cargo install cargo-nextest --locked
cargo nextest run # Accepts cargo test arguments
```

### Requirements

```bash
brew install protobuf flatbuffers
```

## Benchmarking

To run benchmarks, use the following command:

```bash
cargo bench
```

This should run all benchmarks (both criterion and iai) and output the results. Use the `--bench` command to run a specific benchmark or `-p` to run a specific package.

### Flamegraphs

To generate flamegraphs for a criterion test, use the following command

```bash
cargo flamegraph --bench {benchmark} -- --bench
```

`cargo flamegraph` can be installed using the following command:

```bash
cargo install flamegraph
```

This will generate a flamegraph in current directory. To view the flamegraph, use the following command:

```bash
open flamegraph.svg
```

Note that the flamegraph generated under macOS may not be accurate. To generate a more accurate flamegraph, build and run the benchmark under Linux. Using `orb` or a similar tool to get a Linux VM is recommended.

To run under orb, use the following command:
```bash

SKIP_PROTO_GEN=1 PATH=/usr/lib/linux-tools/*-generic:$PATH cargo bench --bench {benchmark} -- --bench

```

- `SKIP_PROTO_GEN=1` skips the protobuf generation step, which is not necessary for benchmarking and avoids having to install the protobuf compiler on the VM.
- ` PATH=/usr/lib/linux-tools/6.5.0-35-generic:$PATH` adds the Linux perf tools to the path. The default perf on Ubuntu tries to find the distribution-specific version of perf, which doesn't exist under orb. Putting the path to the perf tools in front of the path allows the perf tools to be found instead of the Ubuntu-specific wrapper script. Note that the version in the path must match whichever linux-tools package is installed on the VM.

### Coverage

To generate a coverage report, use the following command:

```bash
SKIP_PROTO_GEN=1 cargo tarpaulin --engine llvm -o html
```

This should output a coverage report `./tarpaulin-report.html` in the current directory.

`cargo tarpaulin` can be installed using the following command:

```bash
cargo install cargo-tarpaulin
```

Alternatively you can invoke `tarpaulin` via Docker as explained in the [tarpaulin documentation](https://github.com/xd009642/tarpaulin?tab=readme-ov-file#docker)

### Benchmarks

See [BENCHMARKS.md](BENCHMARKS.md) for more information
