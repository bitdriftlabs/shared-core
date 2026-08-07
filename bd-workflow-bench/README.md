# Workflow Benchmarker

`bd-workflow-bench` replays an exported client log corpus through the real workflow engine and
writes local timing reports. It never reads data from the network. Its Criterion and Callgrind
wrappers default to a small, checked-in synthetic corpus; local customer config and log files can
be supplied explicitly and are never checked in.

```sh
cargo run -p bd-workflow-bench --release -- \
  --config /path/to/config.json \
  --logs /path/to/session.json \
  --output-dir /path/to/workflow-bench-report
```

## Criterion wall-time benchmarks

`criterion.sh` measures full-corpus replay wall time and throughput. Corpus parsing and engine
startup happen outside the timed operation, while each sample begins with a fresh workflow engine
and in-memory state. The default is the checked-in synthetic corpus, making it suitable for
regression tracking in CI or on a developer machine:

```sh
./bd-workflow-bench/criterion.sh
./bd-workflow-bench/criterion.sh -- --save-baseline main
```

To benchmark a live corpus, supply both paths. The paths are passed through environment variables
instead of becoming repository configuration:

```sh
./bd-workflow-bench/criterion.sh \
  --config /path/to/config.json \
  --logs /path/to/session.json
```

Use Criterion results for wall-clock comparisons and regression baselines. Use Callgrind to explain
where the CPU instructions are spent.

## Callgrind CPU profiling on Linux

`profile.sh` builds an optimized binary with debug symbols using the workspace's `bench` profile,
then runs the real corpus replay under Valgrind Callgrind. This follows the repository's Linux
benchmarking approach and produces a KCachegrind-compatible profile with exact instruction counts,
not sampled timings. Instrumentation brackets each `process_event` call, excluding corpus parsing,
engine startup, report writing, and benchmark timing. The engine and in-memory state are reset
before every replay.

Run this inside a Linux VM. Any supplied input paths must exist in the VM. For the one-time
OrbStack/macOS setup, including KCachegrind and X11 forwarding, follow
[the OrbStack Linux VM setup](../BENCHMARKS.md#orbstack-linux-vm-setup-macos).

```sh
# Profile the fixed, checked-in corpus.
./bd-workflow-bench/profile.sh \
  --callgrind-output /tmp/workflow-bench.callgrind.out \
  --open

# Profile a supplied live corpus.
./bd-workflow-bench/profile.sh \
  --callgrind-output /tmp/workflow-bench.callgrind.out \
  --open \
  -- \
  --config /path/to/config.json \
  --logs /path/to/session.json \
  --output-dir /path/to/workflow-bench-report
```

Callgrind does not need repeated samples, so the wrapper defaults to one replay. Pass `--repeat N`
to make several fresh-engine passes. Profiling uses `--summary-only`, so `per-log.jsonl` is empty
instead of containing one record for every replayed log. `summary.json` includes `source_log_count`,
`replay_count`, and the total `log_count` evaluated across all passes. Open an existing profile with
`kcachegrind /tmp/workflow-bench.callgrind.out` when `--open` is not suitable.

Callgrind's elapsed time includes Valgrind instrumentation overhead and must not be compared with
Criterion's wall-clock measurements.

The config file is a JSON `ApiResponse` whose `configurationUpdate` message contains the workflow
configuration.

The log file is NDJSON: one exported log object per line. Records require `id`, `timestamp`,
`session_id`, `log_level`, `message`, and `fields.fields`; `log_type` defaults to `NORMAL`.
Exported field values use the protobuf `Data` shapes such as `string_data` and `int_data`.

The output directory contains `per-log.jsonl`, with source line, log ID, and evaluation duration,
and `summary.json`, with exact latency percentiles and the slowest records. Each slowest record
includes its type, message (truncated to 100 characters), and field count. Existing reports are
left untouched unless `--overwrite` is supplied.

Every per-log and slow-log record also includes the engine outcome available from
`WorkflowsEngineResult`: session boundaries, tracing state, triggered flushes, screenshot requests,
injected logs, and debug-workflow state. `summary.json` aggregates those outcomes across the run.

Pass `--replay-count N` to repeat the corpus in a normal benchmark run. The engine and in-memory
state are reset before each pass; per-log records include `replay_iteration` to distinguish them.

The timer covers only workflow-engine evaluation of each input log. It excludes file I/O, parsing,
report writing, persistence, upload handling, and generated-log reinjection.
