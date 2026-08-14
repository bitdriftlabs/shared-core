# Agent Guidelines for Shared Core

- Primary instructions should be sourced from rust.instructions.md
- ALWAYS use `cargo nextest` commands for verification as it's faster
- Do not review files under `bd-proto/`; they are generated protobuf code.
- Format Rust and TOML changes with `cargo +nightly fmt` followed by
	`../scripts/format-toml.sh`. Use `../scripts/format-toml.sh --check` to verify TOML formatting
	without modifying files.

## Deterministic Tests

- Treat a timeout or intermittent failure as a test defect. Replace wall-clock sleeps and polling
	with lifecycle gates, channel events, mocks, or logical time that observe the transition under
	test.
- When using a manual or paused time provider, advance logical time only after the relevant task
	has registered its timer or reached a lifecycle gate. Do not make a test pass by extending a
	wall-clock deadline.
- Validate an edited test with `--nocache_test_results`. For a deflaked regression, prove it with
	uncached serial repetitions, for example:

	```sh
	../bazelw test --nocache_test_results --runs_per_test=25 --local_test_jobs=1 <target>
	```

- Add focused debug or trace logging around asynchronous lifecycle transitions when a test needs a
	new synchronization point. Keep logs that remain useful for production diagnosis.
