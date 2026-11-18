# Agent Guidelines for Shared Core

- Do not review files under `bd-proto/`; they are generated protobuf code.
- After changing a protobuf or FlatBuffer schema, regenerate bindings with `make protos`.
- Any new files need the license header. Run `ci/check_license.sh`.
- Do not block an async task on file I/O. Use `tokio::fs` or the async helpers in
  `bd-client-common::file_system`, and run blocking work such as building or syncing a
  memory-mapped file with `tokio::task::spawn_blocking`. Never hold a lock across that work.

## Validating Inside the Monorepo

- Run Bazel commands from the monorepo root, where `./bazelw` lives, not from `shared-core/`. Most
  shared-core test targets are `//shared-core/<crate>:test`; read the crate's `BUILD.bazel` before
  guessing a name.
- Format with `cargo +nightly fmt --all` from `shared-core/`; CI checks the same command.
- After changing a public signature, find its callers in shared-core and the monorepo with
  `git grep --recurse-submodules` from the monorepo root (plain `git grep` skips `shared-core/`).
  Lint every affected package, such as `./bazelw test --config=clippy //shared-core/...`, not only
  the touched ones.
- Shared-core CI runs Cargo on the latest stable and nightly toolchains, so it can occasionally
  report a lint newer than the monorepo's pinned Clippy. Fix those from the CI report.
- The root `clippy.toml` applies to every crate without its own. Do not add a per-crate copy.

## Deterministic Tests

- Treat a timeout or intermittent failure as a test defect. Replace wall-clock sleeps and polling
	with lifecycle gates, channel events, mocks, or logical time that observe the transition under
	test.
- When using a manual or paused time provider, advance logical time only after the relevant task
	has registered its timer or reached a lifecycle gate. Do not make a test pass by extending a
	wall-clock deadline.
- When the Bazel profile is selected, validate an edited test with `--nocache_test_results`. For a
  deflaked regression, prove it with uncached serial repetitions, for example:

	```sh
	./bazelw test --nocache_test_results --runs_per_test=25 --local_test_jobs=1 //shared-core/<crate>:test
	```

- Add focused debug or trace logging around asynchronous lifecycle transitions when a test needs a
	new synchronization point. Keep logs that remain useful for production diagnosis.
