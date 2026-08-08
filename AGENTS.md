# Agent Guidelines for Shared Core

- Primary instructions should be sourced from rust.instructions.md
- ALWAYS use `cargo nextest` commands for verification as it's faster
- Do not review files under `bd-proto/`; they are generated protobuf code.
- Format Rust and TOML changes with `cargo +nightly fmt` followed by
	`../scripts/format-toml.sh`. Use `../scripts/format-toml.sh --check` to verify TOML formatting
	without modifying files.
