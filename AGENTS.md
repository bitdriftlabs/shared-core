# Agent Guidelines for Shared Core

## Build/Lint/Test Commands
- Build: `SKIP_PROTO_GEN=1 cargo build --workspace`
- Lint: `SKIP_PROTO_GEN=1 cargo clippy --workspace --bins --examples --tests -- --no-deps`
- Test (all): `SKIP_PROTO_GEN=1 cargo test` or `cargo nextest run`
- Test (single): `SKIP_PROTO_GEN=1 cargo test test_name` or `cargo nextest run test_name`
- Test (specific crate): `SKIP_PROTO_GEN=1 cargo test -p crate-name`
- Coverage: `SKIP_PROTO_GEN=1 cargo tarpaulin --engine llvm -o html`

## Code Style Guidelines
- Use 2-space indentation (no tabs)
- Max line width: 100 characters
- Error handling: Use `anyhow` for error types
- Use `#[cfg(test)]` and separate test files with `_test.rs` suffix
- Imports: Group imports with `One` style, module granularity, and `HorizontalVertical` layout
- Use workspace dependencies where available
- Edition: Rust 2024
- Make sure to run `cargo +nightly fmt` after making changes to apply default formatting rules.

## Test File Conventions
1. Test files should be placed adjacent to the implementation file they're testing
2. Test files should be named with a `_test.rs` suffix (e.g., `network_quality_test.rs`)
3. Link test files in the implementation file using:
   ```rust
   #[cfg(test)]
   #[path = "./file_name_test.rs"]
   mod tests;
   ```
4. Tests in the same file as the implementation code should be avoided
5. Test names should *not* start with `test_`, as this is redundant
