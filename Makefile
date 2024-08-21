.PHONY: build
build: setup
	cargo build --workspace

.PHONY: setup
setup:
	ci/setup.sh

.PHONY: clippy
clippy: setup
	ci/check_license.sh
	cargo clippy --workspace --bins --examples --tests -- --no-deps

.PHONY: test
test: setup
	RUST_LOG=off cargo test --workspace
