.PHONY: build
build: setup
  SKIP_PROTO_GEN=1 cargo build --workspace

.PHONY: setup
setup:
	ci/setup.sh

.PHONY: clippy
clippy: setup
	ci/check_license.sh
	SKIP_PROTO_GEN=1 cargo clippy --workspace --bins --examples --tests -- --no-deps

# Leaving the below loop around to help with debugging flakes if needed.
.PHONY: test
test: setup
	for i in $(shell seq 1 1); do \
  	echo "Running test iteration $$i..."; \
		RUST_BACKTRACE=1 SKIP_PROTO_GEN=1 RUST_LOG=error cargo nextest || exit 1; \
	done
