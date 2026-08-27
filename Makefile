.PHONY: build
build: setup
	SKIP_PROTO_GEN=1 cargo build --workspace
	# `bd-proto` has feature-gated generated module trees that the workspace build does not exercise.
	SKIP_PROTO_GEN=1 cargo build -p bd-proto --all-features

.PHONY: protos
protos:
	if [ "$${FORCE_CARGO_PROTO_GEN:-}" = "1" ]; then \
		echo "Generating protos with the forced in-repo Cargo binary"; \
		cd bd-proto && cargo run --features codegen --bin generate-protos; \
	elif [ -x ../bazelw ]; then \
		echo "Generating protos with the monorepo Bazel target"; \
		../bazelw run //shared-core/bd-proto:generate-protos; \
	else \
		echo "Generating protos with the in-repo Cargo binary"; \
		cd bd-proto && cargo run --features codegen --bin generate-protos; \
	fi

.PHONY: check-protos
check-protos: protos
	git diff --exit-code -- bd-proto/src/protos bd-proto/src/flatbuffers

.PHONY: setup
setup:
	ci/setup.sh

.PHONY: clippy
clippy: setup
	ci/check_license.sh
	SKIP_PROTO_GEN=1 SKIP_FILE_GEN=1 cargo clippy --workspace --bins --examples --tests -- --no-deps

# Leaving the below loop around to help with debugging flakes if needed.
.PHONY: test
test: setup
	for i in $(shell seq 1 1); do \
   	echo "Running test iteration $$i..."; \
		RUST_BACKTRACE=1 SKIP_PROTO_GEN=1 SKIP_FILE_GEN=1 RUST_LOG=error cargo nextest run || exit 1; \
	done

.PHONY: benchmark-json-matcher
benchmark-json-matcher: setup
	bd-workflow-bench/json_path.sh
