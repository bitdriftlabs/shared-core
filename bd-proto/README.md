# bd-proto

Crate containing generated protobuf and FlatBuffer bindings. The generated files are checked in so
SDK consumers can build through Bazel and Rust Analyzer can resolve the bindings. After changing a
source schema, regenerate them from the `shared-core` root with `make protos`. In a monorepo
checkout, this runs the bzlmod-provided generator; in a standalone checkout, it runs the Cargo
generator and requires matching `protoc` and `flatc` tools on `PATH`.

Set `FORCE_CARGO_PROTO_GEN=1` to exercise the Cargo path from a monorepo checkout.

The equivalent monorepo-root command is `./bazelw run //shared-core/bd-proto:generate-protos`.
