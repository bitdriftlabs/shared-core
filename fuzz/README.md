# Fuzzing

## Running

Run fuzz targets through Bazel. Pass libFuzzer flags after `--`; use an absolute corpus path
because Bazel executes the binary from its runfiles directory.

```bash
mkdir -p .tmp/fuzz/buffer_corruption_fuzz_test
cp -R shared-core/fuzz/corpus/buffer_corruption_fuzz_test/. \
	.tmp/fuzz/buffer_corruption_fuzz_test/

./bazelw run --config=fuzz //shared-core/fuzz:buffer_corruption_fuzz_test -- \
	-max_total_time=300 \
	"$PWD/.tmp/fuzz/buffer_corruption_fuzz_test"
```

This preserves the checked-in corpus while allowing libFuzzer to save newly interesting inputs
under `.tmp/fuzz/`. Re-run a saved input with:

```bash
./bazelw run --config=fuzz //shared-core/fuzz:buffer_corruption_fuzz_test -- \
	"$PWD/.tmp/fuzz/buffer_corruption_fuzz_test/<input>"
```

Cargo remains available for the legacy corpus-merging script, which requires the nightly toolchain
and `cargo-fuzz`:

```bash
cargo install cargo-fuzz
fuzz/scripts/run_fuzzer.sh mpsc_buffer_fuzz_test
fuzz/scripts/run_fuzzer.sh spsc_buffer_fuzz_test
fuzz/scripts/run_fuzzer.sh buffer_corruption_fuzz_test
```
