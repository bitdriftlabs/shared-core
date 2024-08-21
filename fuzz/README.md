# Fuzzing

## Setup

For full info see [here](https://rust-fuzz.github.io/book/cargo-fuzz/setup.html).

```
cargo install cargo-fuzz
```

## Running

```
fuzz/scripts/run_fuzzer.sh mpsc_buffer_fuzz_test
fuzz/scripts/run_fuzzer.sh spsc_buffer_fuzz_test
fuzz/scripts/run_fuzzer.sh buffer_corruption_fuzz_test
```
