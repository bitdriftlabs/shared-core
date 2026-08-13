# Workflow benchmark fixtures

Each checked-in corpus lives in its own directory:

```text
fixtures/<corpus-name>/
  manifest.json
  config.json[.zst]
  logs.ndjson[.zst]
```

`manifest.json` declares the format version, corpus name, and the config and log filenames. The
benchmarker resolves checked-in fixture paths only through those manifests.

`small` is the existing synthetic corpus, retained as raw JSON/NDJSON for easy inspection.
`large` is an anonymized capture and is zstd-compressed to keep the checked-in size manageable.
Criterion benchmarks both corpora by default; the Callgrind wrapper defaults to `small` for a
quick, deterministic profile.
