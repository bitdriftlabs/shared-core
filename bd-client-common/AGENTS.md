# bd-client-common SafeFileCache behavior

This document explains `src/safe_file_cache.rs` with emphasis on crash-loop prevention,
TimeProvider-based time checks, and cross-crate integration.

## Purpose

`SafeFileCache<T>` persists compressed protobuf updates and reuses them on startup when safe. It is
designed to:
- improve startup behavior when network/config service is unavailable,
- recover from partial/corrupt cache state,
- avoid infinite startup crash loops from repeatedly applying a bad cached config.

Current uses:
- runtime updates (`bd-runtime/src/runtime.rs`)
- logger configuration updates (`bd-logger/src/client_config.rs`)

## Time model

The 4-hour bypass logic is wall-clock based and always reads time from a `bd_time::TimeProvider`.

Constructors:
- `SafeFileCache::new(...)` uses `bd_time::SystemTimeProvider`.
- `SafeFileCache::new_with_time_provider(...)` accepts `Arc<dyn TimeProvider>`.

This enables deterministic tests by injecting `bd_time::TestTimeProvider` and advancing time
without mutating cache files directly.

## On-disk layout

Per cache name (for example `runtime` or `config`) the directory is:
`<sdk_directory>/<name>/`

Files:
- `protobuf.pb`: compressed protobuf payload
- `retry_count`: one byte (`u8`)
- `last_nonce`: nonce bytes + CRC32 trailer
- `last_successful_cache_at`: optional `i64` unix seconds + CRC32 trailer

Interpretation:
- `last_successful_cache_at` missing is treated as legacy/unknown and therefore old enough to allow
   bypass when evaluating same-nonce crash-loop blocking.

## In-memory state (`LockedState`)

- `cached_config_validated`: process-local idempotence guard for `mark_safe()`
- `cached_nonce`: nonce loaded at startup
- `current_retry_count`: retry count loaded at startup
- `last_successful_cache_at`: timestamp loaded at startup if present

Important: crash-loop guard decisions in `cache_update(...)` use this in-memory state. If a caller
needs guard behavior based on disk state, it must first run startup load path
(`handle_cached_config()`), which populates `LockedState`.

## Startup algorithm (`handle_cached_config`)

`handle_cached_config()`:
1. Calls `try_load_cached_config()`.
2. On unexpected error, logs and requests full cache reset.
3. If reset requested, recreates cache directory.
4. Returns cached protobuf if available.

`try_load_cached_config()`:
1. Requires `retry_count`, `protobuf.pb`, and `last_nonce` to exist.
2. Parses `retry_count` (`len == 1` and value `<= MAX_RETRY_COUNT`).
3. Reads and CRC-validates `last_nonce`.
4. If `last_successful_cache_at` exists, reads and CRC-validates it.
5. Stores loaded values in `LockedState`.
6. If `retry_count >= MAX_RETRY_COUNT`, returns `None` without reading protobuf.
7. Otherwise writes `retry_count + 1` before returning protobuf.

Why increment before apply:
- if process crashes during cached apply, next startup observes incremented retry state,
- repeated crashes eventually hit max and disable cached load.

## Update algorithm (`cache_update`)

Inputs:
- `compressed_protobuf`
- `version_nonce`
- `apply_fn`

Evaluation order:
1. Read `now_unix_seconds` from `time_provider.now().unix_timestamp()`.
2. Read guard state from `LockedState`:
    - `in_suspected_crash_loop = current_retry_count >= MAX_RETRY_COUNT`
    - `same_nonce = cached_nonce == version_nonce`
    - `bypass_elapsed = is_bypass_elapsed(last_successful_cache_at, now_unix_seconds)`
3. Refuse update only if:
    - `in_suspected_crash_loop && same_nonce && !bypass_elapsed`
4. Allow update if guard does not refuse.
    - If `in_suspected_crash_loop && same_nonce && bypass_elapsed`, this is the 4-hour bypass case.
5. Run `apply_fn`. If it fails, no cache files are updated.
6. Best-effort writes:
    - `last_nonce`
    - `protobuf.pb`
    - `last_successful_cache_at = now_unix_seconds`
7. Update in-memory state:
    - `cached_config_validated = true`
    - `cached_nonce = version_nonce`
    - `current_retry_count = 0`
    - `last_successful_cache_at = Some(now_unix_seconds)`
8. Best-effort persist `retry_count = 0`.

## 4-hour bypass rule

`is_bypass_elapsed(last_successful_cache_at, now_unix_seconds)`:
- returns `true` when no timestamp is present,
- otherwise returns `now - last_successful_cache_at >= 4 hours` (using saturating subtraction).

Operationally this means:
- same nonce is blocked while at max retry and within the 4-hour window,
- same nonce is allowed again after 4 hours,
- once allowed and successfully applied, retry count is reset and normal behavior resumes.

## mark_safe behavior

`mark_safe()` is idempotent per process:
- flips `cached_config_validated` only once,
- best-effort writes `retry_count = 0`.

It does not run `apply_fn`; it only marks cached state as safe for future startups.

## Cross-crate usage

### `bd-runtime` (`bd-runtime/src/runtime.rs`)
- `ConfigLoader::new()` uses system time.
- `ConfigLoader::new_with_time_provider(...)` injects provider into `SafeFileCache<RuntimeUpdate>`.
- Startup path calls `try_load_persisted_config()`.

### `bd-logger` config (`bd-logger/src/client_config.rs`)
- `Config::new_with_time_provider(...)` injects provider into
   `SafeFileCache<ConfigurationUpdate>`.
- `Config::new(...)` exists only in tests and defaults to system time.

### `bd-logger` builder (`bd-logger/src/builder.rs`)
- Resolves one `Arc<dyn TimeProvider>` (provided or default).
- Passes the same provider to both runtime loader and config updater cache wiring.

### `bd-api` (`bd-api/src/api.rs`)
- Still marks runtime/config caches safe on handshake/offline/rate-limit conditions.
- This interacts with retry reset but does not alter `cache_update` guard logic.

## Testing guidance

- Prefer `TestTimeProvider` + `new_with_time_provider(...)` for deterministic timeout behavior.
- Avoid rewriting `last_successful_cache_at` in normal behavior tests.
- For guard assertions, ensure startup load path (`handle_cached_config()`) has populated
   `LockedState` before expecting same-nonce refusal.

## Known tradeoffs

- `retry_count` is not checksummed.
- File persistence is not atomic rename-transactional.
- File write order is `last_nonce`, `protobuf.pb`, `last_successful_cache_at`; crash mid-sequence
   can leave partial state (handled by defensive reset paths).
- `cached_config_validated` is a local idempotence flag, not a full state machine.
