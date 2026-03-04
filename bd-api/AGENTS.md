# bd-api API stream and reconnect flow

This document explains how `bd-api` manages stream lifecycle and reconnect behavior, with emphasis on all conditions that:

- prevent reconnecting,
- delay reconnecting,
- reconnect immediately,
- or alter reconnect pacing.

## Scope and key files

- `src/api.rs`: main API stream state machine.
- `src/reconnect.rs`: persisted reconnect delay state.
- `src/network_quality.rs`: offline/unknown/online transitions used during reconnect.
- `src/api_test.rs`, `src/reconnect_test.rs`: behavior coverage for edge cases.
- Runtime defaults come from `../bd-runtime/src/runtime.rs`.

## High-level lifecycle

1. `Api::start()` loads persisted config, checks kill state (`maybe_kill_client()`), then enters `maintain_active_stream()`.
2. `maintain_active_stream()` loops forever:
   - decides whether to wait before reconnect (min reconnect interval gate),
   - checks kill state,
   - updates backoff policy if runtime changed,
   - runs one stream attempt via `do_stream(...)`,
   - if `do_stream` returns an error, applies exponential reconnect backoff (`do_reconnect_backoff`).
3. `do_stream(...)`:
   - starts platform stream,
   - sends handshake,
   - waits for handshake or closure,
   - on success enters request/response loop,
   - exits on stream closure, idle timeout, or error.

## States that stop reconnect entirely (until restart/time expiry)

### 1) Client kill state (`client_killed == true`)

When this flag is true, `maintain_active_stream()` enters `pending()` forever (no more reconnect attempts in this process lifetime).

How it becomes true:

- Active kill file on startup:
  - kill file exists,
  - `kill_until > now`,
  - stored API key hash matches current API key hash.
- Runtime generic kill duration (`client_kill.generic_kill_duration_ms`) is non-zero on startup:
  - `maybe_kill_client()` writes kill file and sets `client_killed`.
- Unauthenticated handshake failure:
  - `wait_for_handshake()` gets `ErrorShutdown` with gRPC code `Unauthenticated`,
  - `do_stream()` writes kill file for `client_kill.unauthenticated_kill_duration_ms` (default 1 day),
  - loop iteration ends, next iteration sees `client_killed`, then parks forever.

Notes:

- Kill state clears cached runtime/config for safety.
- Kill file expiry or API key change allows startup reconnect again on a future process start.
- Corrupt kill file is treated as read failure and ignored (warn + continue connecting).

## Conditions that delay reconnect attempts

Reconnect delay has **two independent layers**:

1. **Min reconnect interval gate** (based on last successful connectivity event).
2. **Exponential backoff / retry-after** (after stream errors).

### A) Min reconnect interval gate (pre-connect gate)

Implemented by `ReconnectState::next_reconnect_delay(...)` in `src/reconnect.rs`.

`ReconnectState` now persists **both**:

- `last_connected_at` (used for min reconnect interval), and
- `next_try_not_before` (absolute minimum reconnect time derived from backoff/retry-after).

Baseline (`last_connected_at`) is updated when:

- handshake succeeds,
- or data upload is sent on an active stream,
- or a data upload received during reconnect delay is flushed immediately after handshake.

Reconnect is delayed until the later of:

- `last_connected_at + min_reconnect_interval`, and
- `next_try_not_before`.

Important behaviors:

- No prior connectivity event => no delay.
- Negative/zero remaining delay => no delay.
- Delay is capped to `min_reconnect_interval` to protect against clock skew/time rollback.
- During this delay window, network quality is forced to `Unknown` (not `Offline`).
- If new data arrives on `data_upload_rx` while waiting, delay is interrupted and reconnect starts early.
- Because `next_try_not_before` is persisted in key-value storage, this minimum retry time now
  survives process restart.

Runtime sources:

- Normal mode: `api.min_reconnect_interval_ms` (default 0ms).
- Sleep mode override: `sleep_mode.api_min_reconnect_interval_ms` (default 15m).

### B) Error backoff and server retry-after

Applied only when `do_stream()` returns `Err(ApiError::...)`.

`do_reconnect_backoff(min_retry_after)` sleeps for:

- exponential backoff next value,
- optionally max(backoff, `retry_after`) when server supplies retry-after from `ErrorShutdown.rate_limited.retry_after`.

Before sleeping, API persists `next_try_not_before = now + selected_delay` to reconnect state.
This makes both backoff-derived and retry-after-derived minimum retry times restart-stable.

Backoff runtime defaults:

- initial upper bound: 500ms,
- max: 20m,
- multiplier: 5.0x,
- jittered by `bd_backoff` (runtime comment indicates attempts are randomized up to current backoff).

Backoff reset behavior:

- If a stream lived for >1 minute after handshake and then closes **without** retry-after, backoff is reset and stream path returns `Ok(())` (no immediate backoff sleep on that iteration).
- Short-lived failing streams do not reset backoff.

## Conditions that reconnect immediately (or near-immediately)

- No kill state, no min reconnect delay, and stream attempt is allowed.
- Stream exits cleanly (`Ok(())`) due idle timeout: no error backoff is applied (but min reconnect gate may still delay).
- Stream closed after >1 minute uptime and no retry-after: backoff reset path returns `Ok(())`, so no error backoff for that transition.
- Data upload arriving during min reconnect delay causes immediate connect attempt (interrupts wait).

## Data-idle timeout interactions

`do_stream()` applies an idle timer using runtime:

- normal: `api.data_idle_timeout_interval_ms` (default 5m),
- sleep mode: `sleep_mode.api_data_idle_timeout_interval_ms` (default 15s).

On idle timeout:

- increments `api:data_idle_timeout`,
- stream exits with `Ok(())` (not an error),
- no exponential backoff,
- subsequent reconnect may still be blocked by min reconnect interval gate.

Data upload activity resets idle timeout baseline (`last_data_received_at`) and records connectivity event.

## Handshake-specific reconnect outcomes

### Handshake succeeds

- network quality set to `Online`,
- reconnect state records connectivity event,
- disconnected timer cleared,
- stream enters normal request/response loop.

### Stream closes before handshake

- treated as connect failure (`remote_connect_failure` counter),
- returns `ApiError::StreamClosure(retry_after?)`,
- exponential backoff applies (plus retry-after floor if provided).

### Non-auth `ErrorShutdown` before handshake

- increments `error_shutdown_total`,
- treated as stream closure (with optional retry-after),
- reconnect continues with backoff.

### `Unauthenticated` before handshake

- kill file is written for unauthenticated kill duration,
- client transitions to non-reconnecting killed state.

## Network quality during reconnect

In `maintain_active_stream()`:

- During intentional min reconnect waiting: `Unknown`.
- When not intentionally waiting and disconnected for <= 15s grace: `Unknown`.
- When not intentionally waiting and disconnected for > 15s: `Offline`.
- After successful handshake: `Online`.

Additional side effect when entering `Offline` after grace:

- runtime/config caches are marked safe once per offline period (`config_marked_safe_due_to_offline`).

## Conditions that mark workflow/runtime config as "safe"

This API flow has two config pipelines:

- **Workflow/config pipeline** via `ClientConfigurationUpdate` (`config_updater`), implemented in
  logger config (`bd-logger/src/client_config.rs`).
- **Runtime pipeline** via `ConfigLoader` (`runtime_loader`) in `bd-runtime/src/runtime.rs`.

Both pipelines use `SafeFileCache` semantics for persisted startup config.

### What "safe" means in `SafeFileCache`

`SafeFileCache::mark_safe()` does two things:

- sets in-memory `cached_config_validated = true` (idempotent guard),
- persists retry count back to `0` (best effort).

Why this matters: cached configs are loaded at startup with a retry counter. If startup keeps
re-applying cached config without being marked safe, retry count eventually reaches
`MAX_RETRY_COUNT` and cache loading is disabled to break crash loops.

### Trigger 1: Successful handshake with up-to-date status bits

After handshake success, `Api::do_stream()` calls:

- `runtime_loader.on_handshake_complete(configuration_update_status)`
- `config_updater.on_handshake_complete(configuration_update_status)`

Each side marks cache safe **only** if its handshake bit is present:

- Runtime safe when `configuration_update_status & HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE != 0`.
- Workflow/config safe when
  `configuration_update_status & HANDSHAKE_FLAG_CONFIG_UP_TO_DATE != 0`.

This is the normal "server confirms current nonce is good" safe-marking path.

### Trigger 2: Offline fallback after disconnect grace period

In `Api::maintain_active_stream()`, when **not** intentionally waiting on min reconnect delay,
once disconnected time exceeds `DISCONNECTED_OFFLINE_GRACE_PERIOD` (15s), API sets network quality
to `Offline`. On the first such transition, it also calls:

- `runtime_loader.mark_safe().await`
- `config_updater.mark_safe().await`

guarded by `config_marked_safe_due_to_offline`.

Important behavior:

- this offline fallback path is unconditional (does not require handshake bits),
- it runs at most once per `Api` instance lifetime (the guard is not reset in current code),
- during intentional min reconnect waiting, this offline path is skipped and network quality stays
  `Unknown`.

### Related nuance: successful config apply also refreshes cache safety state

When new runtime or workflow config is applied and cached via `SafeFileCache::cache_update(...)`,
the cache path also sets `cached_config_validated = true` and persists retry count `0`.

So, even outside explicit `mark_safe()` calls, successful update application refreshes the same
crash-loop protection state for newly written cached config.

### Trigger 3: Explicit server retry-after (`ErrorShutdown.rate_limited.retry_after`)

When API receives `ErrorShutdown` containing `rate_limited.retry_after` (both before and after
handshake), it now also calls:

- `runtime_loader.mark_safe().await`
- `config_updater.mark_safe().await`

This prevents cache churn during load shedding windows and ensures cached runtime/workflow config
remains safe across process restarts while reconnect is intentionally delayed.

## Practical decision matrix

Given a disconnect, reconnect behavior is:

1. If killed => never reconnect in current process.
2. Else, if min reconnect delay active => wait (unless upload arrives, then reconnect early).
3. Attempt stream.
4. If stream result is:
   - `Ok(())` => no error backoff, loop continues.
   - `Err(StreamClosure/Other)` => sleep exponential backoff; if stream closure has retry-after, delay is at least that value.
5. Repeat.

## Test-backed edge cases covered

- Reconnect delay state persists in KV store and survives restart (`reconnect_test.rs`).
- Negative delay returns immediate reconnect.
- Excessive delay from clock rollback is capped.
- Idle timeout + min reconnect interval can hold reconnect for long periods.
- Upload during reconnect wait triggers immediate reconnection.
- Retry-after is honored both before and after handshake.
- Retry-after and exponential backoff minimum retry timing now persist across restart.
- Retry-after responses now mark runtime/workflow config cache safe.
- Unauthenticated before handshake kills client for default one day.
- Corrupt kill file does not permanently block reconnect.
