# `mutex_lock_must_unwrap`

### What it does

Requires every `std::sync::Mutex::lock()` call to be immediately followed by `unwrap()`.

### Why is this bad?

This codebase treats a poisoned standard mutex as a programming error. Continuing after poisoning
can expose state left inconsistent by a panic while the mutex was held.

### Example

```rust
let guard = mutex.lock().expect("mutex poisoned");
```

Use instead:

```rust
let guard = mutex.lock().unwrap();
```
