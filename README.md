# intrusivelock

Intrusive locks for Rust — locks that live inside the memory region they protect.

This is useful when the lock and the data must be co-located in the same allocation, for example inside an `mmap`-backed region, a shared-memory arena, or a custom cache-line-aligned struct.

## Features

- **`SpinRwLock`** — a standalone, 8-byte reader-writer spin lock (`AtomicU64`). Unlike `std::sync::RwLock`, it does not wrap data in an `UnsafeCell<T>`. You manage the protected data yourself.
- **`IntrusiveSpinRwLock<T>`** — a safe wrapper that pairs a value `T` (which contains a `SpinRwLock`) with RAII read/write guards that `Deref`/`DerefMut` into `T`.
- **`try_read` / `try_write`** — non-blocking variants that return `None` on contention.
- **Writer-preferring** — once a writer is pending, new readers are blocked until the writer completes. This prevents writer starvation under read-heavy workloads.

### Design notes

- Being a spin lock, it is intended for **low-to-moderate contention** with short critical sections.
- It is **not** suitable for multi-process synchronization (which requires kernel-assisted locks like `pthread_rwlock` or futexes).
- There is **no poisoning**: if a thread panics inside a critical section, subsequent lock acquisitions will succeed without any indication of the prior panic.

## Usage

### Intrusive (recommended)

Embed the lock inside your data structure and implement `HasIntrusiveSpinRwLock`:

```rust
use intrusivelock::spin_rwlock::*;

#[derive(Default)]
struct MyData {
    _lock: SpinRwLock,
    counter: u64,
}

unsafe impl HasIntrusiveSpinRwLock for MyData {
    fn lock(&self) -> &SpinRwLock {
        &self._lock
    }
}

type Protected = IntrusiveSpinRwLock<MyData>;

let data = Protected::default();

// Write access — returns a guard that DerefMuts into MyData
data.write().counter += 1;

// Read access
assert_eq!(data.read().counter, 1);

// Non-blocking
if let Some(guard) = data.try_write() {
    // got exclusive access
}
```

### Standalone lock

Use `SpinRwLock` directly when you need the lock separate from the data (e.g. protecting a `static mut`):

```rust
use intrusivelock::spin_rwlock::SpinRwLock;

let lock = SpinRwLock::new();

{
    let _r = lock.read();   // shared access
}
{
    let _w = lock.write();  // exclusive access
}
```

## API

### `SpinRwLock`

| Method | Description |
|--------|-------------|
| `new() -> Self` | Create a new unlocked `SpinRwLock` |
| `read() -> ReadGuard` | Acquire shared read access (spins until available) |
| `write() -> WriteGuard` | Acquire exclusive write access (spins until available) |
| `try_read() -> Option<ReadGuard>` | Try to acquire read access; returns `None` if contended |
| `try_write() -> Option<WriteGuard>` | Try to acquire write access; returns `None` if contended |

### `IntrusiveSpinRwLock<T>`

| Method | Description |
|--------|-------------|
| `new(value: T) -> Self` | Wrap a value containing an embedded lock |
| `read() -> IntrusiveReadGuard<T>` | Shared access; guard derefs to `&T` |
| `write() -> IntrusiveWriteGuard<T>` | Exclusive access; guard derefs to `&mut T` |
| `try_read() -> Option<IntrusiveReadGuard<T>>` | Non-blocking read attempt |
| `try_write() -> Option<IntrusiveWriteGuard<T>>` | Non-blocking write attempt |

## `no_std` support

The crate is `no_std`-compatible. The `std` feature is enabled by default (providing `thread::yield_now()` in the spin backoff). To use in a `no_std` environment, disable default features:

```toml
[dependencies]
intrusivelock = { version = "0.1", default-features = false }
```

Under `no_std`, the backoff strategy falls back to `core::hint::spin_loop()` instead of yielding.
