# Intrusive Locks for Rust

The purpose of this crate is to provide intrusive locks (locks that are contained inside the memory region 
they protect). For example, data that's kept in an `mmap` - where you want the lock and the data it protects 
to live side-by-side.

Currently only `SpinRwLock` is implemented. Unlike rust's canonical locks/mutexes, it does not hold an `UnsafeCell<T>` - 
it's a standalone lock object. You can use `IntrusiveSpinRwLock` to encompass the value to protect, which must contain
the lock inside (use `HasIntrusiveSpinRwLock` to return the contained lock).

Notes:
* Being a spin lock, it is expected to work under low-to-moderate contention.
* It is not intended for multiprocessing, only multithreading. You need kernel support 

## Example
```rust
#[derive(Default)]
#[repr(C, align(64))]
struct _MyCacheLine {
    val2: u128,
    val1: u64,
    _lock: SpinRwLock,
    val3: [u8; 32],
}

unsafe impl HasIntrusiveSpinRwLock for _MyCacheLine {
    fn lock<'a>(&'a self) -> &'a SpinRwLock {
        &self._lock
    }
}

type MyCacheLine = IntrusiveSpinRwLock<_MyCacheLine>;

let cache_line = MyCacheLine::new(_MyCacheLine::default());

cache_line.write().val2 = 12345;
assert_eq!(cache_line.read().val2, 12345);
```
