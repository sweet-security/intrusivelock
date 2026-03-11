use core::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU64, Ordering},
};

#[repr(transparent)]
pub struct SpinRwLock {
    // [0..62) - current number of readers
    // 62 - writer pending; readers and potential writers will spin
    // 63 - writer holding; readers and potential writers will spin
    state: AtomicU64,
}

const READERS_BITS: u32 = 62;
const READERS_MASK: u64 = (1u64 << READERS_BITS) - 1;

const WRITER_PENDING: u64 = 1u64 << 62;
const WRITER_HOLDING: u64 = 1u64 << 63;
const WRITER_MASK: u64 = WRITER_PENDING | WRITER_HOLDING;

struct Backoff {
    spins: u32,
}

impl Backoff {
    #[inline(always)]
    fn new() -> Self {
        Self { spins: 0 }
    }

    #[inline(always)]
    fn spin(&mut self) {
        if self.spins < 200 {
            // Phase 1: busy-spin for the first 200 attempts.
            core::hint::spin_loop();
            self.spins += 1;
        } else {
            // Phase 2: yield then do 10 more spins before yielding again.
            #[cfg(feature = "std")]
            std::thread::yield_now();
            #[cfg(not(feature = "std"))]
            core::hint::spin_loop();
            self.spins = 190;
        }
    }
}

impl SpinRwLock {
    #[allow(clippy::declare_interior_mutable_const)]
    pub const INIT: Self = Self {
        state: AtomicU64::new(0),
    };

    pub const fn new() -> Self {
        Self::INIT
    }

    pub fn read(&self) -> ReadGuard<'_> {
        self._read();
        ReadGuard { lock: self }
    }

    pub fn try_read(&self) -> Option<ReadGuard<'_>> {
        if self._try_read() {
            Some(ReadGuard { lock: self })
        } else {
            None
        }
    }

    fn _try_read(&self) -> bool {
        let s = self.state.load(Ordering::Relaxed);
        if (s & WRITER_MASK) != 0 {
            return false;
        }

        let readers = s & READERS_MASK;
        if readers == READERS_MASK {
            return false;
        }

        let new = (s & !READERS_MASK) | (readers + 1);
        self.state
            .compare_exchange(s, new, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    fn _read(&self) {
        let mut s = self.state.load(Ordering::Relaxed);
        let mut backoff = Backoff::new();

        loop {
            if (s & WRITER_MASK) != 0 {
                backoff.spin();
                s = self.state.load(Ordering::Relaxed); // Must reload if we just spun waiting for writer
                continue;
            }

            let readers = s & READERS_MASK;
            if readers == READERS_MASK {
                panic!("SpinRwLock reader count overflow");
            }

            let new = (s & !READERS_MASK) | (readers + 1);

            match self
                .state
                .compare_exchange_weak(s, new, Ordering::Acquire, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => {
                    // We failed the CAS. Instead of a full reload, we just use the 'actual' value
                    // on the next iteration immediately.
                    s = actual;
                    backoff.spin();
                }
            }
        }
    }

    #[inline]
    fn unlock_read(&self) {
        // Release: publish critical-section writes before decrementing.
        let prev = self.state.fetch_sub(1, Ordering::Release);

        debug_assert!((prev & READERS_MASK) != 0, "read_unlock with no readers");
        debug_assert!((prev & WRITER_HOLDING) == 0, "reader while writer holding");
        // It's fine if WRITER_PENDING is set: reader is just draining.
    }

    pub fn write(&self) -> WriteGuard<'_> {
        self._write();
        WriteGuard { lock: self }
    }

    pub fn try_write(&self) -> Option<WriteGuard<'_>> {
        if self._try_write() {
            Some(WriteGuard { lock: self })
        } else {
            None
        }
    }

    fn _try_write(&self) -> bool {
        self.state
            .compare_exchange(0, WRITER_MASK, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    fn _write(&self) {
        let mut backoff = Backoff::new();
        let mut s = self.state.load(Ordering::Relaxed);

        // Phase A: acquire "pending" (only one pending writer at a time)
        loop {
            // If another writer is already pending or holding, wait.
            if (s & WRITER_PENDING) != 0 {
                backoff.spin();
                s = self.state.load(Ordering::Relaxed);
                continue;
            }

            let new = s | WRITER_PENDING;

            match self
                .state
                .compare_exchange_weak(s, new, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    // We successfully claimed the pending bit.
                    // Update `s` to our new state to carry it right into Phase B.
                    s = new;
                    break;
                }
                Err(actual) => {
                    // CAS failed because state changed. Thread the actual state back
                    // into the loop and backoff without a redundant load.
                    s = actual;
                    backoff.spin();
                }
            }
        }

        // Reset our backoff strategy for the next waiting phase
        let mut backoff = Backoff::new();

        // Phase B: wait for readers to drain, then acquire holding
        loop {
            debug_assert!((s & WRITER_PENDING) != 0, "lost WRITER_PENDING ownership");

            // Wait until there are no readers and no other holding writer
            if (s & READERS_MASK) != 0 || (s & WRITER_HOLDING) != 0 {
                backoff.spin();
                s = self.state.load(Ordering::Relaxed);
                continue;
            }

            // Take holding; invariant: holding implies pending.
            let new = s | WRITER_HOLDING;

            match self
                .state
                .compare_exchange_weak(s, new, Ordering::Acquire, Ordering::Relaxed)
            {
                Ok(_) => break, // Successfully acquired the exclusive lock!
                Err(actual) => {
                    s = actual;
                    backoff.spin();
                }
            }
        }
    }

    #[inline]
    fn unlock_write(&self) {
        // Release: publish critical-section writes before releasing the lock.
        let prev = self.state.fetch_and(!WRITER_MASK, Ordering::Release);

        debug_assert!(
            (prev & WRITER_MASK) == WRITER_MASK,
            "write_unlock when not holding writer lock"
        );
        debug_assert!(
            (prev & READERS_MASK) == 0,
            "writer unlock with readers present"
        );
    }
}

impl Default for SpinRwLock {
    fn default() -> Self {
        Self::new()
    }
}

#[must_use]
pub struct ReadGuard<'a> {
    lock: &'a SpinRwLock,
}
impl<'a> Drop for ReadGuard<'a> {
    fn drop(&mut self) {
        self.lock.unlock_read()
    }
}

#[must_use]
pub struct WriteGuard<'a> {
    lock: &'a SpinRwLock,
}
impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        self.lock.unlock_write()
    }
}

pub unsafe trait HasIntrusiveSpinRwLock {
    /// SAFETY CONTRACT:
    /// - Must return the lock that protects this object.
    /// - The returned reference must remain valid and refer to the same lock
    ///   for as long as `self` is alive.  
    fn lock<'a>(&'a self) -> &'a SpinRwLock;
}

#[repr(transparent)]
#[derive(Default)]
pub struct IntrusiveSpinRwLock<T>(UnsafeCell<T>);

unsafe impl<T: HasIntrusiveSpinRwLock + Send> Send for IntrusiveSpinRwLock<T> {}
unsafe impl<T: HasIntrusiveSpinRwLock + Send + Sync> Sync for IntrusiveSpinRwLock<T> {}

impl<T> IntrusiveSpinRwLock<T> {
    pub fn new(value: T) -> Self {
        Self(UnsafeCell::new(value))
    }
}

impl<T: HasIntrusiveSpinRwLock> IntrusiveSpinRwLock<T> {
    pub fn read(&self) -> IntrusiveReadGuard<'_, T>
    where
        T: Sync,
    {
        let inner = unsafe { &*self.0.get() };
        inner.lock()._read();
        IntrusiveReadGuard(inner)
    }

    pub fn try_read(&self) -> Option<IntrusiveReadGuard<'_, T>>
    where
        T: Sync,
    {
        let inner = unsafe { &*self.0.get() };
        if inner.lock()._try_read() {
            Some(IntrusiveReadGuard(inner))
        } else {
            None
        }
    }

    pub fn write(&self) -> IntrusiveWriteGuard<'_, T>
    where
        T: Send,
    {
        // SAFETY: form a shared ref to reach the lock; &mut T is only created
        // after exclusive access is established by _write().
        let inner = unsafe { &*self.0.get() };
        inner.lock()._write();
        IntrusiveWriteGuard(unsafe { &mut *self.0.get() })
    }

    pub fn try_write(&self) -> Option<IntrusiveWriteGuard<'_, T>>
    where
        T: Send,
    {
        let inner = unsafe { &*self.0.get() };
        if inner.lock()._try_write() {
            Some(IntrusiveWriteGuard(unsafe { &mut *self.0.get() }))
        } else {
            None
        }
    }
}

#[must_use]
pub struct IntrusiveReadGuard<'a, T: HasIntrusiveSpinRwLock>(&'a T);

impl<'a, T: HasIntrusiveSpinRwLock> Drop for IntrusiveReadGuard<'a, T> {
    fn drop(&mut self) {
        self.0.lock().unlock_read();
    }
}

impl<'a, T: HasIntrusiveSpinRwLock> Deref for IntrusiveReadGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.0
    }
}

#[must_use]
pub struct IntrusiveWriteGuard<'a, T: HasIntrusiveSpinRwLock>(&'a mut T);

impl<'a, T: HasIntrusiveSpinRwLock> Drop for IntrusiveWriteGuard<'a, T> {
    fn drop(&mut self) {
        self.0.lock().unlock_write();
    }
}

impl<'a, T: HasIntrusiveSpinRwLock> Deref for IntrusiveWriteGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, T: HasIntrusiveSpinRwLock> DerefMut for IntrusiveWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    // Helper: join a thread with timeout by having it send "done" on a channel.
    fn join_with_timeout<T: Send + 'static>(handle: thread::JoinHandle<T>, timeout: Duration) -> T {
        // We can't directly timeout JoinHandle::join, so use an intermediate channel.
        use std::sync::mpsc;

        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let res = handle.join();
            // propagate panic across threads
            tx.send(res).ok();
        });

        match rx.recv_timeout(timeout) {
            Ok(Ok(v)) => v,
            Ok(Err(panic)) => std::panic::resume_unwind(panic),
            Err(_) => panic!("thread did not finish within {:?}", timeout),
        }
    }

    #[test]
    fn many_readers_can_hold_simultaneously() {
        let lock = Arc::new(SpinRwLock::new());
        let n = 50;
        let start = Arc::new(Barrier::new(n + 1));

        let active_readers = Arc::new(AtomicUsize::new(0));
        let max_readers = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..n {
            let lock = lock.clone();
            let start = start.clone();
            let active = active_readers.clone();
            let maxr = max_readers.clone();
            handles.push(thread::spawn(move || {
                start.wait();
                let _g = lock.read();
                let cur = active.fetch_add(1, Ordering::AcqRel) + 1;

                // record max
                loop {
                    let m = maxr.load(Ordering::Relaxed);
                    if cur <= m {
                        break;
                    }
                    if maxr
                        .compare_exchange_weak(m, cur, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok()
                    {
                        break;
                    }
                }

                // hold for a bit to allow overlap
                thread::sleep(Duration::from_millis(20));
                active.fetch_sub(1, Ordering::AcqRel);
            }));
        }

        start.wait();
        for h in handles {
            join_with_timeout(h, Duration::from_secs(2));
        }

        let max_seen = max_readers.load(Ordering::Relaxed);
        assert!(
            max_seen >= 2,
            "expected overlapping readers; max_seen={}",
            max_seen
        );
    }

    #[test]
    fn writer_is_exclusive_against_readers_and_writers() {
        let lock = Arc::new(SpinRwLock::new());
        let stop = Arc::new(AtomicBool::new(false));

        // Track invariants while hammering
        let active_readers = Arc::new(AtomicUsize::new(0));
        let active_writers = Arc::new(AtomicUsize::new(0));
        let violations = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();

        // Readers
        for _ in 0..80 {
            let lock = lock.clone();
            let stop = stop.clone();
            let r = active_readers.clone();
            let w = active_writers.clone();
            let v = violations.clone();
            handles.push(thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let _g = lock.read();
                    let rr = r.fetch_add(1, Ordering::AcqRel) + 1;

                    // If any writer is active, it's a violation.
                    if w.load(Ordering::Acquire) != 0 {
                        v.fetch_add(1, Ordering::Relaxed);
                    }

                    // tiny critical section
                    std::hint::spin_loop();

                    r.fetch_sub(1, Ordering::AcqRel);
                    // help scheduling
                    if rr % 32 == 0 {
                        thread::yield_now();
                    }
                }
            }));
        }

        // Writers
        for _ in 0..30 {
            let lock = lock.clone();
            let stop = stop.clone();
            let r = active_readers.clone();
            let w = active_writers.clone();
            let v = violations.clone();
            handles.push(thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let _g = lock.write();
                    let ww = w.fetch_add(1, Ordering::AcqRel) + 1;

                    // Must be the only writer
                    if ww != 1 {
                        v.fetch_add(1, Ordering::Relaxed);
                    }
                    // Must be no readers
                    if r.load(Ordering::Acquire) != 0 {
                        v.fetch_add(1, Ordering::Relaxed);
                    }

                    std::hint::spin_loop();

                    w.fetch_sub(1, Ordering::AcqRel);
                    thread::yield_now();
                }
            }));
        }

        // Run for a short time
        thread::sleep(Duration::from_millis(300));
        stop.store(true, Ordering::Relaxed);

        for h in handles {
            join_with_timeout(h, Duration::from_secs(3));
        }

        let v = violations.load(Ordering::Relaxed);
        assert_eq!(v, 0, "saw {} exclusivity violations", v);
    }

    #[test]
    fn no_new_readers_after_writer_pending_is_set() {
        // This test checks the key semantic:
        // once WRITER_PENDING is set, read() should not succeed until writer releases.

        let lock = Arc::new(SpinRwLock::new());

        // Hold an initial reader so writer has to become pending (can't immediately hold).
        let reader_guard = lock.read();

        // A thread that will try to take write lock; it should set PENDING then block on readers.
        let lock_w = lock.clone();
        let writer_started = Arc::new(AtomicBool::new(false));
        let writer_started_w = writer_started.clone();

        let writer_h = thread::spawn(move || {
            writer_started_w.store(true, Ordering::Release);
            let _wg = lock_w.write();
            // once acquired, hold briefly
            thread::sleep(Duration::from_millis(50));
        });

        // Wait until writer thread is running and has a chance to set pending.
        // (We can't observe it directly without extra APIs, so we wait a tiny bit.)
        while !writer_started.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        thread::sleep(Duration::from_millis(20));

        // While the original reader is still holding, writer should (eventually) have set PENDING.
        // Now, new readers should NOT be able to enter.
        //
        // We test this by attempting to take read lock in another thread and asserting it doesn't
        // complete until after we drop the original reader and the writer completes.
        let lock_r = lock.clone();
        let (tx, rx) = std::sync::mpsc::channel();

        let reader2_h = thread::spawn(move || {
            let _rg = lock_r.read();
            tx.send(Instant::now()).ok();
            // hold briefly
            thread::sleep(Duration::from_millis(10));
        });

        // Ensure reader2 does NOT acquire quickly while writer is pending and reader1 is holding.
        assert!(
            rx.recv_timeout(Duration::from_millis(30)).is_err(),
            "a new reader acquired while writer should be pending"
        );

        // Now drop the initial reader, allowing writer to acquire.
        drop(reader_guard);

        // Writer should finish soon
        join_with_timeout(writer_h, Duration::from_secs(2));

        // Now reader2 should be able to finish
        join_with_timeout(reader2_h, Duration::from_secs(2));
    }

    #[test]
    fn stress_smoke_test_no_deadlock() {
        let lock = Arc::new(SpinRwLock::new());
        let stop = Arc::new(AtomicBool::new(false));

        let ops = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();

        for t in 0..120 {
            let lock = lock.clone();
            let stop = stop.clone();
            let ops = ops.clone();
            handles.push(thread::spawn(move || {
                let mut x = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    if (t % 4) == 0 {
                        let _g = lock.write();
                        // do a bit more work in writers
                        x = x.wrapping_mul(1664525).wrapping_add(1013904223);
                        std::hint::spin_loop();
                    } else {
                        let _g = lock.read();
                        x = x.wrapping_add(1);
                    }
                    ops.fetch_add(1, Ordering::Relaxed);

                    if (x & 0xFF) == 0 {
                        thread::yield_now();
                    }
                }
                x
            }));
        }

        thread::sleep(Duration::from_millis(400));
        stop.store(true, Ordering::Relaxed);

        for h in handles {
            join_with_timeout(h, Duration::from_secs(3));
        }

        let total_ops = ops.load(Ordering::Relaxed);
        assert!(total_ops > 1_000, "too few ops: {}", total_ops);
    }

    // --- Basic single-threaded semantics ---

    #[test]
    fn uncontended_read_write() {
        let lock = SpinRwLock::new();

        // Write then read — basic sanity.
        {
            let _wg = lock.write();
        }
        {
            let _rg = lock.read();
        }

        // Multiple concurrent readers on same thread (reentrant reads).
        {
            let _r1 = lock.read();
            let _r2 = lock.read();
            let _r3 = lock.read();
        }
    }

    // --- Data integrity through write guards ---

    #[test]
    fn concurrent_counter_integrity() {
        // Multiple threads incrementing a non-atomic counter under write guards;
        // final value must equal the total number of increments.
        use std::cell::UnsafeCell;

        struct Shared {
            lock: SpinRwLock,
            counter: UnsafeCell<u64>,
        }
        unsafe impl Sync for Shared {}

        let shared = Arc::new(Shared {
            lock: SpinRwLock::new(),
            counter: UnsafeCell::new(0),
        });

        let n_threads = 20;
        let n_iters = 50_000u64;

        let mut handles = Vec::new();
        for _ in 0..n_threads {
            let shared = shared.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..n_iters {
                    let _g = shared.lock.write();
                    unsafe { *shared.counter.get() += 1 };
                }
            }));
        }

        for h in handles {
            join_with_timeout(h, Duration::from_secs(10));
        }

        let _rg = shared.lock.read();
        let counter = unsafe { *shared.counter.get() };
        assert_eq!(counter, n_threads as u64 * n_iters);
    }

    // --- IntrusiveSpinRwLock tests ---

    #[derive(Default)]
    struct TestData {
        _lock: SpinRwLock,
        value: u64,
    }

    unsafe impl HasIntrusiveSpinRwLock for TestData {
        fn lock(&self) -> &SpinRwLock {
            &self._lock
        }
    }

    type ProtectedTestData = IntrusiveSpinRwLock<TestData>;

    #[test]
    fn intrusive_basic_read_write() {
        let data = ProtectedTestData::default();

        data.write().value = 42;
        assert_eq!(data.read().value, 42);

        data.write().value += 8;
        assert_eq!(data.read().value, 50);
    }

    #[test]
    fn intrusive_concurrent_counter() {
        let data = Arc::new(ProtectedTestData::default());

        let n_threads = 20;
        let n_iters = 50_000u64;

        let mut handles = Vec::new();
        for _ in 0..n_threads {
            let data = data.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..n_iters {
                    data.write().value += 1;
                }
            }));
        }

        for h in handles {
            join_with_timeout(h, Duration::from_secs(10));
        }

        assert_eq!(data.read().value, n_threads as u64 * n_iters);
    }

    #[test]
    fn intrusive_concurrent_readers_see_consistent_data() {
        let data = Arc::new(ProtectedTestData::new(TestData {
            _lock: SpinRwLock::new(),
            value: 999,
        }));

        let barrier = Arc::new(Barrier::new(50));
        let mut handles = Vec::new();

        for _ in 0..50 {
            let data = data.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                let guard = data.read();
                assert_eq!(guard.value, 999);
            }));
        }

        for h in handles {
            join_with_timeout(h, Duration::from_secs(2));
        }
    }

    #[test]
    fn drop_read_then_write_no_deadlock() {
        // Verify that dropping a read guard and immediately taking a write guard works.
        let lock = SpinRwLock::new();

        let _rg = lock.read();
        drop(_rg);
        let _wg = lock.write();
        drop(_wg);
        let _rg2 = lock.read();
    }

    // --- try_read / try_write ---

    #[test]
    fn try_read_succeeds_when_uncontended() {
        let lock = SpinRwLock::new();
        assert!(lock.try_read().is_some());
    }

    #[test]
    fn try_write_succeeds_when_uncontended() {
        let lock = SpinRwLock::new();
        assert!(lock.try_write().is_some());
    }

    #[test]
    fn try_read_succeeds_with_existing_readers() {
        let lock = SpinRwLock::new();
        let _r1 = lock.read();
        assert!(lock.try_read().is_some());
    }

    #[test]
    fn try_read_fails_when_writer_holds() {
        let lock = SpinRwLock::new();
        let _wg = lock.write();
        assert!(lock.try_read().is_none());
    }

    #[test]
    fn try_write_fails_when_reader_holds() {
        let lock = SpinRwLock::new();
        let _rg = lock.read();
        assert!(lock.try_write().is_none());
    }

    #[test]
    fn try_write_fails_when_writer_holds() {
        let lock = SpinRwLock::new();
        let _wg = lock.write();
        assert!(lock.try_write().is_none());
    }

    #[test]
    fn try_write_succeeds_after_guard_dropped() {
        let lock = SpinRwLock::new();
        let rg = lock.read();
        assert!(lock.try_write().is_none());
        drop(rg);
        assert!(lock.try_write().is_some());
    }

    #[test]
    fn intrusive_try_read_write() {
        let data = ProtectedTestData::default();

        // try_write on uncontended
        {
            let mut wg = data.try_write().expect("should succeed");
            wg.value = 77;
        }

        // try_read sees the write
        {
            let rg = data.try_read().expect("should succeed");
            assert_eq!(rg.value, 77);
        }

        // try_write fails while reader holds
        let rg = data.read();
        assert!(data.try_write().is_none());
        drop(rg);

        // try_read fails while writer holds
        let wg = data.write();
        assert!(data.try_read().is_none());
        drop(wg);
    }
}
