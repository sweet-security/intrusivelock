use intrusivelock::spin_rwlock::{HasIntrusiveSpinRwLock, IntrusiveSpinRwLock, SpinRwLock};
use std::{sync::Arc, thread};

#[derive(Default)]
struct DataWithLock {
    _lock: SpinRwLock,
    counter: u64,
}

unsafe impl HasIntrusiveSpinRwLock for DataWithLock {
    fn lock<'a>(&'a self) -> &'a SpinRwLock {
        &self._lock
    }
}

type ProtectedData = IntrusiveSpinRwLock<DataWithLock>;

fn main() {
    let data = Arc::new(ProtectedData::default());

    let mut threads = Vec::new();
    for _ in 0..10 {
        let data = data.clone();
        threads.push(thread::spawn(move || {
            for _ in 0..100_000 {
                data.write().counter += 1;
            }
        }));
    }

    for t in threads {
        t.join().expect("worker thread panicked");
    }

    let counter = data.read().counter;
    assert_eq!(counter, 1_000_000);
}
