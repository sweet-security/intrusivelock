use intrusivelock::spin_rwlock::SpinRwLock;
use std::{sync::Arc, thread, time::Duration};

//
// 10 threads incrementing a static counter protected by a spinlock
// note: this is unsafe by design, for demo purposes only
//
fn main() {
    static mut COUNTER1: u64 = 0;
    static mut COUNTER2: u64 = 0;

    let lock = Arc::new(SpinRwLock::new());

    let mut threads = Vec::new();
    for _ in 0..10 {
        let lock = lock.clone();
        threads.push(thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(1));
            for _ in 0..100_000 {
                unsafe {
                    COUNTER1 += 1;
                }
                let _guard = lock.write();
                unsafe {
                    COUNTER2 += 1;
                }
            }
        }));
    }

    for t in threads {
        t.join().expect("worker thread panicked");
    }

    let counter1 = unsafe { COUNTER1 };
    let counter2 = unsafe { COUNTER2 };
    println!("{counter1} vs {counter2}");
    assert_eq!(counter2, 1_000_000);
}
