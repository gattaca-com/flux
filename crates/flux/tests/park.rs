#[cfg(feature = "park")]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::Duration,
    };

    use flux::park::Signal;

    #[test]
    fn test_park_unpark() {
        let signal = Arc::new(Signal::new());
        let signal_clone = signal.clone();
        let parked = Arc::new(AtomicBool::new(false));
        let parked_clone = parked.clone();

        let counter = signal.read_counter();

        let handle = thread::spawn(move || {
            parked_clone.store(true, Ordering::Release);
            signal_clone.park(counter);
            parked_clone.store(false, Ordering::Release);
        });

        // Wait for thread to run and park
        thread::sleep(Duration::from_millis(50));
        assert!(parked.load(Ordering::Acquire));

        // Signal to unpark
        signal.signal();

        handle.join().unwrap();
        assert!(!parked.load(Ordering::Acquire));
    }

    #[test]
    fn test_sticky_behavior() {
        let signal = Signal::new();
        let counter = signal.read_counter();

        // Signal first (sticky)
        signal.signal();

        // Park with old counter should not block
        let start = std::time::Instant::now();
        signal.park(counter);
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[test]
    fn test_mio_waker() {
        let signal = Signal::new();
        let mut poll = mio::Poll::new().unwrap();
        let waker = mio::Waker::new(poll.registry(), mio::Token(42)).unwrap();

        signal.register_waker(waker);

        signal.signal();

        let mut events = mio::Events::with_capacity(10);
        poll.poll(&mut events, Some(Duration::from_millis(500))).unwrap();

        let mut found = false;
        for event in events.iter() {
            if event.token() == mio::Token(42) {
                found = true;
            }
        }

        assert!(found, "mio waker was not woken");
    }
}
