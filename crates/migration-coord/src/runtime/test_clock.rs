use super::Clock;
use chrono::{DateTime, Utc};
use std::sync::{Arc, Mutex};

/// Wall-clock that returns a fixed instant; tests bump it
/// explicitly between `ingest` calls to control event `at`
/// timestamps.
#[derive(Debug)]
pub struct FixedClock {
    now: Mutex<DateTime<Utc>>,
}

impl FixedClock {
    pub fn new(at: DateTime<Utc>) -> Arc<Self> {
        Arc::new(Self {
            now: Mutex::new(at),
        })
    }

    pub fn set(&self, at: DateTime<Utc>) {
        *self.now.lock().unwrap() = at;
    }

    pub fn advance(&self, by: chrono::Duration) {
        let mut n = self.now.lock().unwrap();
        *n += by;
    }
}

impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }
}
