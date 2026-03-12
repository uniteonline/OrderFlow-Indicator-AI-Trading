use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct SimpleRateLimiter {
    interval: Duration,
    gate: Arc<Mutex<Instant>>,
}

impl SimpleRateLimiter {
    pub fn new(req_per_sec: u64) -> Self {
        let micros = if req_per_sec == 0 {
            1_000_000
        } else {
            1_000_000 / req_per_sec
        };
        Self {
            interval: Duration::from_micros(micros),
            gate: Arc::new(Mutex::new(Instant::now())),
        }
    }

    pub async fn acquire(&self) {
        let mut gate = self.gate.lock().await;
        let now = Instant::now();
        if *gate > now {
            tokio::time::sleep(*gate - now).await;
        }
        *gate = Instant::now() + self.interval;
    }
}
