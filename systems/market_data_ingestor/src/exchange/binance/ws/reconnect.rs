use std::time::Duration;

pub fn next_backoff(attempt: u32) -> Duration {
    let capped = attempt.min(6);
    Duration::from_secs(2u64.pow(capped))
}
