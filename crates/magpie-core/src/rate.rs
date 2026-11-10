//! Token-bucket rate limiting.
//!
//! A download holds its *own* bucket (per-download cap and its weighted share of
//! the global cap) and also acquires from a shared global bucket. Segments call
//! [`RateLimiter::acquire`] for every chunk *before* writing it, so throttling
//! never requires buffering data in memory.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

struct Bucket {
    /// Refill rate in bytes/sec. `0.0` means unlimited.
    rate: f64,
    capacity: f64,
    tokens: f64,
    last: Instant,
}

impl Bucket {
    fn refill(&mut self) {
        if self.rate <= 0.0 {
            return;
        }
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
    }
}

/// A single token bucket. Cheap to clone (shared via `Arc` internally).
#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<Mutex<Bucket>>,
}

impl RateLimiter {
    /// `rate_bps == 0` → unlimited.
    pub fn new(rate_bps: u64) -> Self {
        let rate = rate_bps as f64;
        let capacity = rate.max(1024.0 * 1024.0); // allow at least a 1 MiB burst
        Self {
            inner: Arc::new(Mutex::new(Bucket {
                rate,
                capacity,
                tokens: capacity,
                last: Instant::now(),
            })),
        }
    }

    pub fn unlimited() -> Self {
        Self::new(0)
    }

    /// Change the refill rate on the fly (used by the bandwidth manager when the
    /// set of active downloads or the global cap changes).
    pub async fn set_rate(&self, rate_bps: u64) {
        let mut b = self.inner.lock().await;
        b.refill();
        b.rate = rate_bps as f64;
        b.capacity = b.rate.max(1024.0 * 1024.0);
        b.tokens = b.tokens.min(b.capacity);
    }

    /// Block until `n` bytes of allowance are available. Returns immediately when
    /// the bucket is unlimited.
    pub async fn acquire(&self, n: usize) {
        let n = n as f64;
        loop {
            let wait = {
                let mut b = self.inner.lock().await;
                if b.rate <= 0.0 {
                    return;
                }
                b.refill();
                let need = n.min(b.capacity);
                if b.tokens >= need {
                    b.tokens -= need;
                    return;
                }
                let deficit = need - b.tokens;
                Duration::from_secs_f64((deficit / b.rate).max(0.001))
            };
            tokio::time::sleep(wait).await;
        }
    }
}
