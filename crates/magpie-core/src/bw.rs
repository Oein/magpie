//! Bandwidth manager: converts per-download priority + the optional global cap
//! into a concrete refill rate for each download's [`RateLimiter`].
//!
//! Model (weighted fair share):
//! * No global cap  → each download is limited only by its own `max_speed_bps`.
//! * Global cap `C` → each *active* download gets `C * weight / Σweight`, then
//!   further clamped by its own `max_speed_bps`.

use std::collections::HashMap;
use std::sync::Arc;

use magpie_proto::{DownloadId, Priority};

use crate::rate::RateLimiter;

struct Entry {
    priority: Priority,
    own_cap: Option<u64>,
    limiter: Arc<RateLimiter>,
    active: bool,
}

#[derive(Default)]
pub struct BandwidthManager {
    global_cap: Option<u64>,
    entries: HashMap<DownloadId, Entry>,
}

impl BandwidthManager {
    pub fn register(
        &mut self,
        id: DownloadId,
        priority: Priority,
        own_cap: Option<u64>,
        limiter: Arc<RateLimiter>,
    ) {
        self.entries.insert(
            id,
            Entry {
                priority,
                own_cap,
                limiter,
                active: false,
            },
        );
    }

    pub fn unregister(&mut self, id: DownloadId) {
        self.entries.remove(&id);
    }

    pub fn set_active(&mut self, id: DownloadId, active: bool) {
        if let Some(e) = self.entries.get_mut(&id) {
            e.active = active;
        }
    }

    pub fn set_options(&mut self, id: DownloadId, priority: Priority, own_cap: Option<u64>) {
        if let Some(e) = self.entries.get_mut(&id) {
            e.priority = priority;
            e.own_cap = own_cap;
        }
    }

    pub fn set_global_cap(&mut self, cap: Option<u64>) {
        self.global_cap = cap;
    }

    /// Recompute and push new rates to every limiter. Call after any change.
    pub async fn recompute(&self) {
        match self.global_cap {
            None => {
                for e in self.entries.values() {
                    e.limiter.set_rate(e.own_cap.unwrap_or(0)).await;
                }
            }
            Some(cap) => {
                let total_weight: u32 = self
                    .entries
                    .values()
                    .filter(|e| e.active)
                    .map(|e| e.priority.weight())
                    .sum();
                for e in self.entries.values() {
                    let rate = if !e.active || total_weight == 0 {
                        e.own_cap.unwrap_or(0)
                    } else {
                        let share =
                            (cap as u128 * e.priority.weight() as u128 / total_weight as u128) as u64;
                        match e.own_cap {
                            Some(c) => share.min(c),
                            None => share,
                        }
                    };
                    e.limiter.set_rate(rate).await;
                }
            }
        }
    }
}
