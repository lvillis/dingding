//! Pluggable event deduplication with cancellation-safe in-memory reservations.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use serde_json::Value;

use crate::{Error, Result};

/// Future returned by a deduplication backend. Implementations must not block the executor.
pub type DeduplicationFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Atomic reservation result for one event key.
pub enum Deduplication {
    /// This caller owns the event until the lease is completed or dropped.
    Acquired(Box<dyn DeduplicationLease>),
    /// Another caller is still processing this event. Do not acknowledge success yet.
    InFlight,
    /// A previous caller completed successfully; replay its saved response.
    Completed(Value),
}

/// Reservation for one event. Dropping an unfinished lease must allow a later retry.
///
/// Distributed implementations must fence stale owners and recover abandoned reservations
/// using renewable leases. Cleanup on drop must not block. Completion must atomically retain
/// the response before releasing ownership, including when its future is cancelled.
pub trait DeduplicationLease: Send {
    /// Stores the successful response for subsequent duplicates.
    fn complete(self: Box<Self>, response: Value) -> DeduplicationFuture<'static, ()>;
}

/// Shared asynchronous event deduplication backend.
///
/// Keys include the protocol and event identity. Use a separate namespace for each application
/// when sharing a backend. Storage errors must fail closed rather than execute an unreserved event.
pub trait EventDeduplicator: Send + Sync {
    /// Atomically reserves a new event or returns its current state.
    fn claim<'a>(&'a self, key: &'a str) -> DeduplicationFuture<'a, Deduplication>;
}

/// Bounded process-local cache shared by clones. Defaults to 10,000 entries and five minutes.
///
/// Active reservations never expire while their lease exists. Completed entries expire after
/// `retention`; a full cache rejects new claims until space becomes available. This cache does
/// not provide exactly-once business effects across process crashes or retention expiry.
#[derive(Clone)]
pub struct MemoryEventDeduplicator {
    inner: Arc<Mutex<HashMap<String, Entry>>>,
    capacity: usize,
    retention: Duration,
}

enum Entry {
    InFlight,
    Completed {
        response: Value,
        expires_at: Instant,
    },
}

impl Default for MemoryEventDeduplicator {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            capacity: 10_000,
            retention: Duration::from_secs(300),
        }
    }
}

impl MemoryEventDeduplicator {
    /// Creates a bounded cache with an explicit successful-response retention period.
    pub fn new(capacity: usize, retention: Duration) -> Result<Self> {
        if capacity == 0 || retention.is_zero() || Instant::now().checked_add(retention).is_none() {
            return Err(Error::InvalidConfig(
                "deduplication capacity and retention must be positive and finite".into(),
            ));
        }
        Ok(Self {
            capacity,
            retention,
            ..Self::default()
        })
    }
}

impl EventDeduplicator for MemoryEventDeduplicator {
    fn claim<'a>(&'a self, key: &'a str) -> DeduplicationFuture<'a, Deduplication> {
        Box::pin(async move {
            let mut entries = self.inner.lock().map_err(|_| cache_error())?;
            let now = Instant::now();
            if matches!(entries.get(key), Some(Entry::Completed { expires_at, .. }) if *expires_at <= now)
            {
                entries.remove(key);
            }
            match entries.get(key) {
                Some(Entry::InFlight) => return Ok(Deduplication::InFlight),
                Some(Entry::Completed { response, .. }) => {
                    return Ok(Deduplication::Completed(response.clone()));
                }
                None => {}
            }
            if entries.len() >= self.capacity {
                entries.retain(|_, entry| !matches!(entry, Entry::Completed { expires_at, .. } if *expires_at <= now));
                if entries.len() >= self.capacity {
                    return Err(Error::InvalidConfig(
                        "event deduplication cache is full".into(),
                    ));
                }
            }
            entries.insert(key.to_owned(), Entry::InFlight);
            Ok(Deduplication::Acquired(Box::new(MemoryLease {
                cache: self.clone(),
                key: key.to_owned(),
                completed: false,
            })))
        })
    }
}

struct MemoryLease {
    cache: MemoryEventDeduplicator,
    key: String,
    completed: bool,
}

impl DeduplicationLease for MemoryLease {
    fn complete(mut self: Box<Self>, response: Value) -> DeduplicationFuture<'static, ()> {
        Box::pin(async move {
            let expires_at = Instant::now()
                .checked_add(self.cache.retention)
                .ok_or_else(cache_error)?;
            {
                let mut entries = self.cache.inner.lock().map_err(|_| cache_error())?;
                entries.insert(
                    self.key.clone(),
                    Entry::Completed {
                        response,
                        expires_at,
                    },
                );
            }
            self.completed = true;
            Ok(())
        })
    }
}

impl Drop for MemoryLease {
    fn drop(&mut self) {
        if !self.completed
            && let Ok(mut entries) = self.cache.inner.lock()
        {
            entries.remove(&self.key);
        }
    }
}

fn cache_error() -> Error {
    Error::InvalidConfig("event deduplication cache is unavailable".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn acquire(store: &MemoryEventDeduplicator, key: &str) -> Box<dyn DeduplicationLease> {
        match store.claim(key).await.expect("claim") {
            Deduplication::Acquired(lease) => lease,
            _ => panic!("expected an acquired lease"),
        }
    }

    #[tokio::test]
    async fn claims_are_exclusive_and_success_replays_response() {
        let store = MemoryEventDeduplicator::default();
        let lease = acquire(&store, "key").await;
        assert!(matches!(
            store.clone().claim("key").await.expect("claim"),
            Deduplication::InFlight
        ));
        lease
            .complete(serde_json::json!({"response":"saved"}))
            .await
            .expect("complete");
        assert!(
            matches!(store.claim("key").await.expect("claim"), Deduplication::Completed(value) if value == serde_json::json!({"response":"saved"}))
        );
    }

    #[tokio::test]
    async fn dropping_work_or_unpolled_completion_releases_the_reservation() {
        let store = MemoryEventDeduplicator::default();
        drop(acquire(&store, "key").await);
        drop(acquire(&store, "key").await.complete(Value::Null));
        drop(acquire(&store, "key").await);
    }

    #[tokio::test]
    async fn full_cache_does_not_evict_active_or_unexpired_entries() {
        let store = MemoryEventDeduplicator::new(1, Duration::from_secs(300)).expect("cache");
        let lease = acquire(&store, "key").await;
        assert!(store.claim("other").await.is_err());
        lease.complete(Value::Null).await.expect("complete");
        assert!(store.claim("other").await.is_err());
        {
            let mut entries = store.inner.lock().expect("cache");
            if let Some(Entry::Completed { expires_at, .. }) = entries.get_mut("key") {
                *expires_at = Instant::now();
            }
        }
        drop(acquire(&store, "other").await);
    }

    #[test]
    fn rejects_zero_or_overflowing_configuration() {
        assert!(MemoryEventDeduplicator::new(0, Duration::from_secs(1)).is_err());
        assert!(MemoryEventDeduplicator::new(1, Duration::ZERO).is_err());
        assert!(MemoryEventDeduplicator::new(1, Duration::MAX).is_err());
    }
}
