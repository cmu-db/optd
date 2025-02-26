use futures::lock::Mutex;
use std::sync::Arc;

#[derive(Clone)]
pub(super) struct StreamCache<T> {
    cache: Arc<Mutex<Option<T>>>,
}

impl<T: Clone> StreamCache<T> {
    pub(super) fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(None)),
        }
    }

    pub(super) async fn get_or_compute<F, Fut>(&self, compute_fn: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let mut cache_lock = self.cache.lock().await;
        if let Some(ref cached_results) = *cache_lock {
            cached_results.clone()
        } else {
            let computed_results = compute_fn().await;
            *cache_lock = Some(computed_results.clone());
            computed_results
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn test_memoization() {
        let compute_count = Arc::new(AtomicUsize::new(0));
        let cache = StreamCache::new();

        // First call should compute
        let result1 = cache
            .get_or_compute(|| {
                let counter = Arc::clone(&compute_count);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    "result".to_string()
                }
            })
            .await;

        // Second call should use cache
        let result2 = cache
            .get_or_compute(|| {
                let counter = Arc::clone(&compute_count);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    "different result".to_string()
                }
            })
            .await;

        assert_eq!(result1, "result");
        assert_eq!(result2, "result"); // Should return first result
        assert_eq!(compute_count.load(Ordering::SeqCst), 1); // Should only compute once
    }
}
