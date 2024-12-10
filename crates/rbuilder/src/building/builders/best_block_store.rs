use futures::stream::{Stream, StreamExt};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::BroadcastStream;
use tracing::info;

use super::Block;

const DEFAULT_BUFFER_SIZE: usize = 64;

/// Error returned when an update is rejected.
#[derive(Debug)]
pub struct UpdateRejected;

/// GlobalBestBlockStore maintains a single global best block.
/// It is updated by the best block builder and broadcast to all listeners.
#[derive(Clone, Debug)]
pub struct GlobalBestBlockStore {
    best_block: Arc<Mutex<Option<Block>>>,
    tx: broadcast::Sender<Block>,
}

impl Default for GlobalBestBlockStore {
    fn default() -> Self {
        Self::new()
    }
}

impl GlobalBestBlockStore {
    /// Create a new global best block store with default buffer size.
    pub fn new() -> Self {
        Self::with_buffer_size(DEFAULT_BUFFER_SIZE)
    }

    /// Create a new global best block store with specified buffer size.
    pub fn with_buffer_size(buffer_size: usize) -> Self {
        let (tx, _rx) = broadcast::channel(buffer_size);
        Self {
            best_block: Arc::new(Mutex::new(None)),
            tx,
        }
    }

    /// Compare the new block with the current best block and update if the new block is better.
    pub async fn compare_and_update(&self, new_block: Block) -> Result<(), UpdateRejected> {
        info!(
            "Comparing new block {:?} with current best block {:?}",
            new_block.trace.bid_value,
            self.get_best_block().await.unwrap().trace.bid_value
        );
        let mut guard = self.best_block.lock().await;
        if new_block.has_higher_bid_value_than(guard.as_ref()) {
            info!("Updating best block to {:?}", new_block.trace.bid_value);
            *guard = Some(new_block.clone());
            let _ = self.tx.send(new_block);
            Ok(())
        } else {
            Err(UpdateRejected)
        }
    }

    /// Subscribe to updates on the best block.
    pub fn subscribe(&self) -> impl Stream<Item = Block> {
        let rx = self.tx.subscribe();
        BroadcastStream::new(rx).filter_map(|res| async move { res.ok() })
    }

    /// Get the current best block.
    pub async fn get_best_block(&self) -> Option<Block> {
        let guard = self.best_block.lock().await;
        guard.clone()
    }

    /// Get a broadcast receiver for best block updates
    pub fn subscribe_to_updates(&self) -> broadcast::Receiver<Block> {
        self.tx.subscribe()
    }
}

/// A tracker that keeps a local view of the best block.
/// It provides a method to try to update the best block and a method to get the best block.
#[derive(Debug, Clone)]
pub struct BestBlockTracker {
    store: GlobalBestBlockStore,
    best_block: Arc<Mutex<Option<Block>>>,
}

impl BestBlockTracker {
    /// Craete a new tracker and start background tracking of the provided store.
    /// This spawns a background task that will track the best block and update the local best block.
    pub async fn new(store: GlobalBestBlockStore) -> Self {
        let best_block = Arc::new(Mutex::new(store.get_best_block().await));
        let best_block_clone = best_block.clone();
        let mut rx = store.subscribe_to_updates();

        // start background task to track best blocks
        tokio::spawn(async move {
            while let Ok(block) = rx.recv().await {
                *best_block_clone.lock().await = Some(block);
            }
        });

        Self { store, best_block }
    }

    /// Attempt to update the best block with `new_block`.
    /// If `new_block` is better than the current best block, it tries to update the store.
    /// Returns `true` if the global store was successfully updated.
    pub async fn try_and_update(&self, new_block: Block) -> bool {
        info!(
            "Trying to update best block {:?} with new block {:?}",
            self.get_best_block().await.unwrap().trace.bid_value,
            new_block.trace.bid_value
        );
        let current_best = self.best_block.lock().await;
        if new_block.has_higher_bid_value_than(current_best.as_ref()) {
            self.store.compare_and_update(new_block).await.is_ok()
        } else {
            false
        }
    }

    /// Get current best block
    pub async fn get_best_block(&self) -> Option<Block> {
        self.best_block.lock().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::building::{BuiltBlockTrace, SealedBlock};
    use alloy_primitives::U256;
    use futures::pin_mut;
    use std::time::Duration;
    use time::OffsetDateTime;

    #[allow(dead_code)]
    fn make_block(bid: u64) -> Block {
        Block {
            trace: BuiltBlockTrace {
                included_orders: vec![],
                bid_value: U256::from(bid),
                true_bid_value: U256::from(bid),
                got_no_signer_error: false,
                orders_closed_at: OffsetDateTime::now_utc(),
                orders_sealed_at: OffsetDateTime::now_utc(),
                fill_time: Duration::from_secs(0),
                finalize_time: Duration::from_secs(0),
                root_hash_time: Duration::from_secs(0),
            },
            sealed_block: SealedBlock::default(),
            txs_blobs_sidecars: vec![],
            execution_requests: vec![],
            builder_name: "test_builder".to_string(),
        }
    }

    #[tokio::test]
    async fn test_compare_and_update() {
        let store = GlobalBestBlockStore::new();
        let initial_block = make_block(100);
        let better_block = make_block(101);
        let worse_block = make_block(99);

        // Initially store is empty so any block is better
        assert!(store
            .compare_and_update(initial_block.clone())
            .await
            .is_ok());
        assert_eq!(
            store.get_best_block().await.unwrap().trace.bid_value,
            U256::from(100)
        );

        // Update with a better block
        assert!(store.compare_and_update(better_block.clone()).await.is_ok());
        assert_eq!(
            store.get_best_block().await.unwrap().trace.bid_value,
            U256::from(101)
        );

        // Update with a worse block
        assert!(store.compare_and_update(worse_block.clone()).await.is_err());
        assert_eq!(
            store.get_best_block().await.unwrap().trace.bid_value,
            U256::from(101)
        );
    }

    #[tokio::test]
    async fn test_subscribe() {
        let store = GlobalBestBlockStore::new();
        let stream = store.subscribe();
        pin_mut!(stream);

        let block = make_block(100);

        // No updates yet, stream should yield None.
        tokio::select! {
            biased;
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {},
            val = stream.next() => {
                panic!("Received a block when none was broadcast: {:?}", val);
            }
        }

        // Update the store; subscribers should see this.
        assert!(store.compare_and_update(block.clone()).await.is_ok());

        let received = stream.next().await.unwrap();
        assert_eq!(received.trace.bid_value, U256::from(100));
    }

    #[tokio::test]
    async fn test_concurrent_updates() {
        let store = GlobalBestBlockStore::new();
        let mut handles = vec![];
        use rand::Rng;
        let mut rng = rand::thread_rng();

        // Generate random bid values and store the max for later assertion.
        let mut values = vec![];
        for _ in 0..10 {
            values.push(rng.gen_range(50..200u64));
        }
        let max_value = *values.iter().max().unwrap();

        // Spawn tasks to update the store concurrently.
        for v in values.into_iter() {
            let s = store.clone();
            handles.push(tokio::spawn(async move {
                let block = make_block(v);
                let _ = s.compare_and_update(block).await;
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // After all updates, ensure the best block matches the highest submitted value.
        let best_block = store.get_best_block().await.unwrap();
        assert_eq!(best_block.trace.bid_value, U256::from(max_value));
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let store = GlobalBestBlockStore::new();

        // Create three subscribers before any updates
        let stream1 = store.subscribe();
        let stream2 = store.subscribe();
        let stream3 = store.subscribe();

        // Pin them for iteration
        tokio::pin!(stream1, stream2, stream3);

        let block = make_block(150);

        // Update the store
        assert!(store.compare_and_update(block.clone()).await.is_ok());

        // All subscribers should receive the update
        let received1 = stream1.next().await.unwrap();
        let received2 = stream2.next().await.unwrap();
        let received3 = stream3.next().await.unwrap();

        assert_eq!(received1.trace.bid_value, U256::from(150));
        assert_eq!(received2.trace.bid_value, U256::from(150));
        assert_eq!(received3.trace.bid_value, U256::from(150));

        // Now create a new subscriber after the first update
        let late_subscriber = store.subscribe();
        pin_mut!(late_subscriber);

        let better_block = make_block(200);

        // Update again
        assert!(store.compare_and_update(better_block.clone()).await.is_ok());

        let late_received = late_subscriber.next().await.unwrap();
        assert_eq!(late_received.trace.bid_value, U256::from(200));
    }

    #[tokio::test]
    async fn test_tracker_initially_empty_store() {
        let store = GlobalBestBlockStore::new();
        let tracker = BestBlockTracker::new(store.clone()).await;

        // Store is empty initially, tracker should also reflect no best block
        assert!(tracker.get_best_block().await.is_none());
    }

    #[tokio::test]
    async fn test_tracker_initially_with_store_data() {
        let store = GlobalBestBlockStore::new();
        let initial_block = make_block(100);
        store
            .compare_and_update(initial_block.clone())
            .await
            .unwrap();

        let tracker = BestBlockTracker::new(store.clone()).await;

        // Tracker should immediately see the block that was in the store
        let best_block = tracker.get_best_block().await;
        assert!(best_block.is_some());
        assert_eq!(best_block.unwrap().trace.bid_value, U256::from(100));
    }

    #[tokio::test]
    async fn test_tracker_updates_on_new_broadcast() {
        let store = GlobalBestBlockStore::new();
        let tracker = BestBlockTracker::new(store.clone()).await;

        // Initially empty
        assert!(tracker.get_best_block().await.is_none());

        // Update the store
        let better_block = make_block(200);
        store
            .compare_and_update(better_block.clone())
            .await
            .unwrap();

        // Allow some time for the background task to pick up changes
        tokio::time::sleep(Duration::from_millis(100)).await;

        let best_block = tracker.get_best_block().await;
        assert!(best_block.is_some());
        assert_eq!(best_block.unwrap().trace.bid_value, U256::from(200));
    }

    #[tokio::test]
    async fn test_try_and_update_improves_block() {
        let store = GlobalBestBlockStore::new();
        let tracker = BestBlockTracker::new(store.clone()).await;

        let block_100 = make_block(100);
        assert!(tracker.try_and_update(block_100.clone()).await);

        let current_best = store.get_best_block().await.unwrap();
        assert_eq!(current_best.trace.bid_value, U256::from(100));

        // Try a worse block, should fail to update
        let block_50 = make_block(50);
        assert!(!tracker.try_and_update(block_50.clone()).await);

        let current_best = store.get_best_block().await.unwrap();
        assert_eq!(current_best.trace.bid_value, U256::from(100));

        // Try a better block, should succeed
        let block_150 = make_block(150);
        assert!(tracker.try_and_update(block_150.clone()).await);

        let current_best = store.get_best_block().await.unwrap();
        assert_eq!(current_best.trace.bid_value, U256::from(150));
    }

    #[tokio::test]
    async fn test_multiple_trackers_see_same_updates() {
        let store = GlobalBestBlockStore::new();
        let tracker1 = BestBlockTracker::new(store.clone()).await;
        let tracker2 = BestBlockTracker::new(store.clone()).await;

        // No best block initially
        assert!(tracker1.get_best_block().await.is_none());
        assert!(tracker2.get_best_block().await.is_none());

        // Update from tracker1
        let block_100 = make_block(100);
        assert!(tracker1.try_and_update(block_100.clone()).await);

        // Both should now see the updated block
        tokio::time::sleep(Duration::from_millis(100)).await;
        let t1_best = tracker1.get_best_block().await;
        let t2_best = tracker2.get_best_block().await;
        assert_eq!(t1_best.unwrap().trace.bid_value, U256::from(100));
        assert_eq!(t2_best.unwrap().trace.bid_value, U256::from(100));

        // Update directly via store to simulate external update
        let block_200 = make_block(200);
        store.compare_and_update(block_200.clone()).await.unwrap();

        // Both trackers should see the improved block after some time
        tokio::time::sleep(Duration::from_millis(100)).await;
        let t1_best = tracker1.get_best_block().await.unwrap();
        let t2_best = tracker2.get_best_block().await.unwrap();
        assert_eq!(t1_best.trace.bid_value, U256::from(200));
        assert_eq!(t2_best.trace.bid_value, U256::from(200));
    }

    #[tokio::test]
    async fn test_concurrent_try_and_update() {
        let store = GlobalBestBlockStore::new();
        let tracker = BestBlockTracker::new(store.clone()).await;

        // We'll try multiple updates concurrently
        let candidates = vec![50, 150, 120, 200, 100];
        let mut handles = vec![];

        for bid in candidates {
            let t = tracker.clone();
            let block = make_block(bid);
            let handle = tokio::spawn(async move { t.try_and_update(block).await });
            handles.push(handle);
        }

        let _results = futures::future::join_all(handles).await;
        // Some updates might fail (lower bids), some might succeed.
        // We only care that the final store best_block matches the highest bid submitted (200).

        let final_best = store.get_best_block().await.unwrap();
        assert_eq!(final_best.trace.bid_value, U256::from(200));
    }
}
