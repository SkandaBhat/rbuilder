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
#[derive(Clone)]
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::building::{BuiltBlockTrace, SealedBlock};
    use alloy_primitives::U256;
    use futures::pin_mut;
    use std::time::Duration;
    use time::OffsetDateTime;

    #[tokio::test]
    async fn test_compare_and_update() {
        let store = GlobalBestBlockStore::new();
        let initial_block = Block {
            trace: BuiltBlockTrace {
                included_orders: vec![],
                bid_value: U256::from(100),
                true_bid_value: U256::from(100),
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
            builder_name: "test".to_string(),
        };

        let better_block = Block {
            trace: BuiltBlockTrace {
                included_orders: vec![],
                bid_value: U256::from(101),
                true_bid_value: U256::from(101),
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
            builder_name: "test".to_string(),
        };

        let worse_block = Block {
            trace: BuiltBlockTrace {
                included_orders: vec![],
                bid_value: U256::from(99),
                true_bid_value: U256::from(99),
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
            builder_name: "test".to_string(),
        };

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

        let block = Block {
            trace: BuiltBlockTrace {
                included_orders: vec![],
                bid_value: U256::from(100),
                true_bid_value: U256::from(100),
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
            builder_name: "test".to_string(),
        };

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
                let block = Block {
                    trace: BuiltBlockTrace {
                        included_orders: vec![],
                        bid_value: U256::from(v),
                        true_bid_value: U256::from(v),
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
                    builder_name: "concurrent_test".to_string(),
                };
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

        let block = Block {
            trace: BuiltBlockTrace {
                included_orders: vec![],
                bid_value: U256::from(150),
                true_bid_value: U256::from(150),
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
            builder_name: "multiple_subscribers_test".to_string(),
        };

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

        let better_block = Block {
            trace: BuiltBlockTrace {
                included_orders: vec![],
                bid_value: U256::from(200),
                true_bid_value: U256::from(200),
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
            builder_name: "multiple_subscribers_test".to_string(),
        };

        // Update again
        assert!(store.compare_and_update(better_block.clone()).await.is_ok());

        let late_received = late_subscriber.next().await.unwrap();
        assert_eq!(late_received.trace.bid_value, U256::from(200));
    }
}
