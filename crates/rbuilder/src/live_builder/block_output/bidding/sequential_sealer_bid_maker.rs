use crate::building::builders::best_block_store::BestBlockTracker;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::error;

use super::interfaces::{Bid, BidMaker};
use tracing::info;

/// BidMaker with a background task sealing only one bid at a time.
/// If several bids arrive while sealing another one we keep only the last one since we assume new is better.
#[derive(Debug)]
pub struct SequentialSealerBidMaker {
    pending_bid: Arc<PendingBid>,
}

impl BidMaker for SequentialSealerBidMaker {
    fn send_bid(&self, bid: Bid) {
        self.pending_bid.update(bid);
    }
}

/// Object used to send new bids to the [SequentialSealerBidMakerProcess].
#[derive(Debug)]
struct PendingBid {
    /// Next bid to send.
    bid: Mutex<Option<Bid>>,
    /// Signaled when we set a new bid.
    bid_notify: Notify,
}

impl PendingBid {
    fn new() -> Self {
        Self {
            bid: Default::default(),
            bid_notify: Notify::new(),
        }
    }
    pub async fn wait_for_change(&self) {
        self.bid_notify.notified().await
    }
    /// Updates bid, replacing  on current (we assume they are always increasing but we don't check it).
    fn update(&self, bid: Bid) {
        *self.bid.lock() = Some(bid);
        self.bid_notify.notify_one();
    }

    fn consume_bid(&self) -> Option<Bid> {
        self.bid.lock().take()
    }
}

impl SequentialSealerBidMaker {
    pub fn new(best_block_tracker: BestBlockTracker, cancel: CancellationToken) -> Self {
        let pending_bid = Arc::new(PendingBid::new());
        let mut sealing_process = SequentialSealerBidMakerProcess {
            cancel,
            pending_bid: pending_bid.clone(),
            best_block_tracker: best_block_tracker.clone(),
        };

        tokio::task::spawn(async move {
            sealing_process.run().await;
        });
        Self { pending_bid }
    }
}

/// Background task waiting for new bids to seal.
struct SequentialSealerBidMakerProcess {
    /// Destination of the finished blocks.
    cancel: CancellationToken,
    pending_bid: Arc<PendingBid>,
    best_block_tracker: BestBlockTracker,
}

impl SequentialSealerBidMakerProcess {
    async fn run(&mut self) {
        info!("SequentialSealerBidMakerProcess starting");
        self.best_block_tracker.init().await;
        info!("tracker bg initialized");
        loop {
            tokio::select! {
                _ = self.pending_bid.wait_for_change() => self.check_for_new_bid().await,
                _ = self.cancel.cancelled() => return
            }
        }
    }

    /// block.finalize_block inside spawn_blocking.
    async fn check_for_new_bid(&mut self) {
        if let Some(bid) = self.pending_bid.consume_bid() {
            let payout_tx_val = bid.payout_tx_value();
            let block = bid.block();
            let block_number = block.building_context().block();
            match tokio::task::spawn_blocking(move || block.finalize_block(payout_tx_val)).await {
                Ok(finalize_res) => match finalize_res {
                    Ok(res) => {
                        self.best_block_tracker.try_and_update(res.block).await;
                    }
                    Err(error) => {
                        if error.is_critical() {
                            error!(
                                block_number,
                                ?error,
                                "Error on finalize_block on SequentialSealerBidMaker"
                            )
                        }
                    }
                },
                Err(error) => error!(
                    block_number,
                    ?error,
                    "Error on join finalize_block on on SequentialSealerBidMaker"
                ),
            }
        }
    }
}
