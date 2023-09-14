use chrono::{DateTime, Duration, Utc};
use near_async::messaging::CanSend;
use near_chain::Chain;
use near_client_primitives::types::SyncStatus;
use near_network::types::{
    HighestHeightPeerInfo, NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest,
};
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::static_clock::StaticClock;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{BlockHeight, EpochId};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::{HashMap, HashSet};
use std::time::Duration as TimeDuration;

/// Helper to keep track of the Epoch Sync
// TODO #3488
#[allow(dead_code)]
pub struct EpochSync {
    network_adapter: PeerManagerAdapter,
    /// Datastructure to keep track of when the last request to each peer was made.
    /// Peers do not respond to Epoch Sync requests more frequently than once per a certain time
    /// interval, thus there's no point in requesting more frequently.
    peer_to_last_request_time: HashMap<PeerId, DateTime<Utc>>,
    /// Tracks all the peers who have reported that we are already up to date
    peers_reporting_up_to_date: HashSet<PeerId>,
    /// The last epoch we are synced to
    current_epoch_id: EpochId,
    /// The next epoch id we need to sync
    next_epoch_id: EpochId,
    /// The block producers set to validate the light client block view for the next epoch
    next_block_producers: Vec<ValidatorStake>,
    /// The last epoch id that we have requested
    pub requested_epoch_id: EpochId,
    /// When and to whom was the last request made
    last_request_time: DateTime<Utc>,
    pub last_request_peer_id: Option<PeerId>,

    /// How long to wait for a response before re-requesting the same light client block view
    request_timeout: Duration,
    /// How frequently to send request to the same peer
    peer_timeout: Duration,

    /// True, if all peers agreed that we're at the last Epoch.
    /// Only finalization is needed.
    have_all_epochs: bool,
    /// Whether the Epoch Sync was performed to completion previously.
    /// Current state machine allows for only one Epoch Sync.
    pub done: bool,

    pub sync_hash: CryptoHash,

    received_epoch: bool,

    is_just_started: bool,
}

impl EpochSync {
    pub fn new(
        network_adapter: PeerManagerAdapter,
        genesis_epoch_id: EpochId,
        genesis_next_epoch_id: EpochId,
        first_epoch_block_producers: Vec<ValidatorStake>,
        request_timeout: TimeDuration,
        peer_timeout: TimeDuration,
    ) -> Self {
        Self {
            network_adapter,
            peer_to_last_request_time: HashMap::new(),
            peers_reporting_up_to_date: HashSet::new(),
            current_epoch_id: genesis_epoch_id.clone(),
            next_epoch_id: genesis_next_epoch_id,
            next_block_producers: first_epoch_block_producers,
            requested_epoch_id: genesis_epoch_id,
            last_request_time: StaticClock::utc(),
            last_request_peer_id: None,
            request_timeout: Duration::from_std(request_timeout).unwrap(),
            peer_timeout: Duration::from_std(peer_timeout).unwrap(),
            received_epoch: false,
            have_all_epochs: false,
            done: false,
            sync_hash: CryptoHash::default(),
            is_just_started: true,
        }
    }

    pub fn epoch_sync_due(
        &self,
        sync_status: &mut SyncStatus,
        chain: &Chain,
        highest_height: BlockHeight,
    ) -> Result<bool, near_chain::Error> {
        let header_head = chain.header_head()?;
        let epoch_id = chain.epoch_manager.get_epoch_id(&header_head.last_block_hash)?;
        let epoch_length = chain.epoch_manager.get_estimated_epoch_length(&epoch_id)?;
        // let is_at_epoch_end = chain.epoch_manager.is_next_block_epoch_start(&header_head.last_block_hash)?;

        Ok(header_head.height + epoch_length < highest_height)
    }

    // TODO(posvyatokum): implement
    pub fn should_continue(
        &self,
        sync_status: &mut SyncStatus,
        chain: &Chain,
        highest_height: BlockHeight,
    ) -> Result<bool, near_chain::Error> {
        self.epoch_sync_due(sync_status, chain, highest_height)
    }

    pub fn run(
        &mut self,
        sync_status: &mut SyncStatus,
        chain: &Chain,
        highest_height: BlockHeight,
        highest_height_peers: &[PeerId],
    ) -> Result<(), near_chain::Error> {
        let _span = tracing::debug_span!(target: "sync", "run", sync = "EpochSync").entered();
        if !self.epoch_sync_due(sync_status, chain, highest_height)? {
            return Ok(());
        }
        let header_head = chain.header_head()?;
        // Get epoch_id for which we should run sync
        let epoch_id =
            if chain.epoch_manager.is_next_block_epoch_start(&header_head.last_block_hash)? {
                chain.epoch_manager.get_next_epoch_id(&header_head.last_block_hash)?
            } else {
                chain.epoch_manager.get_epoch_id(&header_head.last_block_hash)?
            };
        *sync_status = SyncStatus::EpochSync { epoch_id: epoch_id.clone() };
        if let Some(peer_id) = highest_height_peers.choose(&mut thread_rng()).cloned() {
            self.request_epoch(&epoch_id, peer_id.clone());
        }
        Ok(())
    }

    pub fn request_epoch(&mut self, epoch_id: &EpochId, peer_id: PeerId) {
        tracing::debug!(target: "sync", "Sync: request epoch sync info: asking {} for epoch {:?}", peer_id, epoch_id);
        self.requested_epoch_id = epoch_id.clone();
        self.last_request_peer_id = Some(peer_id.clone());
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::EpochSyncInfoRequest { epoch_id: epoch_id.clone(), peer_id },
        ));
    }
}
