use super::TrieNodesCount;
use crate::TrieStorage;
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic;

/// Switch that controls whether the `TrieAccountingCache` is enabled.
pub struct TrieAccessTrackerSwitch(Arc<thread_local::ThreadLocal<atomic::AtomicBool>>);

impl TrieAccessTrackerSwitch {
    pub fn set(&self, enabled: bool) {
        self.0.get_or(Default::default).store(enabled, atomic::Ordering::Relaxed);
    }

    pub fn enabled(&self) -> bool {
        self.0.get_or(Default::default).load(atomic::Ordering::Relaxed)
    }
}

/// Deterministic cache to store trie nodes that have been accessed so far
/// during the cache's lifetime. It is used for deterministic gas accounting
/// so that previously accessed trie nodes and values are charged at a
/// cheaper gas cost.
///
/// This cache's correctness is critical as it contributes to the gas
/// accounting of storage operations during contract execution. For that
/// reason, a new TrieAccountingCache must be created at the beginning of a
/// chunk's execution, and the db_read_nodes and mem_read_nodes must be taken
/// into account whenever a storage operation is performed to calculate what
/// kind of operation it was.
///
/// Note that we don't have a size limit for values in the accounting cache.
/// There are two reasons:
///   - for nodes, value size is an implementation detail. If we change
///     internal representation of a node (e.g. change `memory_usage` field
///     from `RawTrieNodeWithSize`), this would have to be a protocol upgrade.
///   - total size of all values is limited by the runtime fees. More
///     thoroughly:
///       - number of nodes is limited by receipt gas limit / touching trie
///         node fee ~= 500 Tgas / 16 Ggas = 31_250;
///       - size of trie keys and values is limited by receipt gas limit /
///         lowest per byte fee (`storage_read_value_byte`) ~=
///         (500 * 10**12 / 5611005) / 2**20 ~= 85 MB.
/// All values are given as of 16/03/2022. We may consider more precise limit
/// for the accounting cache as well.
///
/// Note that in general, it is NOT true that all storage access is either a
/// db read or mem read. It can also be a flat storage read, which is not
/// tracked via TrieAccountingCache.
pub struct TrieAccessTracker {
    /// Whether the cache is enabled. By default it is not, but it can be turned on or off on the fly.
    enable: TrieAccessTrackerSwitch,
    /// Cache of trie node hash -> trie node body, or a leaf value hash ->
    /// leaf value.
    keys: BTreeSet<CryptoHash>,
    /// The number of times a key was accessed by reading from the underlying
    /// storage. (This does not necessarily mean it was accessed from *disk*,
    /// as the underlying storage layer may have a best-effort cache.)
    db_read_nodes: u64,
    /// The number of times a key was accessed when it was deterministically
    /// already cached during the processing of this chunk.
    mem_read_nodes: u64,
}

impl TrieAccessTracker {
    /// Constructs a new accounting cache. By default it is not enabled.
    /// The optional parameter is passed in if prometheus metrics are desired.
    pub fn new() -> Self {
        let switch = TrieAccessTrackerSwitch(Default::default());
        Self {
            enable: switch,
            keys: Default::default(),
            db_read_nodes: Default::default(),
            mem_read_nodes: Default::default(),
        }
    }

    pub fn enable_switch(&self) -> TrieAccessTrackerSwitch {
        TrieAccessTrackerSwitch(Arc::clone(&self.enable.0))
    }

    /// Retrieve raw bytes from the cache if it exists, otherwise retrieve it
    /// from the given storage, and count it as a db access.
    pub fn retrieve_raw_bytes_with_accounting(
        &mut self,
        hash: &CryptoHash,
        storage: &dyn TrieStorage,
    ) -> Result<Arc<[u8]>, StorageError> {
        let db_read = if self.enable.enabled() {
            self.keys.insert(hash.clone())
        } else {
            !self.keys.contains(hash)
        };
        if db_read {
            self.db_read_nodes += 1;
        } else {
            self.mem_read_nodes += 1;
        }
        let node = storage.retrieve_raw_bytes(hash)?;
        Ok(node)
    }

    /// Used to retroactively account for a node or value that was already accessed
    /// through other means (e.g. flat storage read).
    pub fn retroactively_account(&mut self, hash: CryptoHash) {
        let db_read = if self.enable.enabled() {
            self.keys.insert(hash.clone())
        } else {
            !self.keys.contains(&hash)
        };
        if db_read {
            self.db_read_nodes += 1;
        } else {
            self.mem_read_nodes += 1;
        }
    }

    pub fn get_trie_nodes_count(&self) -> TrieNodesCount {
        TrieNodesCount { db_reads: self.db_read_nodes, mem_reads: self.mem_read_nodes }
    }
}
