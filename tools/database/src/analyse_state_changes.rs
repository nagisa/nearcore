use std::collections::HashMap;
use near_store::{DBCol, Mode, NodeStorage};
use std::path::Path;
use near_chain_configs::GenesisValidationMode;
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, EpochId, RawStateChangesWithTrieKey, StateChanges, StateChangeValue, StoreKey};
use near_primitives::utils::index_to_bytes;
use nearcore::load_config;
use near_primitives::types::StateChangesExt;
use near_epoch_manager::EpochManager;

#[derive(clap::Parser)]
pub(crate) struct AnalyseStateChangesCommand {
    #[clap(long)]
    from_height: Option<BlockHeight>,
    #[clap(long)]
    to_height: Option<BlockHeight>,
    #[clap(long)]
    epoch_id: Option<CryptoHash>,
}

impl AnalyseStateChangesCommand {
    pub(crate) fn run(&self, home_dir: &Path) -> anyhow::Result<()> {
        let near_config = load_config(home_dir, GenesisValidationMode::UnsafeFast)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        let cold_config: Option<&near_store::StoreConfig> = near_config.config.cold_store.as_ref();
        let store_opener = NodeStorage::opener(
            home_dir,
            near_config.config.archive,
            &near_config.config.store,
            cold_config,
        );

        let storage = store_opener.open_in_mode(Mode::ReadOnly).unwrap();
        let store = storage.get_hot_store();

        let epoch_manager =
            EpochManager::new_from_genesis_config(store.clone(), &near_config.genesis.config)
                .expect("Failed to start Epoch Manager");

        let (from_height, to_height) = if let Some(epoch_id) = &self.epoch_id {
            let from_height = epoch_manager.get_epoch_start_from_epoch_id(&EpochId(*epoch_id))?;
            let to_height = from_height + 12 * 60 * 60;
            (from_height, to_height)
        } else {
            (self.from_height.unwrap(), self.to_height.unwrap())
        };

        let mut height = from_height;
        let mut block_hash = store.get_ser::<CryptoHash>(DBCol::BlockHeight, &index_to_bytes(height)).unwrap().unwrap();
        let mut counter_map = HashMap::new();
        while height < to_height {
            for change in StateChanges::from_changes(store.iter_prefix_ser::<RawStateChangesWithTrieKey>(
                DBCol::StateChanges,
                block_hash.as_ref(),
            ).map(|r| r.map(|s| s.1)))? {
                //for change in changes.iter() {
                    let key = match change.value {
                        StateChangeValue::DataUpdate {account_id, key, ..} => {
                            (change.cause, account_id, key)
                        }
                        StateChangeValue::DataDeletion {account_id, key} => {
                            (change.cause, account_id, key)
                        }
                        _ => {
                            (change.cause, change.value.affected_account_id().clone(), StoreKey::from(vec![]))
                        }
                    };
                    counter_map.entry(key).and_modify(|v| *v += 1).or_insert(1);
                //}
            }

            let next_hash = store.get_ser::<CryptoHash>(DBCol::NextBlockHashes, block_hash.as_ref()).unwrap().unwrap();
            let next_height = store.get_ser::<BlockHeader>(DBCol::BlockHeader, next_hash.as_ref()).unwrap().unwrap().height();
            height = next_height;
            block_hash = next_hash;
        }
        for (key, value) in counter_map.iter() {
            println!("{:?}\t{:?}", key, value);
        }
        Ok(())
    }
}
