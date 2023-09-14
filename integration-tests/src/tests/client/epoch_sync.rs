use super::utils::TestEnvNightshadeSetupExt;
use near_chain::types::RuntimeAdapter;
use near_chain::{
    BlockProcessingArtifact, ChainGenesis, ChainStoreAccess, DoneApplyChunkCallback, Provenance,
};
use near_chain_configs::Genesis;
use near_client::sync::state::StateSync;
use near_client::test_utils::TestEnv;
use near_client::{Client, ProcessTxResponse};
use near_crypto::{InMemorySigner, KeyType};
use near_epoch_manager::{EpochManager, EpochManagerHandle};
use near_o11y::testonly::init_test_logger;
use near_primitives::network::PeerId;
use near_primitives::state_part::PartId;
use near_primitives::syncing::get_num_state_parts;
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_primitives::types::EpochId;
use near_primitives::utils::MaybeValidated;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::BlockHeight;
use near_store::genesis::initialize_genesis_state;
use near_store::{get_postponed_receipt, DBCol, NodeStorage, Store};
use nearcore::config::GenesisExt;
use nearcore::NightshadeRuntime;
use std::sync::Arc;

fn generate_transactions(last_hash: &CryptoHash, h: BlockHeight) -> Vec<SignedTransaction> {
    let mut txs = vec![];
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    if h == 1 {
        txs.push(SignedTransaction::from_actions(
            h,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::DeployContract(DeployContractAction {
                code: near_test_contracts::rs_contract().to_vec(),
            })],
            last_hash.clone(),
        ));
    }

    for i in 0..5 {
        txs.push(SignedTransaction::from_actions(
            h * 10 + i,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::FunctionCall(FunctionCallAction {
                method_name: "write_random_value".to_string(),
                args: vec![],
                gas: 100_000_000_000_000,
                deposit: 0,
            })],
            last_hash.clone(),
        ));
    }

    for i in 0..5 {
        txs.push(SignedTransaction::send_money(
            h * 10 + i,
            "test0".parse().unwrap(),
            "test1".parse().unwrap(),
            &signer,
            1,
            last_hash.clone(),
        ));
    }
    txs
}

/// Produce 4 epochs with some transactions.
/// At the end of each epoch check that `EpochSyncInfo` has been recorded.
#[test]
fn test_continuous_epoch_sync_info_population() {
    init_test_logger();

    let epoch_length = 5;
    let max_height = epoch_length * 4 + 1;

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);

    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    let mut last_hash = *env.clients[0].chain.genesis().hash();

    for h in 1..max_height {
        for tx in generate_transactions(&last_hash, h) {
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        last_hash = *block.hash();

        if env.clients[0].epoch_manager.is_next_block_epoch_start(&last_hash).unwrap() {
            let epoch_id = block.header().epoch_id().clone();

            tracing::debug!("Checking epoch: {:?}", &epoch_id);
            assert!(env.clients[0].chain.store().get_epoch_sync_info(&epoch_id).is_ok());
            tracing::debug!("OK");
        }
    }
}

/// Check that everything epoch sync related for this hash was written into the DB
fn check_epoch_sync_data(clients: &Vec<Client>, block_hash: &CryptoHash) {
    assert_eq!(
        clients[0].epoch_manager.get_block_info(block_hash).unwrap(),
        clients[1].epoch_manager.get_block_info(block_hash).unwrap()
    );
    assert_eq!(
        clients[0].chain.get_block_header(block_hash).unwrap(),
        clients[1].chain.get_block_header(block_hash).unwrap()
    );
}

/// Check that EpochSyncInfo column is populated for a given epoch_id
fn check_epoch_sync_info(clients: &Vec<Client>, epoch_id: &EpochId) {
    assert_eq!(
        clients[0].chain.store().get_epoch_sync_info(&epoch_id).unwrap(),
        clients[1].chain.store().get_epoch_sync_info(&epoch_id).unwrap()
    );
}

/// Check that block header head is correct
fn check_header_head(clients: &Vec<Client>) {
    assert_eq!(
        clients[0].chain.header_head().unwrap().last_block_hash,
        clients[1].chain.header_head().unwrap().last_block_hash
    );
}

/// Run two clients, first one is a validator and producing blocks, the second one is behind.
/// After each epoch we generate epoch sync data with first client and handle it on second client.
/// After that we check that relevant block headers and block infos had been written into second DB.
#[test]
fn test_epoch_sync_message_generation_and_handling() {
    init_test_logger();
    let num_clients = 2;
    let epoch_length = 5;
    let num_epochs = 4;
    let max_height = epoch_length * num_epochs + 1;

    // TestEnv setup
    let mut genesis = Genesis::test_with_genesis_height(
        vec!["test0".parse().unwrap(), "test1".parse().unwrap()],
        1,
        13,
    );
    genesis.config.epoch_length = epoch_length;

    let env_objects = (0..num_clients).map(|_|{
        let tmp_dir = tempfile::tempdir().unwrap();
        // Use default StoreConfig rather than NodeStorage::test_opener so we’re using the
        // same configuration as in production.
        let store= NodeStorage::opener(&tmp_dir.path(), false, &Default::default(), None)
            .open()
            .unwrap()
            .get_hot_store();
        initialize_genesis_state(store.clone(), &genesis, Some(tmp_dir.path()));
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let runtime =
            NightshadeRuntime::test(tmp_dir.path(), store.clone(), &genesis.config, epoch_manager.clone())
                as Arc<dyn RuntimeAdapter>;
        (tmp_dir, store, epoch_manager, runtime)
    }).collect::<Vec<(tempfile::TempDir, Store, Arc<EpochManagerHandle>, Arc<dyn RuntimeAdapter>)>>();

    let stores = env_objects.iter().map(|x| x.1.clone()).collect::<Vec<_>>();
    let epoch_managers = env_objects.iter().map(|x| x.2.clone()).collect::<Vec<_>>();
    let runtimes = env_objects.iter().map(|x| x.3.clone()).collect::<Vec<_>>();

    let mut env = TestEnv::builder(ChainGenesis::test())
        .clients_count(env_objects.len())
        .stores(stores.clone())
        .epoch_managers(epoch_managers)
        .runtimes(runtimes)
        .use_state_snapshots()
        .build();

    // Produce blocks
    let mut last_hash = *env.clients[0].chain.genesis().hash();
    let mut blocks_per_epoch = vec![];

    for h in 1..max_height {
        for tx in generate_transactions(&last_hash, h) {
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        last_hash = *block.hash();

        blocks_per_epoch.push(block.hash().clone());

        // On epoch end
        if env.clients[0].epoch_manager.is_next_block_epoch_start(&last_hash).unwrap() {
            let epoch_id = block.header().epoch_id().clone();

            // TODO: change this to obtaining a message
            let epoch_sync_data =
                env.clients[0].chain.store().get_epoch_sync_info(&epoch_id).unwrap();
            // TODO: change this to handling a message
            env.clients[1].chain.sync_epoch_sync_info(&epoch_sync_data).unwrap();
            // Check that relevant data is in the second client's DB
            check_epoch_sync_data(&env.clients, &blocks_per_epoch[0]);
            check_epoch_sync_data(&env.clients, &blocks_per_epoch[blocks_per_epoch.len() - 1]);
            check_epoch_sync_data(&env.clients, &blocks_per_epoch[blocks_per_epoch.len() - 2]);
            check_epoch_sync_info(&env.clients, &epoch_id);
            check_header_head(&env.clients);
            // TODO: add assert_eq on block_info and block_headers between clients
            blocks_per_epoch.clear();
        }
    }
}

#[test]
fn test_node_after_simulated_sync() {
    init_test_logger();
    let num_clients = 2;
    let epoch_length = 10;
    let num_epochs = 2;
    let max_height = epoch_length * num_epochs - 1;

    // TestEnv setup
    let mut genesis = Genesis::test_with_genesis_height(
        vec!["test0".parse().unwrap(), "test1".parse().unwrap()],
        1,
        13,
    );
    genesis.config.epoch_length = epoch_length;

    let env_objects = (0..num_clients).map(|_|{
        let tmp_dir = tempfile::tempdir().unwrap();
        // Use default StoreConfig rather than NodeStorage::test_opener so we’re using the
        // same configuration as in production.
        let store= NodeStorage::opener(&tmp_dir.path(), false, &Default::default(), None)
            .open()
            .unwrap()
            .get_hot_store();
        initialize_genesis_state(store.clone(), &genesis, Some(tmp_dir.path()));
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let runtime =
            NightshadeRuntime::test(tmp_dir.path(), store.clone(), &genesis.config, epoch_manager.clone())
                as Arc<dyn RuntimeAdapter>;
        (tmp_dir, store, epoch_manager, runtime)
    }).collect::<Vec<(tempfile::TempDir, Store, Arc<EpochManagerHandle>, Arc<dyn RuntimeAdapter>)>>();

    let stores = env_objects.iter().map(|x| x.1.clone()).collect::<Vec<_>>();
    let epoch_managers = env_objects.iter().map(|x| x.2.clone()).collect::<Vec<_>>();
    let runtimes = env_objects.iter().map(|x| x.3.clone()).collect::<Vec<_>>();

    let mut env = TestEnv::builder(ChainGenesis::test())
        .clients_count(env_objects.len())
        .stores(stores.clone())
        .epoch_managers(epoch_managers)
        .runtimes(runtimes)
        .use_state_snapshots()
        .build();

    env.clients[1].chain.state_snapshot_helper = None;

    // Produce blocks
    let mut last_hash = *env.clients[0].chain.genesis().hash();
    let mut blocks_per_epoch = vec![];

    for h in 1..max_height {
        for tx in generate_transactions(&last_hash, h) {
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        last_hash = *block.hash();
        blocks_per_epoch.push(block.hash().clone());

        // On epoch end
        if env.clients[0].epoch_manager.is_next_block_epoch_start(&last_hash).unwrap() {
            blocks_per_epoch.clear();
        }
    }

    // TODO: loop sync_epoch_sync_info, while header head is too far away
    let epoch_id0 = env.clients[0].chain.header_head().unwrap().epoch_id;
    let mut next_epoch_id1 = env.clients[1]
        .epoch_manager
        .get_next_epoch_id(&env.clients[1].chain.header_head().unwrap().last_block_hash)
        .unwrap();

    while next_epoch_id1 != epoch_id0 {
        tracing::debug!("Syncing epoch {:?}", next_epoch_id1);

        let epoch_sync_data =
            env.clients[0].chain.store().get_epoch_sync_info(&next_epoch_id1).unwrap();
        env.clients[1].chain.sync_epoch_sync_info(&epoch_sync_data).unwrap();

        next_epoch_id1 = env.clients[1]
            .epoch_manager
            .get_next_epoch_id(&env.clients[1].chain.header_head().unwrap().last_block_hash)
            .unwrap();
    }

    // TODO: do "header sync" for the last epoch
    tracing::debug!("Client 0 Header Head: {:?}", env.clients[0].chain.header_head());
    tracing::debug!("Client 1 Header Head Before: {:?}", env.clients[1].chain.header_head());

    let mut last_epoch_headers = vec![];
    for block_hash in &blocks_per_epoch {
        last_epoch_headers.push(env.clients[0].chain.get_block_header(block_hash).unwrap());
    }
    env.clients[1].chain.sync_block_headers(last_epoch_headers, &mut vec![]).unwrap();

    tracing::debug!("Client 0 Header Head: {:?}", env.clients[0].chain.header_head());
    tracing::debug!("Client 1 Header Head After: {:?}", env.clients[1].chain.header_head());

    // TODO: do "state sync" for the last epoch

    // write last block of prev epoch
    {
        let mut store_update = env.clients[1].chain.store().store().store_update();

        let last_hash = env.clients[0]
            .chain
            .get_block_header(&blocks_per_epoch[0])
            .unwrap()
            .prev_hash()
            .clone();
        let last_block = env.clients[0].chain.get_block(&last_hash).unwrap();

        tracing::debug!("Write block {:?}", last_block.header());

        store_update.insert_ser(DBCol::Block, last_hash.as_ref(), &last_block).unwrap();
        store_update.commit().unwrap();
    }

    let epoch_id = epoch_id0;
    let sync_hash = env.clients[0]
        .epoch_manager
        .get_block_info(&env.clients[0].chain.header_head().unwrap().last_block_hash)
        .unwrap()
        .epoch_first_block()
        .clone();
    tracing::debug!("SYNC HASH: {:?}", sync_hash);
    for shard_id in 0..env.clients[0].epoch_manager.num_shards(&epoch_id).unwrap() {
        tracing::debug!("Start syncing shard {:?}", shard_id);
        let sync_block_header = env.clients[0].chain.get_block_header(&sync_hash).unwrap();
        let sync_prev_header =
            env.clients[0].chain.get_previous_header(&sync_block_header).unwrap();
        let sync_prev_prev_hash = sync_prev_header.prev_hash();

        let state_header = env.clients[0]
            .chain
            .compute_state_response_header(shard_id, sync_hash.clone())
            .unwrap();
        let state_root = state_header.chunk_prev_state_root();
        let num_parts = get_num_state_parts(state_header.state_root_node().memory_usage);

        for part_id in 0..num_parts {
            tracing::debug!("Syncing part {:?} of {:?}", part_id, num_parts);
            let state_part = env.clients[0]
                .chain
                .runtime_adapter
                .obtain_state_part(
                    shard_id,
                    sync_prev_prev_hash,
                    &state_root,
                    PartId::new(part_id, num_parts),
                )
                .unwrap();

            env.clients[1]
                .runtime_adapter
                .apply_state_part(
                    shard_id,
                    &state_root,
                    PartId::new(part_id, num_parts),
                    state_part.as_ref(),
                    &epoch_id,
                )
                .unwrap();
        }
    }

    env.clients[1]
        .chain
        .reset_heads_post_state_sync(
            &None,
            sync_hash,
            &mut BlockProcessingArtifact::default(),
            Arc::new(|_| {}),
        )
        .unwrap();

    tracing::debug!("Client 0 Head: {:?}", env.clients[0].chain.head());
    tracing::debug!("Client 1 Head: {:?}", env.clients[1].chain.head());

    // TODO: do body sync for the last epoch

    for block_hash in &blocks_per_epoch {
        let block = env.clients[0].chain.get_block(block_hash).unwrap();
        tracing::debug!("Receive block {:?}", block.header());
        env.clients[1].process_block_test(MaybeValidated::from(block), Provenance::NONE).unwrap();
    }

    // TODO: produce blocks on clients[0] and process them on clients[1]

    for h in max_height..(max_height + 2 * epoch_length) {
        tracing::debug!("Produce and process block {}", h);
        for tx in generate_transactions(&last_hash, h) {
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        env.process_block(1, block.clone(), Provenance::NONE);
        last_hash = *block.hash();
        blocks_per_epoch.push(block.hash().clone());

        // On epoch end
        if env.clients[0].epoch_manager.is_next_block_epoch_start(&last_hash).unwrap() {
            blocks_per_epoch.clear();
        }
    }
}
