use crate::tests::client::process_blocks::deploy_test_contract_with_protocol_version;
use assert_matches::assert_matches;
use near_chain::{ChainGenesis, RuntimeWithEpochManagerAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::test_utils::encode;
use near_primitives::transaction::{
    Action, ExecutionMetadata, ExecutionStatus, FunctionCallAction, Transaction,
};
use near_primitives::version::ProtocolFeature;
use near_primitives::views::FinalExecutionStatus;
use near_primitives_core::config::ExtCosts;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::Gas;
use near_store::test_utils::create_test_store;
use nearcore::config::GenesisExt;
use nearcore::TrackedConfig;
use std::path::Path;
use std::sync::Arc;

/// Check that after flat storage upgrade:
/// - value read from contract is the same;
/// - touching trie node cost for read decreases to zero.
#[test]
fn test_flat_storage_upgrade() {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let epoch_length = 12;
    let old_protocol_version = ProtocolFeature::FlatStorageReads.protocol_version() - 1;
    let new_protocol_version = old_protocol_version + 1;
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = old_protocol_version;
    let chain_genesis = ChainGenesis::new(&genesis);
    let runtimes: Vec<Arc<dyn RuntimeWithEpochManagerAdapter>> =
        vec![Arc::new(nearcore::NightshadeRuntime::test_with_runtime_config_store(
            Path::new("../../../.."),
            create_test_store(),
            &genesis,
            TrackedConfig::new_empty(),
            RuntimeConfigStore::new(None),
        ))];
    let mut env = TestEnv::builder(chain_genesis).runtime_adapters(runtimes).build();

    // Deploy contract to state.
    deploy_test_contract_with_protocol_version(
        &mut env,
        "test0".parse().unwrap(),
        near_test_contracts::base_rs_contract(),
        epoch_length / 3,
        1,
        old_protocol_version,
    );

    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let gas = 20_000_000_000_000;
    let tx = Transaction {
        signer_id: "test0".parse().unwrap(),
        receiver_id: "test0".parse().unwrap(),
        public_key: signer.public_key(),
        actions: vec![],
        nonce: 0,
        block_hash: CryptoHash::default(),
    };

    // Write key-value pair to state.
    {
        let write_value_action = vec![Action::FunctionCall(FunctionCallAction {
            args: encode(&[1u64, 10u64]),
            method_name: "write_key_value".to_string(),
            gas,
            deposit: 0,
        })];
        let tip = env.clients[0].chain.head().unwrap();
        let signed_transaction = Transaction {
            nonce: 10,
            block_hash: tip.last_block_hash,
            actions: write_value_action,
            ..tx.clone()
        }
        .sign(&signer);
        let tx_hash = signed_transaction.get_hash();
        env.clients[0].process_tx(signed_transaction, false, false);
        for i in 0..epoch_length / 3 {
            env.produce_block(0, tip.height + i + 1);
        }

        let final_result = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
        assert_matches!(final_result.status, FinalExecutionStatus::SuccessValue(_));
        let transaction_outcome = env.clients[0].chain.get_execution_outcome(&tx_hash).unwrap();
        let receipt_ids = transaction_outcome.outcome_with_id.outcome.receipt_ids;
        assert_eq!(receipt_ids.len(), 1);
        let receipt_execution_outcome =
            env.clients[0].chain.get_execution_outcome(&receipt_ids[0]).unwrap();
        assert_matches!(
            receipt_execution_outcome.outcome_with_id.outcome.status,
            ExecutionStatus::SuccessValue(_)
        );
    }

    let touching_trie_node_costs: Vec<_> = (0..2)
        .map(|i| {
            let read_value_action = vec![Action::FunctionCall(FunctionCallAction {
                args: encode(&[1u64]),
                method_name: "read_value".to_string(),
                gas,
                deposit: 0,
            })];
            let tip = env.clients[0].chain.head().unwrap();
            let signed_transaction = Transaction {
                nonce: 20 + i,
                block_hash: tip.last_block_hash,
                actions: read_value_action,
                ..tx.clone()
            }
            .sign(&signer);
            let tx_hash = signed_transaction.get_hash();
            env.clients[0].process_tx(signed_transaction, false, false);
            for i in 0..epoch_length / 3 {
                env.produce_block(0, tip.height + i + 1);
            }
            if i == 0 {
                env.upgrade_protocol(new_protocol_version);
            }

            let final_result = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
            assert_matches!(final_result.status, FinalExecutionStatus::SuccessValue(_));
            let transaction_outcome = env.clients[0].chain.get_execution_outcome(&tx_hash).unwrap();
            let receipt_ids = transaction_outcome.outcome_with_id.outcome.receipt_ids;
            assert_eq!(receipt_ids.len(), 1);
            let receipt_execution_outcome =
                env.clients[0].chain.get_execution_outcome(&receipt_ids[0]).unwrap();
            let metadata = receipt_execution_outcome.outcome_with_id.outcome.metadata;
            if let ExecutionMetadata::V3(profile_data) = metadata {
                profile_data.get_ext_cost(ExtCosts::touching_trie_node)
            } else {
                panic!("Too old version of metadata: {metadata:?}");
            }
        })
        .collect();

    let touching_trie_node_base_cost: Gas = 16_101_955_926;

    // For the first read, cost should be 3 TTNs because trie path is:
    // (Branch) -> (Extension) -> (Leaf) -> (Value)
    // but due to a bug in storage_read we don't charge for Value.
    assert_eq!(touching_trie_node_costs[0], touching_trie_node_base_cost * 3);

    // For the second read, we don't go to Flat storage and don't charge TTN.
    assert_eq!(touching_trie_node_costs[1], 0);
}
