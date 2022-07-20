use std::path::Path;
use near_store::{Store, StoreConfig, DBCol, db::Mode};
use borsh::BorshDeserialize;
use near_primitives::transaction::{SignedTransaction, Action};
use near_primitives::receipt::{Receipt, ReceiptEnum};
use near_primitives::hash::CryptoHash;
use std::collections::HashMap;
use near_primitives::transaction::{ExecutionOutcomeWithIdAndProof, ExecutionMetadata, ExecutionOutcome};
use std::fs::File;
use std::io::{self, BufRead};
use std::str::FromStr;

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn get_height(header: &near_primitives::block_header::BlockHeader) -> u64 {
    match header {
        near_primitives::block_header::BlockHeader::BlockHeaderV1(a) => a.inner_lite.height,
        near_primitives::block_header::BlockHeader::BlockHeaderV2(a) => a.inner_lite.height,
        near_primitives::block_header::BlockHeader::BlockHeaderV3(a) => a.inner_lite.height,
    }
}

fn main() {
    let lines = read_lines("./x");
    if !lines.is_ok() {
        println!(":(");
        return ();
    }

    let store = Store::opener(&Path::new("/home/ubuntu/.near"), &StoreConfig::default()).mode(Mode::ReadOnly).open();

    /*for key_value in store.iter(DBCol::TransactionResult) {
        if key_value.is_err() {
            continue;
        }
        let (key, _) = key_value.unwrap();
        println!("{:?}", &key);
    }

    if 2 + 2 == 4 {
        return ();
    }*/

    let mut cnt = 0;
    for line in lines.unwrap() {
        let hash = CryptoHash::from_str(&line.unwrap()).unwrap();
        let execution_outcomes = store.get_ser::<Vec<ExecutionOutcomeWithIdAndProof>>(DBCol::TransactionResult, &hash.0).unwrap();
        if !execution_outcomes.is_some() {
            println!("Can't find outcome for receipt: {:?}", &hash);
            continue;
        }
        let execution_outcomes = execution_outcomes.unwrap();
        assert!(execution_outcomes.len() == 1, ":(");
        let mut heights = String::new();
        for execution_outcome in &execution_outcomes {
            let header = store.get_ser::<near_primitives::block_header::BlockHeader>(DBCol::BlockHeader, &execution_outcome.block_hash.0).unwrap().unwrap();
            heights = format!("{} ", &get_height(&header));
        }
        println!("{} {}", &hash, heights);
        cnt += 1;
        if cnt % 10000 == 0 {
            eprintln!("Progress: {}", cnt);
        }
    }

    if 2 + 2 == 4 {
        return ();
    }

    let mut attached_gas = HashMap::new();
    for key_value in store.iter(DBCol::Transactions) {
        if key_value.is_err() {
            continue;
        }
        let (key, value) = key_value.unwrap();
        let hash = CryptoHash::try_from_slice(&key).unwrap();
        let signed_transaction = SignedTransaction::try_from_slice(&value).unwrap();
        if signed_transaction.transaction.actions.len() != 1 {
            continue;
        }
        for action in &signed_transaction.transaction.actions {
            if let Action::FunctionCall(f) = action {
                let execution_outcomes = store.get_ser::<Vec<ExecutionOutcomeWithIdAndProof>>(DBCol::TransactionResult, &key).unwrap();
                if !execution_outcomes.is_some() {
                    continue;
                }
                let execution_outcomes = execution_outcomes.unwrap();
                assert!(execution_outcomes.len() == 1, ":(");
                let outcome = &execution_outcomes[0].outcome_with_id.outcome;
                assert!(outcome.receipt_ids.len() == 1, ":(");
                let receipt_id = &outcome.receipt_ids[0];
                // println!("{:?} {:?}", receipt_id, f.gas);
                attached_gas.insert(receipt_id.clone(), f.gas);
            }
        }
    }

    for key_value in store.iter(DBCol::TransactionResult) {
        if key_value.is_err() {
            continue;
        }
        let (key, value) = key_value.unwrap();
        let hash = CryptoHash::try_from_slice(&key).unwrap();
        let execution_outcome = Vec::<ExecutionOutcomeWithIdAndProof>::try_from_slice(&value).unwrap();
        /*println!("################################## BEGIN OUTCOME ###############");
        println!("{:?} {:?}", hash, execution_outcome);
        println!("################################## END ########################");*/
        for outcome in execution_outcome.iter() {
            let id = outcome.outcome_with_id.id;
            // println!("{}", id);
            if let ExecutionMetadata::V2(metadata) = &outcome.outcome_with_id.outcome.metadata {
                let mut gas = 0;
                if let Some(receipt) = store.get_ser::<Receipt>(DBCol::Receipts, &id.0).unwrap() {
                    println!("RECEIPT");
                    if let ReceiptEnum::Action(action) = receipt.receipt {
                        let mut cnt = 0;
                        for fc_action in &action.actions {
                            if let Action::FunctionCall(fc) = &fc_action {
                                gas = fc.gas;
                                cnt += 1;
                                let execution_outcomes = store.get_ser::<Vec<ExecutionOutcomeWithIdAndProof>>(DBCol::TransactionResult, &key).unwrap();
                                assert!(cnt <= 1, ":( {:?} XXXXXXXXXXXXX {:?} XXXXXXXXXXXX {:?}", hash, &action.actions, &execution_outcomes);
                            }
                        }
                    }
                } else if attached_gas.contains_key(&id) {
                    println!("TRANSACTION");
                    gas = attached_gas.get(&id).unwrap().clone();
                }
                if gas == 0 {
                    println!("NOT_FOUND {:?}", id);
                }
                /*println!("################################## BEGIN TRANSACTION_RESULT ####");
                println!("{:?} {:?} {:?}", hash, id, metadata);
                println!("################################## END ########################");
                continue;*/
            }
        }
    }
}
