#!/usr/bin/env python3
# Spins up one validating node.
# Spins a non-validating node that tracks some shards and the set of tracked shards changes regularly.
# The node gets stopped, and gets restarted close to an epoch boundary but in a way to trigger epoch sync.
#
# This test is a regression test to ensure that the node doesn't panic during
# function execution during block sync after a state sync.

import pathlib
import random
import sys
import tempfile
import requests

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config, apply_config_changes
import account
import transaction
import utils

from configured_logger import logger

EPOCH_LENGTH = 500


def epoch_height(block_height):
    if block_height == 0:
        return 0
    if block_height <= EPOCH_LENGTH:
        # According to the protocol specifications, there are two epochs with height 1.
        return "1*"
    return int((block_height - 1) / EPOCH_LENGTH)


def random_u64():
    return bytes(random.randint(0, 255) for _ in range(8))


def call_function(op, key, nonce, signer_key, last_block_hash, tx_node):
    if op == 'read':
        args = key
        fn = 'read_value'
    else:
        args = key + random_u64()
        fn = 'write_key_value'

    tx = transaction.sign_function_call_tx(signer_key, signer_key.account_id,
                                           fn, args, 300 * account.TGAS, 0,
                                           nonce, last_block_hash)
    return tx_node.send_tx(tx).get('result')


state_parts_dir = str(pathlib.Path(tempfile.gettempdir()) / 'state_parts')

config0 = {
    'enable_multiline_logging': False,
    'gc_num_epochs_to_keep': 100,
    'log_summary_period': {
        'secs': 0,
        'nanos': 100000000
    },
    'log_summary_style': 'plain',
    'state_sync': {
        'dump': {
            'location': {
                'Filesystem': {
                    'root_dir': state_parts_dir
                }
            },
            'iteration_delay': {
                'secs': 0,
                'nanos': 100000000
            },
        }
    },
    'store.state_snapshot_enabled': True,
    'tracked_shards': [0],
}

def get_config(num_validators, index):
    config = {
        'enable_multiline_logging': False,
        'gc_num_epochs_to_keep': 100,
        'log_summary_period': {
            'secs': 0,
            'nanos': 100000000
        },
        'log_summary_style': 'plain',
        'state_sync': {
            'sync': {
                'ExternalStorage': {
                    'location': {
                        'Filesystem': {
                            'root_dir': state_parts_dir
                        }
                    }
                }
            }
        },
        'state_sync_enabled': True,
        'consensus.state_sync_timeout': {
            'secs': 0,
            'nanos': 500000000
        },
        'tracked_shard_schedule': [],
        'tracked_shards': [],
    }
    if index == num_validators:
        config['tracked_shards'] = [0]
    else:
        for i in range(20):
            if random.random() < 0.5:
                config['tracked_shard_schedule'].append(
                    [
                        random.randint(0, 3),
                    ]
                )
            else:
                config['tracked_shard_schedule'].append(
                    [
                        random.randint(0, 3),
                        random.randint(0, 3)
                    ]
                )

    return config

num_validators = 6
num_non_validators = 4
num_nodes = num_validators + num_non_validators

config = load_config()
near_root, node_dirs = init_cluster(num_validators, num_non_validators, 4, config, [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 1], ["chunk_producer_kickout_threshold", 1]], {i: config0 if i < num_validators else get_config(num_validators, i) for i in range(num_nodes)})

i = 0
node = spin_up_node(config, near_root, node_dirs[i], i)
boot_nodes = [node]
for i in range(1, num_validators):
    boot_nodes.append(spin_up_node(config, near_root, node_dirs[i], i, boot_node=boot_nodes[0]))
logger.info('started boot_nodes')

other_nodes = [spin_up_node(config, near_root, node_dirs[i], i, boot_node=random.choice(boot_nodes)) for i in range(num_validators, num_nodes)]
logger.info('started nodes')

cur_height = boot_nodes[0].get_latest_block().height
logger.info(f'cur_height: {cur_height}')

all_nodes = boot_nodes + other_nodes
node_account_ids = [node.signer_key.account_id for node in all_nodes]
all_account_ids = node_account_ids + ["near"]

contract = utils.load_test_contract()

for node in boot_nodes:
    latest_block_hash = boot_nodes[0].get_latest_block().hash_bytes
    deploy_contract_tx = transaction.sign_deploy_contract_tx(
        node.signer_key, contract, 10, latest_block_hash)
    result = boot_nodes[0].send_tx_and_wait(deploy_contract_tx, 10)
    assert 'result' in result and 'error' not in result, (
        'Expected "result" and no "error" in response, got: {}'.format(result))
    logger.info(f'deployed contract to {node.signer_key.account_id}')

def random_workload_until2(target, nonce, keys, target_node, alive_nodes):
    last_height = -1
    last_kill = -1
    while True:
        nonce += 1
        last_block = target_node.get_latest_block()
        height = last_block.height
        logger.info(height)
        if height != last_height:
            logger.info(f'{target_node.signer_key.account_id}@{height}, epoch_height: {epoch_height(height)}')
            if height > target:
                break
            last_height = height

        tx_node = random.choice(boot_nodes)

        last_block_hash = boot_nodes[0].get_latest_block().hash_bytes
        if random.random() < 0.5:
            # Make a transfer between shards.
            # The goal is to generate cross-shard receipts.
            key_from = random.choice(all_nodes).signer_key
            account_to = random.choice(all_account_ids)
            payment_tx = transaction.sign_payment_tx(key_from, account_to, 1, nonce, last_block_hash)
            tx_node.send_tx(payment_tx).get('result')
        elif (len(keys) > 100 and random.random() < 0.5) or len(keys) > 1000:
            # Do some flat storage reads, but only if we have enough keys populated.
            key = keys[random.randint(0, len(keys) - 1)]
            call_function('read', key, nonce, random.choice(boot_nodes).signer_key, last_block_hash, tx_node)
        else:
            # Generate some data for flat storage reads
            key = random_u64()
            keys.append(key)
            for node in boot_nodes:
                call_function('write', key, nonce, node.signer_key, last_block_hash, tx_node)

        if last_kill + 10 < last_height:
            node = random.choice(other_nodes)
            try:
                h = requests.get("http://%s:%s/status" % node.rpc_addr(), timeout=2).json()['sync_info']['latest_block_height']
                if h + 10 > last_height:
                    logger.info(f'Node {node.signer_key.account_id} is in sync')
                    node.kill()
                    node.reset_data()
                    logger.info(f'Killed {node.signer_key.account_id}')
                    node.start(boot_node=boot_nodes[0])
                    res = node.json_rpc('adv_disable_doomslug', [])
                    logger.info(f'Started {node.signer_key.account_id}')
                else:
                    logger.info(f'Node {node.signer_key.account_id} is not in sync')
            except:
                logger.info(f'Node {node.signer_key.account_id} is down')

    return nonce, keys

# Generates traffic for all possible shards.
# Assumes that `test0`, `test1`, `near` all belong to different shards.
def random_workload_until(target, nonce, keys, target_node, alive_nodes):
    last_height = -1
    while True:
        nonce += 1
        """
        if False and nonce % (int(EPOCH_LENGTH/2)) == 0:
            heights = [(node.signer_key.account_id, node.get_latest_block().height) for node in alive_nodes]
            logger.info(f'heights: {heights}')
        """

        last_block = target_node.get_latest_block()
        height = last_block.height
        logger.info(height)
        if height != last_height:
            logger.info(f'{target_node.signer_key.account_id}@{height}, epoch_height: {epoch_height(height)}')
            if height > target:
                break
            last_height = height

        tx_node = random.choice(boot_nodes)

        last_block_hash = boot_nodes[0].get_latest_block().hash_bytes
        if random.random() < 0.5:
            # Make a transfer between shards.
            # The goal is to generate cross-shard receipts.
            key_from = random.choice(all_nodes).signer_key
            account_to = random.choice(all_account_ids)
            payment_tx = transaction.sign_payment_tx(key_from, account_to, 1, nonce, last_block_hash)
            tx_node.send_tx(payment_tx).get('result')
        elif (len(keys) > 100 and random.random() < 0.5) or len(keys) > 1000:
            # Do some flat storage reads, but only if we have enough keys populated.
            key = keys[random.randint(0, len(keys) - 1)]
            call_function('read', key, nonce, random.choice(boot_nodes).signer_key, last_block_hash, tx_node)
        else:
            # Generate some data for flat storage reads
            key = random_u64()
            keys.append(key)
            for node in boot_nodes:
                call_function('write', key, nonce, node.signer_key, last_block_hash, tx_node)

    return nonce, keys

nonce, keys = random_workload_until(cur_height + EPOCH_LENGTH * 2, 1, [], boot_nodes[0], all_nodes)

for node in other_nodes:
    node.kill()
    node.reset_data()
    logger.info(f'killed {node.signer_key.account_id}')

# Disable doomslug to cause forks, but keep one validator node with the correct finality mechanism.
for node in boot_nodes:
    res = node.json_rpc('adv_disable_doomslug', [])
    assert 'result' in res, res
    logger.info(f'Disabled doomslug on {node.signer_key.account_id}')

cur_height = boot_nodes[0].get_latest_block().height
logger.info(f'cur_height: {cur_height}')

nonce, keys = random_workload_until(EPOCH_LENGTH * 2 + 5, 1, [], boot_nodes[0], all_nodes)

# Nodes are now behind and needs to do header sync and block sync.
for node in other_nodes:
    node.start(boot_node=boot_nodes[0])
    res = node.json_rpc('adv_disable_doomslug', [])
    assert 'result' in res, res
    logger.info(f'{node.signer_key.account_id}: Started and disabled doomslug')

# Run node0 more to trigger block sync in node1.

cur_height = boot_nodes[0].get_latest_block().height
logger.info(f'cur_height: {cur_height}')
