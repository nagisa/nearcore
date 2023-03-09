#!/usr/bin/env python3

import argparse
import json
import pathlib
import random
import requests
import re
import sys
import subprocess
import time
from prometheus_client import start_http_server, Counter

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'pytest' / 'lib'))

import configured_logger

logger = configured_logger.new_logger("stderr", stderr=True)


def json_rpc(method, params, url):
    try:
        j = {
            'method': method,
            'params': params,
            'id': 'dontcare',
            'jsonrpc': '2.0'
        }
        start_time = time.time()
        r = requests.post(url, json=j, timeout=5)
        latency_ms = (time.time() - start_time)
        print(
            f'prober_request_latency_ms{{method="{method}",url="{url}"}} {latency_ms:.2f}'
        )
        result = json.loads(r.content)
        return result
    except Exception as e:
        logger.error(f'Query failed: {e}')
        sys.exit(1)


def get_genesis_height(url):
    try:
        genesis_config = json_rpc('EXPERIMENTAL_genesis_config', None, url)
        genesis_height = genesis_config['result']['genesis_height']
        logger.info(f'Got genesis_height {genesis_height}')
        return genesis_height
    except Exception as e:
        logger.error(f'get_genesis_height() failed: {e}')
        sys.exit(1)


def get_head(url):
    try:
        status = json_rpc('status', None, url)
        head = status['result']['sync_info']['latest_block_height']
        logger.info(f'Got latest_block_height {head}')
        return head
    except Exception as e:
        logger.error(f'get_head() failed: {e}')
        sys.exit(1)


def get_chain_id(url):
    try:
        status = json_rpc('status', None, url)
        chain_id = status['result']['chain_id']
        logger.info(f'Got chain_id {chain_id}')
        return chain_id
    except Exception as e:
        logger.error(f'get_chain_id() failed: {e}')
        sys.exit(1)


def stop_neard():
    logger.info(f'Stopping neard')
    subprocess.call(["sudo", "-S", "systemctl", "stop", "neard"])


def start_neard():
    logger.info(f'Starting neard')
    subprocess.call(["sudo", "-S", "systemctl", "start", "neard"])


def start_test(max_depth, max_count, height):
    args = ["/home/ubuntu/neard", "cold-store", "check-state-root"]

    if max_depth:
        args += ["--max-depth", max_depth]

    if max_count:
        args += ["--max-count", max_count]

    args += ["height", str(height)]

    logger.info(f"Starting state test subprocess {args}")
    return subprocess.Popen(args, env={"RUST_LOG": "debug"}, stderr=subprocess.PIPE)


def is_panic_log_line(line):
    return "thread \'main\' panicked" in line


def is_error_log_line(line):
    return "Error" in line


def wait_for_log_line(process, line_regex):
    """
    Waiting for process to output log line into stderr that matches provided regex.
    If process failed, returns error message, ignoring provided regex.
    If process succeeded, still tries to find the right log message.
    Return either that log line without processing the rest of process' stderr,
    or empty line, if correct log wasn't found.
    """

    logger.info(f"Waiting got log line to match {line_regex}")

    panic_log_line = ""
    error_log_line = ""

    while True:
        process.poll()
        # If process failed, try to find some error message and return it
        if process.returncode is not None and process.returncode != 0:
            return panic_log_line if panic_log_line else error_log_line

        # Get next log line
        line = process.stderr.readline().decode('utf-8')
        if len(line) == 0:
            # If process ended, then empty line means the end of output.
            # We should return empty line rather than endlessly waiting for correct log line.
            if process.returncode == 0:
                return line
            # Otherwise process probably temporarily not printing anything.
            # Better wait a bit.
            time.sleep(20)
            continue

        # Printing non-empty log line.
        logger.debug(line)
        # If we have been waiting for this line, return it.
        if line_regex.match(line):
            return line
        # Save suspicious log lines in case of process failure
        if is_panic_log_line(line):
            panic_log_line = line
        if is_error_log_line(line):
            error_log_line = line



def cleanup_snapshots():
    subprocess.call(["rm", "-r", "/home/ubuntu/.near/hot-data/migration-snapshot"])
    subprocess.call(["rm", "-r", "/home/ubuntu/.near/cold-data/migration-snapshot"])


def main():
    parser = argparse.ArgumentParser(description='Run state test in a loop')
    parser.add_argument('--max-depth', required=False)
    parser.add_argument('--max-count', required=False)
    parser.add_argument('--prometheus-port', required=True, type=int)
    parser.add_argument('--node-id', required=True)
    args = parser.parse_args()

    url = "http://0.0.0.0:3030"

    chain_id = get_chain_id(url)
    node_id = args.node_id

    start_http_server(args.prometheus_port)

    total_cnt = Counter(
        'cold_store_state_test_total',
        'Total number of state test runs',
        ['chain_id', 'node_id']
    )
    success_cnt = Counter(
        'cold_store_state_test_success',
        'Number of successful state test runs',
        ['chain_id', 'node_id']
    )

    genesis_height = get_genesis_height(url)
    while True:
        head = get_head(url)
        if head < genesis_height:
            logger.error(f"Head {head} is less than genesis height {genesis_height}")
            break

        random_height = random.randint(genesis_height, head)
        logger.info(f"Selected height {random_height}")

        stop_neard()
        test_process = start_test(args.max_depth, args.max_count, random_height)

        inter_log_line = wait_for_log_line(test_process, re.compile(".*check_trie.*"))
        print(inter_log_line)
        logger.info(f"Caught line {inter_log_line}")

        start_neard()

        final_log_line = wait_for_log_line(test_process, re.compile(".*Dropped a RocksDB instance. num_instances=0.*"))
        logger.info(f"Caught line {final_log_line}")

        test_process.wait()

        total_cnt.labels(chain_id=chain_id, node_id=node_id).inc()
        if test_process.returncode == 0:
            success_cnt.labels(chain_id=chain_id, node_id=node_id).inc()

        print(random_height, test_process.returncode, final_log_line)
        logger.info(random_height, test_process.returncode, final_log_line)

        cleanup_snapshots()

        # Wait between test runs
        time.sleep(120)


if __name__ == '__main__':
    main()
