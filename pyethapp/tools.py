from __future__ import print_function
import os
import sys
import time
import json
import yaml

from ethereum import utils
from ethereum.state_transition import apply_const_message
from ethereum.casper_utils import RandaoManager, generate_validation_code, make_casper_genesis, casper_config, call_casper
from devp2p.crypto import privtopub

def generate_data_dirs(num_participants, prefix='v'):
    privkeys = [utils.sha3(str(i)) for i in range(num_participants)]
    addrs = [utils.privtoaddr(k) for k in privkeys]
    genesis = generate_genesis(None, num_participants)

    for i in range(num_participants):
        privkey = privkeys[i]
        addr = addrs[i]
        port = 40000+i
        jsonrpc_port = 4000+i
        deposit_size = 500 + 500*i

        bootstrap_nodes = range(num_participants)
        bootstrap_nodes.remove(i)
        bootstrap_nodes = ["enode://%s@0.0.0.0:%d" % (utils.encode_hex(privtopub(privkeys[n])), 40000+n) for n in bootstrap_nodes]

        dir = prefix + str(i)
        try:
            os.stat(dir)
        except:
            os.mkdir(dir)
        genesis_path = dir + '/genesis.json'
        config_path = dir + '/config.yaml'

        config = {
            "node": {
                "privkey_hex": utils.encode_hex(privkey)
            },
            "validator": {
                "privkey_hex": utils.encode_hex(privkey),
                "deposit_size": deposit_size
            },
            "eth": {
                "genesis": genesis_path,
                "network_id": 42
            },
            "p2p": {
                "num_peers": num_participants-1,
                "listen_host": '0.0.0.0',
                "listen_port": port
            },
            "discovery": {
                "listen_host": '0.0.0.0',
                "listen_port": port,
                "bootstrap_nodes": bootstrap_nodes
            },
            "jsonrpc": {
                "listen_host": '0.0.0.0',
                "listen_port": jsonrpc_port
            }
        }

        with open(genesis_path, 'w') as f:
            json.dump(genesis, f, sort_keys=False, indent=4, separators=(',', ': '))
        print("genesis for validator %d generated" % i)

        with open(config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, indent=4)
        print("config for validator %d generated" % i)


def generate_genesis(path=None, num_participants=1):
    privkeys = [utils.sha3(str(i)) for i in range(num_participants)]
    addrs = [utils.privtoaddr(k) for k in privkeys]
    deposit_sizes = [i * 500 + 500 for i in range(num_participants)]
    randaos = [RandaoManager(utils.sha3(k)) for k in privkeys]

    validators = [(generate_validation_code(a), ds * 10**18, r.get(9999), a) for a, ds, r in zip(addrs, deposit_sizes, randaos)]
    s = make_casper_genesis(validators=validators,
                            alloc={a: {'balance': 10**18} for a in addrs},
                            timestamp=int(time.time()),
                            epoch_length=100)
    genesis_hash = apply_const_message(s,
                                       sender=casper_config['METROPOLIS_ENTRY_POINT'],
                                       to=casper_config['METROPOLIS_BLOCKHASH_STORE'],
                                       data=utils.encode_int32(0))
    genesis_number = call_casper(s, 'getBlockNumber')
    print('genesis block hash: %s' % utils.encode_hex(genesis_hash))
    print('genesis block number: %d' % genesis_number)
    print('%d validators: %r' % (num_participants, [utils.encode_hex(a) for a in addrs]))

    snapshot = s.to_snapshot()
    header = s.prev_headers[0]
    genesis = {
        "nonce": "0x" + utils.encode_hex(header.nonce),
        "difficulty": utils.int_to_hex(header.difficulty),
        "mixhash": "0x" + utils.encode_hex(header.mixhash),
        "coinbase": "0x" + utils.encode_hex(header.coinbase),
        "timestamp": utils.int_to_hex(header.timestamp),
        "parentHash": "0x" + utils.encode_hex(header.prevhash),
        "extraData": "0x" + utils.encode_hex(header.extra_data),
        "gasLimit": utils.int_to_hex(header.gas_limit),
        "alloc": snapshot["alloc"]
    }

    if path:
        with open(path, 'w') as f:
            json.dump(genesis, f, sort_keys=False, indent=4, separators=(',', ': '))
        print('casper genesis generated')
    else:
        return genesis


def usage():
    print("usage:")
    print("python pyethapp/tools.py genesis pyethapp/genesisdata/genesis_metropolis.json 3")
    print("python pyethapp/tools.py datadir 3")

if __name__ == "__main__":
    if len(sys.argv) == 1:
        usage()
        sys.exit(0)

    if sys.argv[1] == "genesis":
        generate_genesis(sys.argv[2], int(sys.argv[3]))
    elif sys.argv[1] == "datadir":
        generate_data_dirs(int(sys.argv[2]))
    else:
        print("unknown command: %s" % sys.argv[1])
        usage()
        sys.exit(1)
