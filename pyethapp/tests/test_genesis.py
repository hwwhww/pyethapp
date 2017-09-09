from pprint import pprint
import pytest
from ethereum.db import DB
from ethereum.config import Env, default_config
from ethereum.genesis_helpers import mk_genesis_block
from ethereum.state import State
from pyethapp.utils import merge_dict
from pyethapp.config import update_config_from_genesis_json
import pyethapp.config as konfig
from pyethapp.profiles import PROFILES


def test_genesis_config():
    "test setting genesis alloc using the config"
    alloc = {'1' * 40: {'wei': 1},  # wei
             '2' * 40: {'balance': 2},  # balance
             '3' * 20: {'balance': 3},  # 20 bytes
             }
    config = dict(eth=dict(genesis=dict(alloc=alloc)))
    konfig.update_config_with_defaults(config, {'eth': {'block': default_config}})

    # Load genesis config
    update_config_from_genesis_json(config, config['eth']['genesis'])

    bc = config['eth']['block']
    pprint(bc)
    env = Env(DB(), bc)

    genesis = mk_genesis_block(env)
    state = State(genesis.state_root, env)
    for address, value_dict in list(alloc.items()):
        value = list(value_dict.values())[0]
        assert state.get_balance(address) == value


@pytest.mark.parametrize('profile', list(PROFILES.keys()))
def test_profile(profile):
    config = dict(eth=dict())

    konfig.update_config_with_defaults(config, {'eth': {'block': default_config}})

    # Set config values based on profile selection
    merge_dict(config, PROFILES[profile])

    # Load genesis config
    update_config_from_genesis_json(config, config['eth']['genesis'])

    bc = config['eth']['block']
    pprint(bc)
    env = Env(DB(), bc)

    genesis = mk_genesis_block(env)
    assert genesis.hash.encode('hex') == config['eth']['genesis_hash']
