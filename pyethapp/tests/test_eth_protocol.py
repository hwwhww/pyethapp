from __future__ import print_function
from pyethapp.eth_protocol import ETHProtocol, TransientBlockBody
from devp2p.service import WiredService
from devp2p.protocol import BaseProtocol
from devp2p.app import BaseApp
from ethereum.tools import tester
import rlp


class PeerMock(object):
    packets = []
    config = dict()

    def send_packet(self, packet):
        self.packets.append(packet)


def setup():
    peer = PeerMock()
    proto = ETHProtocol(peer, WiredService(BaseApp()))
    proto.service.app.config['eth'] = dict(network_id=1337)
    chain = tester.Chain()
    cb_data = []

    def cb(proto, **data):
        cb_data.append((proto, data))
    return peer, proto, chain, cb_data, cb


def test_basics():
    peer, proto, chain, cb_data, cb = setup()

    assert isinstance(proto, BaseProtocol)

    d = dict()
    d[proto] = 1
    assert proto in d
    assert d[proto] == 1
    assert not proto
    proto.start()
    assert proto


def test_status():
    peer, proto, chain, cb_data, cb = setup()
    genesis = head = chain.chain.get_descendants(chain.chain.get_block_by_number(0))[-1]

    # test status
    proto.send_status(
        chain_difficulty=chain.chain.get_score(head),
        chain_head_hash=head.hash,
        genesis_hash=genesis.hash
    )
    packet = peer.packets.pop()
    proto.receive_status_callbacks.append(cb)
    proto._receive_status(packet)

    _p, _d = cb_data.pop()
    assert _p == proto
    assert isinstance(_d, dict)
    assert _d['chain_difficulty'] == chain.chain.get_score(head)
    print(_d)
    assert _d['chain_head_hash'] == head.hash
    assert _d['genesis_hash'] == genesis.hash
    assert 'eth_version' in _d
    assert 'network_id' in _d


def test_blocks():
    peer, proto, chain, cb_data, cb = setup()

    # test blocks
    chain.mine(number_of_blocks=2)
    assert chain.block.number == 3
    # monkey patch to make "blocks" attribute available
    chain.blocks = chain.chain.get_descendants(chain.chain.get_block_by_number(0))
    proto.send_blockbodies(*chain.blocks)
    packet = peer.packets.pop()
    assert len(rlp.decode(packet.payload)) == 3

    def list_cb(proto, blocks):  # different cb, as we expect a list of blocks
        cb_data.append((proto, blocks))

    proto.receive_blockbodies_callbacks.append(list_cb)
    proto._receive_blockbodies(packet)

    _p, blocks = cb_data.pop()
    assert isinstance(blocks, tuple)
    for block in blocks:
        assert isinstance(block, TransientBlockBody)
        assert isinstance(block.transactions, tuple)
        assert isinstance(block.uncles, tuple)
        # assert that transactions and uncles have not been decoded
        assert len(block.transactions) == 0
        assert len(block.uncles) == 0

    # newblock
    approximate_difficulty = chain.blocks[-1].difficulty * 3
    proto.send_newblock(block=chain.blocks[-1], chain_difficulty=approximate_difficulty)
    packet = peer.packets.pop()
    proto.receive_newblock_callbacks.append(cb)
    proto._receive_newblock(packet)

    _p, _d = cb_data.pop()
    assert 'block' in _d
    assert 'chain_difficulty' in _d
    assert _d['chain_difficulty'] == approximate_difficulty
    assert _d['block'].header == chain.blocks[-1].header
    assert isinstance(_d['block'].transactions, tuple)
    assert isinstance(_d['block'].uncles, tuple)
    # assert that transactions and uncles have not been decoded
    assert len(_d['block'].transactions) == 0
    assert len(_d['block'].uncles) == 0
