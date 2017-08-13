import pytest
from pyethapp.sharding_protocol import ShardingProtocol, TransientCollationBody, TransientCollation
from devp2p.service import WiredService
from devp2p.protocol import BaseProtocol
from devp2p.app import BaseApp
# from ethereum.tools import tester
import rlp
from ethereum.transaction_queue import TransactionQueue
from ethereum.transactions import Transaction
from sharding.collation import Collation, CollationHeader
from sharding.tools import tester


class PeerMock(object):
    packets = []
    config = dict()

    def send_packet(self, packet):
        self.packets.append(packet)


def setup():
    peer = PeerMock()
    proto = ShardingProtocol(peer, WiredService(BaseApp()))
    proto.service.app.config['shd'] = dict(network_id=1337)

    shardId = 1

    t = tester.Chain(env='sharding')
    t.mine(5)
    t.add_test_shard(shardId)

    cb_data = []

    def cb(proto, **data):
        cb_data.append((proto, data))
    return peer, proto, t, cb_data, cb, shardId


def test_basics():
    peer, proto, chain, cb_data, cb, shardId = setup()

    assert isinstance(proto, BaseProtocol)

    d = dict()
    d[proto] = 1
    assert proto in d
    assert d[proto] == 1
    assert not proto
    proto.start()
    assert proto


def test_status():
    peer, proto, chain, cb_data, cb, shardId = setup()
    shard = chain.chain.shards[shardId]

    # test status
    proto.send_status(
        shardId=shardId,
        shard_score=0,
        shard_head_hash=shard.head.hash
    )
    packet = peer.packets.pop()
    proto.receive_status_callbacks.append(cb)
    proto._receive_status(packet)

    _p, _d = cb_data.pop()
    assert _p == proto
    assert isinstance(_d, dict)
    assert _d['shardId'] == shardId
    assert _d['shard_score'] == shard.get_score(shard.head)
    print _d
    assert _d['shard_head_hash'] == shard.head.hash
    assert 'eth_version' in _d
    assert 'network_id' in _d


def test_collations():
    peer, proto, chain, cb_data, cb, shardId = setup()
    shard = chain.chain.shards[shardId]

    # test collationbodies
    # empty collationbodies
    proto.send_collationbodies(*[])
    packet = peer.packets.pop()
    assert len(rlp.decode(packet.payload)) == 0

    # generate one collation
    tx1 = chain.generate_shard_tx(sender=tester.k2, to=tester.a4, value=1)
    tx2 = chain.generate_shard_tx(sender=tester.k3, to=tester.a5, value=1)
    txqueue = TransactionQueue()
    txqueue.add_transaction(tx1)
    txqueue.add_transaction(tx2)
    chain.collations = [chain.generate_collation(shardId=1, coinbase=tester.a1, key=tester.k1, txqueue=txqueue)]

    proto.send_collationbodies(*chain.collations)
    packet = peer.packets.pop()
    assert len(rlp.decode(packet.payload)) == 1

    def list_cb(proto, collations):  # different cb, as we expect a list of collations
        cb_data.append((proto, collations))

    proto.receive_collationbodies_callbacks.append(list_cb)
    proto._receive_collationbodies(packet)

    _p, collations = cb_data.pop()
    assert isinstance(collations, tuple)
    for collation in collations:
        assert isinstance(collation, TransientCollationBody)
        assert isinstance(collation.transactions, tuple)
        # assert that transactions have not been decoded
        assert len(collation.transactions) == 2

    # test transactions
    tx_list = [tx1, tx2]
    proto.send_transactions(*tx_list)
    packet = peer.packets.pop()
    proto.receive_transactions_callbacks.append(list_cb)
    proto._receive_transactions(packet)

    _p, txs = cb_data.pop()
    for tx in txs:
        assert isinstance(tx, Transaction)
        assert len(tx.to) > 0

    # test newcollation
    approximate_score = shard.get_score(chain.collations[-1]) + 1
    proto.send_newcollation(collation=chain.collations[-1], score=approximate_score)
    packet = peer.packets.pop()
    proto.receive_newcollation_callbacks.append(cb)
    proto._receive_newcollation(packet)

    _p, _d = cb_data.pop()
    assert 'collation' in _d
    assert 'score' in _d
    assert _d['score'] == approximate_score
    assert _d['collation'].header == chain.collations[-1].header
    assert isinstance(_d['collation'], TransientCollation)
    assert isinstance(_d['collation'].to_collation(), Collation)
    assert isinstance(_d['collation'].transactions, tuple)
    assert _d['collation'].hex_hash == chain.collations[-1].hex_hash
    # assert that transactions have not been decoded
    assert len(_d['collation'].transactions) == 2

    # test getcollationheader
    collhash = chain.collations[-1].header.hash
    amount = 100
    proto.send_getcollationheaders(collhash, amount)
    packet = peer.packets.pop()
    proto.receive_getcollationheaders_callbacks.append(cb)
    proto._receive_getcollationheaders(packet)

    _p, _d = cb_data.pop()
    assert _p == proto
    assert isinstance(_d, dict)
    assert _d['amount'] == amount

    # test getcollationheader - exception
    collhash = ''
    amount = 100
    proto.send_getcollationheaders(collhash, amount)
    packet = peer.packets.pop()
    proto.receive_getcollationheaders_callbacks.append(cb)

    with pytest.raises(Exception):
        proto._receive_getcollationheaders(packet)

    # test collationheader
    proto.send_collationheaders(*[collation.header for collation in chain.collations])
    packet = peer.packets.pop()
    assert len(rlp.decode(packet.payload)) == 1

    def list_header_cb(proto, collation_headers):  # different cb, as we expect a list of CollationHeader
        cb_data.append((proto, collation_headers))

    proto.receive_collationheaders_callbacks.append(list_header_cb)
    proto._receive_collationheaders(packet)

    _p, collation_headers = cb_data.pop()
    assert isinstance(collation_headers, tuple)
    for header in collation_headers:
        assert isinstance(header, CollationHeader)
