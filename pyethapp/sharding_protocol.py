import time
import rlp
import gevent
from devp2p.protocol import BaseProtocol, SubProtocolError
from ethereum.transactions import Transaction
from ethereum.utils import hash32
from ethereum import slogging
from sharding.collation import Collation, CollationHeader

log = slogging.get_logger('protocol.eth')


class TransientCollationBody(rlp.Serializable):
    fields = [
        ('transactions', rlp.sedes.CountableList(Transaction))
    ]


class TransientCollation(rlp.Serializable):

    """A partially decoded, unvalidated collation."""

    fields = [
        ('header', CollationHeader),
        ('transactions', rlp.sedes.CountableList(Transaction)),
    ]

    @classmethod
    def init_from_rlp(cls, collation_data, newcollation_timestamp=0):
        header = CollationHeader.deserialize(collation_data[0])
        transactions = rlp.sedes.CountableList(Transaction).deserialize(collation_data[1])
        return cls(header, transactions, newcollation_timestamp)

    def __init__(self, header, transactions, newcollation_timestamp=0):
        self.newcollation_timestamp = newcollation_timestamp
        self.header = header
        self.transactions = transactions

    def to_collation(self):
        """Convert the transient collation to a :class:`sharding.collation.Collation`"""
        return Collation(self.header, transactions=self.transactions)

    @property
    def hex_hash(self):
        return self.header.hex_hash

    def __repr__(self):
        return '<TransientCollation(#%d %s)>' % (self.header.number, self.header.hash.encode('hex')[:8])


class ShardingProtocolError(SubProtocolError):
    pass


class ShardingProtocol(BaseProtocol):

    """
    Simple Sharding Wire Protocol
    """
    protocol_id = 300
    network_id = 0   # TODO
    max_cmd_id = 15  # TODO
    name = 'shd'
    version = 1

    max_getcollations_count = 128
    max_getcollationheaders_count = 192

    def __init__(self, peer, service):
        # required by P2PProtocol
        self.config = peer.config
        BaseProtocol.__init__(self, peer, service)

    class status(BaseProtocol.command):

        """
        Status [+0x00: P, protocolVersion: P, networkId: P, shardId: P, score: P, bestHash: B_32,]
        Inform a peer of its current ethereum shard state. This message should be sent after
        the initial handshake and prior to any ethereum shard related messages.

        protocolVersion: the current protocol version. The first version is 1.
        networkId: the network id of this shard
        shardId: the id of shard chain
        shard_score is the score of the head collation
        shard_head_hash is the hash of the best known collation
        """
        cmd_id = 0
        sent = False

        structure = [
            ('eth_version', rlp.sedes.big_endian_int),
            ('network_id', rlp.sedes.big_endian_int),
            ('shardId', rlp.sedes.big_endian_int),     # TODO: shardId -> shard_id
            ('shard_score', rlp.sedes.big_endian_int),  # score
            ('shard_head_hash', rlp.sedes.binary)]      # best_hash

        def create(self, proto, shardId, shard_score, shard_head_hash):
            self.sent = True
            # TODO: config network_id?
            network_id = proto.service.app.config['shd'].get('network_id', proto.network_id)
            return [proto.version, network_id, shardId, shard_score, shard_head_hash]

    class newcollationhashes(BaseProtocol.command):

        """
        NewCollationHashes [+0x01: P, [hash_0: B_32, number_0: P], [hash_1: B_32, number_1: P], ...]
        Specify one or more new collations which have appeared on the network.
        To be maximally helpful, nodes should inform peers of all collations that they
        may not be aware of. Including hashes that the sending peer could reasonably be
        considered to know (due to the fact they were previously informed of because that
        node has itself advertised knowledge of the hashes through NewCollationHashes)
        is considered Bad Form, and may reduce the reputation of the sending node.
        Including hashes that the sending node later refuses to honour with a proceeding
        GetCollationHeaders message is considered Bad Form, and may reduce the reputation
        of the sending node.
        """
        cmd_id = 1

        class Data(rlp.Serializable):
            fields = [
                ('hash', hash32),
                ('number', rlp.sedes.big_endian_int)
            ]
        structure = rlp.sedes.CountableList(Data)

    # TODO: redundant class, as same as EthProtocol.transactions
    class transactions(BaseProtocol.command):
        """
        Specify (a) transaction(s) that the peer should make sure is included on its transaction
        queue. The items in the list (following the first item 0x12) are transactions in the
        format described in the main Ethereum specification. Nodes must not resend the same
        transaction to a peer in the same session. This packet must contain at least one (new)
        transaction.
        """
        cmd_id = 2
        structure = rlp.sedes.CountableList(Transaction)

        # todo: bloomfilter: so we don't send tx to the originating peer

        @classmethod
        def decode_payload(cls, rlp_data):
            # convert to dict
            txs = []
            for i, tx in enumerate(rlp.decode_lazy(rlp_data)):
                txs.append(Transaction.deserialize(tx))
                if not i % 10:
                    gevent.sleep(0.0001)
            return txs

    class getcollationheaders(BaseProtocol.command):

        """
        GetCollationHeaders [+0x03: P, collation: { P , B_32 }, maxHeaders:
        P, skip: P, reverse: P in { 0 , 1 }]
        Require peer to return a CollationHeaders message.
        Reply must contain a number of collation headers, of rising number
        when reverse is 0, falling when 1, skip collation apart, beginning at
        collation collation (denoted by hash) in the canonical chain, and with
        at most maxHeaders items.
        """
        cmd_id = 3

        structure = [
            ('collation', rlp.sedes.binary),
            ('amount', rlp.sedes.big_endian_int),
            ('skip', rlp.sedes.big_endian_int),
            ('reverse', rlp.sedes.big_endian_int)
        ]

        def create(self, proto, collation, amount, skip=0, reverse=1):
            return [collation, amount, skip, reverse]

        def receive(self, proto, data):
            if len(data['collation']) == 32:
                hash_or_number = (data['collation'], 0)
            else:
                raise Exception('invalid hash_or_number value')
            data['hash_or_number'] = hash_or_number

            for cb in self.receive_callbacks:
                cb(proto, **data)

    class collationheaders(BaseProtocol.command):

        """
        CollationHeaders [+0x04, collationHeader_0, collationHeader_1, ...]
        Reply to GetCollationHeaders.
        The items in the list (following the message ID) are collation headers
        in the format described in the Sharding specification, previously
        asked for in a GetCollationHeaders message.
        This may validly contain no collation headers if no collation headers
        were able to be returned for the GetCollationHeaders query.
        """
        cmd_id = 4
        structure = rlp.sedes.CountableList(CollationHeader)

    class getcollationbodies(BaseProtocol.command):

        """
        GetCollationBodies [+0x05, hash_0: B_32, hash_1: B_32, ...]
        Require peer to return a CollationBodies message.
        Specify the set of collations that we're interested in with the hashes.
        """
        cmd_id = 5
        structure = rlp.sedes.CountableList(rlp.sedes.binary)

    class collationbodies(BaseProtocol.command):

        """
        CollationBodies [+0x06, [transactions_0] , ...]
        Reply to GetCollationBodies.
        The items in the list (following the message ID) are some of the
        collations, minus the header, in the format described in the
        Sharding specification, previously asked for in a GetCollationBodies
        message. This may validly contain no items if no collation is able to
        be returned for the GetCollationBodies query.
        """
        cmd_id = 6
        structure = rlp.sedes.CountableList(TransientCollationBody)

        def create(self, proto, *bodies):
            if len(bodies) == 0:
                return []
            if isinstance(bodies[0], Collation):
                bodies = [TransientCollationBody(b.transactions) for b in bodies]
            return bodies

    class newcollation(BaseProtocol.command):

        """
        NewCollation [+0x07, [collationHeader, transactionList], score]
        Specify a single collation that the peer should know about.
        The composite item in the list (following the message ID) is a
        collation in the format described in the main Sharding specification.
        """
        cmd_id = 7
        structure = [('collation', Collation), ('score', rlp.sedes.big_endian_int)]

        # todo: bloomfilter: so we don't send block to the originating peer

        @classmethod
        def decode_payload(cls, rlp_data):
            # convert to dict
            # print rlp_data.encode('hex')
            ll = rlp.decode_lazy(rlp_data)
            assert len(ll) == 2
            transient_collation = TransientCollation.init_from_rlp(ll[0], time.time())
            score = rlp.sedes.big_endian_int.deserialize(ll[1])
            data = [transient_collation, score]
            return dict((cls.structure[i][0], v) for i, v in enumerate(data))
