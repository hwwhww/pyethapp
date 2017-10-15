from __future__ import print_function
import time
import random
import gevent
from devp2p.service import BaseService
from ethereum.slogging import get_logger
from ethereum.utils import privtoaddr, encode_hex, sha3
from ethereum.casper_utils import generate_validation_code, call_casper, check_skips, \
                                  get_timestamp, \
                                  get_casper_ct, get_dunkle_candidates, sign_block, \
                                  make_withdrawal_signature, RandaoManager

log = get_logger('validator')

BLOCK_TIME = 3

global_block_counter = 0

casper_ct = get_casper_ct()

class ValidatorService(BaseService):

    name = 'validator'
    default_config = dict(validator=dict(
        activated=False,
        privkey='',
        deposit_size=0,
        seed=''
    ))

    def __init__(self, app):
        super(ValidatorService, self).__init__(app)

        self.config = app.config

        self.chainservice = app.services.chain
        self.chain = self.chainservice.chain
        self.chain.time = lambda: int(time.time())

        self.key = self.config['validator']['privkey']
        print("*"*100)
        print(repr(self.key))
        print(len(self.key))
        self.address = privtoaddr(self.key)
        self.validation_code = generate_validation_code(self.address)
        self.validation_code_hash = sha3(self.validation_code)

        # TODO: allow configure seed?
        seed = sha3(self.key)
        self.randao = RandaoManager(seed)

        self.received_objects = {}
        self.used_parents = {}

        self.next_skip_count = 0
        self.next_skip_timestamp = 0
        self.epoch_length = self.call_casper('getEpochLength')
        self.active = False
        self.activated = self.app.config['validator']['activated']

        app.services.chain.on_new_head_cbs.append(self.on_new_head)
        self.update_activity_status()
        self.cached_head = self.chain.head_hash

    def on_new_head(self, block):
        if not self.activated:
            return
        if self.app.services.chain.is_syncing:
            return
        self.update()

    def update_activity_status(self):
        start_epoch = self.call_casper('getStartEpoch', [self.validation_code_hash])
        now_epoch = self.call_casper('getEpoch')
        end_epoch = self.call_casper('getEndEpoch', [self.validation_code_hash])
        if start_epoch <= now_epoch < end_epoch:
            self.active = True
            self.next_skip_count = 0
            self.next_skip_timestamp = get_timestamp(self.chain, self.next_skip_count)
        else:
            self.active = False

    def tick(self):
        delay = 0
        # Try to create a block
        # Conditions:
        # (i) you are an active validator,
        # (ii) you have not yet made a block with this parent
        if self.active and self.chain.head_hash not in self.used_parents:
            t = time.time()
            # Is it early enough to create the block?
            if t >= self.next_skip_timestamp and (not self.chain.head or t > self.chain.head.header.timestamp):
                # Wrong validator; in this case, just wait for the next skip count
                if not check_skips(self.chain, self.validation_code_hash, self.next_skip_count):
                    self.next_skip_count += 1
                    self.next_skip_timestamp = get_timestamp(self.chain, self.next_skip_count)
                    log.debug('Not my turn, wait',
                              next_skip_count=self.next_skip_count,
                              next_skip_timestamp=self.next_skip_timestamp,
                              now=int(time.time()))
                    return
                self.used_parents[self.chain.head_hash] = True
                blk = self.make_block()
                assert blk.timestamp >= self.next_skip_timestamp
                if self.chainservice.add_mined_block(blk):
                    self.received_objects[blk.hash] = True
                    log.debug('0x%s made and added block %d (%s) to chain' % (encode_hex(self.address[:8]), blk.header.number, encode_hex(blk.header.hash[:8])))
                else:
                    log.debug('0x%s failed to make and add block %d (%s) to chain' % (encode_hex(self.address[:8]), blk.header.number, encode_hex(blk.header.hash[:8])))
                self.update()
            else:
                delay = max(self.next_skip_timestamp - t, 0)
        # Sometimes we received blocks too early or out of order;
        # run an occasional loop that processes these
        if random.random() < 0.02:
            self.chain.process_time_queue()
            self.chain.process_parent_queue()
            self.update()
        return delay

    def make_block(self):
        pre_dunkle_count = self.call_casper('getTotalDunklesIncluded')
        dunkle_txs = get_dunkle_candidates(self.chain, self.chain.state)
        blk = self.chainservice.head_candidate
        randao = self.randao.get_parent(self.call_casper('getRandao', [self.validation_code_hash]))
        blk = sign_block(blk, self.key, randao, self.validation_code_hash, self.next_skip_count)
        # Make sure it's valid
        global global_block_counter
        global_block_counter += 1
        for dtx in dunkle_txs:
            assert dtx in blk.transactions, (dtx, blk.transactions)
            log.debug('made block with timestamp %d and %d dunkles' % (blk.timestamp, len(dunkle_txs)))
        return blk

    def update(self):
        if self.cached_head == self.chain.head_hash:
            return
        self.cached_head = self.chain.head_hash
        if self.chain.state.block_number % self.epoch_length == 0:
            self.update_activity_status()
        if self.active:
            self.next_skip_count = 0
            self.next_skip_timestamp = get_timestamp(self.chain, self.next_skip_count)
        log.debug('Head changed: %s, will attempt creating a block at %d' % (encode_hex(self.chain.head_hash), self.next_skip_timestamp))

    def withdraw(self, gasprice=20 * 10**9):
        sigdata = make_withdrawal_signature(self.key)
        txdata = casper_ct.encode('startWithdrawal', [self.validation_code_hash, sigdata])
        tx = Transaction(self.chain.state.get_nonce(self.address), gasprice, 650000, self.chain.config['CASPER_ADDR'], 0, txdata).sign(self.key)
        self.chainservice.add_transaction(tx, force=True)

    def deposit(self, gasprice=20 * 10**9):
        assert value * 10**18 >= self.chain.state.get_balance(self.address) + gasprice * 1000000
        tx = Transaction(self.chain.state.get_nonce(self.address) * 10**18, gasprice, 1000000,
                         casper_config['CASPER_ADDR'], value * 10**18,
                         ct.encode('deposit', [self.validation_code, self.randao.get(9999)]))

    def call_casper(self, fun, args=[]):
        return call_casper(self.chain.state, fun, args)

    def _run(self):
        while True:
            if self.activated:
                delay = self.tick()
                gevent.sleep(delay)

    def stop(self):
        super(ValidatorService, self).stop()

