from __future__ import division
from builtins import object
from past.utils import old_div
import time
import gevent
import gipc
import random
from devp2p.service import BaseService
from ethereum.pow.ethpow import mine, TT64M1
from ethereum.slogging import get_logger
from ethereum.utils import encode_hex
log = get_logger('pow')
log_sub = get_logger('pow.subprocess')


class Miner(gevent.Greenlet):

    rounds = 100
    max_elapsed = 1.

    def __init__(self, mining_hash, block_number, difficulty, nonce_callback,
                 hashrate_callback, cpu_pct=100):
        self.mining_hash = mining_hash
        self.block_number = block_number
        self.difficulty = difficulty
        self.nonce_callback = nonce_callback
        self.hashrate_callback = hashrate_callback
        self.cpu_pct = cpu_pct
        self.is_stopped = False
        super(Miner, self).__init__()

    def _run(self):
        nonce = random.randint(0, TT64M1)
        while not self.is_stopped:
            log_sub.trace('starting mining round')
            st = time.time()
            bin_nonce, mixhash = mine(self.block_number, self.difficulty, self.mining_hash,
                                      start_nonce=nonce, rounds=self.rounds)
            elapsed = time.time() - st
            if bin_nonce:
                log_sub.info('nonce found')
                self.nonce_callback(bin_nonce, mixhash, self.mining_hash)
                break
            delay = elapsed * (1 - old_div(self.cpu_pct, 100.))
            hashrate = int(self.rounds // (elapsed + delay))
            self.hashrate_callback(hashrate)
            log_sub.trace('sleeping', delay=delay, elapsed=elapsed, rounds=self.rounds)
            gevent.sleep(delay + 0.001)
            nonce += self.rounds
            # adjust
            adjust = old_div(elapsed, self.max_elapsed)
            self.rounds = int(old_div(self.rounds, adjust))

        log_sub.debug('mining task finished', is_stopped=self.is_stopped)

    def stop(self):
        self.is_stopped = True
        self.join()


class PoWWorker(object):

    """
    communicates with the parent process using: tuple(str_cmd, dict_kargs)
    """

    def __init__(self, cpipe, cpu_pct):
        self.cpipe = cpipe
        self.miner = None
        self.cpu_pct = cpu_pct

    def send_found_nonce(self, bin_nonce, mixhash, mining_hash):
        log_sub.info('sending nonce')
        self.cpipe.put(('found_nonce', dict(bin_nonce=bin_nonce, mixhash=mixhash,
                                            mining_hash=mining_hash)))

    def send_hashrate(self, hashrate):
        log_sub.trace('sending hashrate')
        self.cpipe.put(('hashrate', dict(hashrate=hashrate)))

    def recv_set_cpu_pct(self, cpu_pct):
        self.cpu_pct = max(0, min(100, cpu_pct))
        if self.miner:
            self.miner.cpu_pct = self.cpu_pct

    def recv_mine(self, mining_hash, block_number, difficulty):
        "restarts the miner"
        log_sub.debug('received new mining task', difficulty=difficulty)
        assert isinstance(block_number, int)
        if self.miner:
            self.miner.stop()
        self.miner = Miner(mining_hash, block_number, difficulty, self.send_found_nonce,
                           self.send_hashrate, self.cpu_pct)
        self.miner.start()

    def run(self):
        while True:
            cmd, kargs = self.cpipe.get()
            assert isinstance(kargs, dict)
            getattr(self, 'recv_' + cmd)(**kargs)


def powworker_process(cpipe, cpu_pct):
    "entry point in forked sub processes, setup env"
    gevent.get_hub().SYSTEM_ERROR = BaseException  # stop on any exception
    PoWWorker(cpipe, cpu_pct).run()


# parent process defined below ##############################################3

class PoWService(BaseService):

    name = 'pow'
    default_config = dict(pow=dict(
        activated=False,
        cpu_pct=100,
        coinbase_hex=None,
        mine_empty_blocks=True
    ))

    def __init__(self, app):
        super(PoWService, self).__init__(app)
        cpu_pct = self.app.config['pow']['cpu_pct']
        self.cpipe, self.ppipe = gipc.pipe(duplex=True)
        self.worker_process = gipc.start_process(
            target=powworker_process, args=(self.cpipe, cpu_pct))
        self.chain = app.services.chain
        self.chain.on_new_head_cbs.append(self.mine_head_candidate)
        self.hashrate = 0

    @property
    def active(self):
        return self.app.config['pow']['activated']

    def mine_head_candidate(self, _=None):
        hc = self.chain.head_candidate
        if not self.active or self.chain.is_syncing:
            return
        elif (hc.transaction_count == 0 and
              not self.app.config['pow']['mine_empty_blocks']):
            return

        log.debug('mining', difficulty=hc.difficulty)
        self.ppipe.put(('mine', dict(mining_hash=hc.mining_hash,
                                     block_number=hc.number,
                                     difficulty=hc.difficulty)))

    def recv_hashrate(self, hashrate):
        log.trace('hashrate updated', hashrate=hashrate)
        self.hashrate = hashrate

    def recv_found_nonce(self, bin_nonce, mixhash, mining_hash):
        log.info('nonce found', mining_hash=mining_hash.encode('hex'))
        block = self.chain.head_candidate
        if block.mining_hash != mining_hash:
            log.warn('mining_hash does not match')
            return False
        block.header.mixhash = mixhash
        block.header.nonce = bin_nonce
        if self.chain.add_mined_block(block):
            log.debug('mined block %d (%s) added to chain' % (
                block.number, encode_hex(block.hash[:8])))
            return True
        else:
            log.debug('failed to add mined block %d (%s) to chain' % (
                block.number, encode_hex(block.hash[:8])))
            return False

    def _run(self):
        self.mine_head_candidate()
        while True:
            cmd, kargs = self.ppipe.get()
            assert isinstance(kargs, dict)
            getattr(self, 'recv_' + cmd)(**kargs)

    def stop(self):
        self.worker_process.terminate()
        self.worker_process.join()
        super(PoWService, self).stop()
