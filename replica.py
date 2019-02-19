from time import sleep, time
from itertools import chain, islice
from contextlib import contextmanager
from random import random

import Pyro4
from models import Update
from threading import Lock, Thread
from utils import generate_id, find_random_peers, ignore_disconnects, apply_updates, sort_buffer
import vector_clock as vc


PRIMARY_ID = "_"


class Replica:
    def __init__(self):
        self.id = generate_id(5)
        self.ns = Pyro4.locateNS()
        # state and updates
        self.db = {}
        self.log = []
        self.ts = vc.create() # timestamp of state
        self.executed = set()
        self._lock = Lock()
        self.buffer = []
        # gossip
        self.busy = False
        self.sync_period = 2
        self.sync_ts = vc.create() # timestamp of replica
        self.has_new_gossip = False
        self.need_reconstruct = False
        self.is_online = True
        self.forced = {}

    @property
    @contextmanager
    def lock(self):
        self.busy = True
        with self._lock:
            yield self._lock
            self.busy = False

    def peers(self):
        with self.ns:
            peers = find_random_peers(self.ns, self.id, "replica")
        for peer in peers:
            peer = Pyro4.Proxy(peer)
            with ignore_disconnects():
                if peer.status() != 'online':
                    peer._pyroRelease()
                    continue
            yield peer

    def gossip(self):
        n = 0
        while True:
            n += 1
            with self.lock:
                self.is_online = random() <= 0.75
                if self.has_new_gossip:
                    self.need_reconstruct = True
                    self.has_new_gossip = False
                    self.apply_updates()
                    n = 0
                # relax 5 rounds and apply a global order to the updates
                elif n >= 5 and self.need_reconstruct and not self.buffer:
                    self.need_reconstruct = False
                    self.reconstruct()
                    n = 0
            sleep(self.sync_period)
            logs = []
            for peer in islice(self.peers(), 5):
                with ignore_disconnects():
                    t = peer.get_timestamp()
                    with self.lock:
                        if t == self.sync_ts:
                            continue
                        # check if we need to go back past the checkpoint
                        log = (
                            self.buffer if vc.greater_than(t, self.ts) else
                            chain(self.log, self.buffer)
                            )
                        # get all events which are concurrent or greater than
                        events = [u for u in log if vc.compare(u.ts, t) >= 0]
                        logs.append((peer, self.sync_ts, events))
            # now send events
            for peer, ts, log in logs:
                with peer, ignore_disconnects():
                    if log:
                        peer.sync(log, ts)

    def reconstruct(self):
        self.ts = {}
        self.db.clear()
        self.executed.clear()
        self.log, self.buffer = [], self.log
        self.apply_updates()

    def apply_updates(self):
        sort_buffer(self.buffer)
        self.ts, self.buffer = apply_updates(self.ts, self.db, self.executed,
                                             self.log, self.buffer)

    def forced_update(self, u, patience=5):
        # find all the peers
        with self.ns:
            uris = find_random_peers(self.ns, self.id, "replica")
        told = set()
        while True:
            for uri in set(uris) - told:
                peer = Pyro4.Proxy(uri)
                with ignore_disconnects(), peer:
                    if peer.status() != 'offline':
                        peer.sync_forced(u)
                        told.add(uri)
            if len(told) == len(uris):
                break
            patience -= 1
            if patience == 0:
                raise RuntimeError("cannot apply forced update!")
            # give time to recover
            sleep(self.sync_period / 2)
        # ack
        for uri in told:
            with Pyro4.Proxy(uri) as peer:
                peer.commit_forced(u.id)

    # exposed methods

    @Pyro4.expose
    def sync_forced(self, u):
        with self.lock:
            u = Update(*u)
            self.forced[u.id] = u

    @Pyro4.expose
    def commit_forced(self, id):
        with self.lock:
            self.buffer.append(self.forced[id])
            self.sync_ts[PRIMARY_ID] = self.forced[id].ts[PRIMARY_ID]
            self.apply_updates()
            del self.forced[id]
            # delete stale logs
            for uid in list(self.forced):
                if self.forced[uid].ts[PRIMARY_ID] < self.ts[PRIMARY_ID]:
                    del self.forced[uid]
            self.need_reconstruct = True

    @Pyro4.expose
    def status(self):
        if not self.is_online:
            return 'offline'
        if self.busy or random() <= 0.25:
            return 'overloaded'
        return 'online'

    @Pyro4.expose
    def sync(self, log, ts):
        with self.lock:
            self.sync_ts = vc.merge(self.sync_ts, ts)
            self.buffer.extend(Update(*u) for u in log)
            self.has_new_gossip = True

    @Pyro4.expose
    def get_log(self):
        return self.ts, self.log

    @Pyro4.expose
    def get_timestamp(self):
        return self.sync_ts

    @Pyro4.expose
    def get(self, user_id, ts, guarantee=20):
        while True:
            with self.lock:
                # can respond
                if vc.geq(self.ts, ts):
                    return self.db.get(user_id, {}), self.ts
                if guarantee == 0:
                    raise RuntimeError("Cannot retrieve value!")
            sleep(self.sync_period)
            guarantee -= 1

    @Pyro4.expose
    def update(self, update, ts, forced=False):
        with self.lock:
            prev = ts.copy()
            id = PRIMARY_ID if forced else self.id
            new_sync_ts = vc.increment(self.sync_ts, id)
            ts[id] = new_sync_ts[id]

            u = Update(generate_id(5), *update, prev, ts, time())

            if forced:
                self.forced_update(u)
            # commit changes and apply update immediately if possible
            self.sync_ts = new_sync_ts
            self.need_reconstruct = True
            self.buffer.append(u)
            self.apply_updates()
            return ts


if __name__ == '__main__':
    r = Replica()
    with Pyro4.Daemon() as daemon:
        uri = daemon.register(r)
        with Pyro4.locateNS() as ns:
            ns.register("replica:%s" % r.id, uri, metadata={"replica"})
        Thread(target=r.gossip).start()
        daemon.requestLoop()
