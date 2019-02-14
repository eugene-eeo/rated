from time import sleep, time
from itertools import chain, islice
from contextlib import contextmanager
from random import random

import Pyro4
from models import Update
from threading import Lock, Thread
from utils import generate_id, find_random_peers, ignore_disconnects
import vector_clock as vc


class Replica:
    def __init__(self):
        self.id = generate_id(5)
        self.ns = Pyro4.locateNS()
        # state and updates
        self.db = {}
        self.log = []
        self.ts = vc.create() # timestamp of state
        self._lock = Lock()
        # gossip
        self.busy = False
        self.sync_period = 2
        self.sync_ts = vc.create() # timestamp of replica
        self.has_new_gossip = False

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
                if not peer.available():
                    peer._pyroRelease()
                    continue
            yield peer

    def gossip(self):
        while True:
            with self.lock:
                if self.has_new_gossip:
                    self.has_new_gossip = False
                    self.apply_updates()
            sleep(self.sync_period)
            logs = []
            for peer in islice(self.peers(), 5):
                with ignore_disconnects():
                    t = peer.get_timestamp()
                    with self.lock:
                        log = chain(self.log)
                        events = [u for u in log if vc.is_concurrent(u.ts, t) or vc.greater_than(u.ts, t)]
                        logs.append((peer, self.sync_ts, events))
            # now send events
            for peer, ts, log in logs:
                with peer, ignore_disconnects():
                    if log:
                        peer.sync(log, ts)

    def apply_updates(self):
        # replay history
        self.ts = vc.create()
        self.db = {}
        seen = set()
        # apply updates if possible
        h = True
        unprocessed = self.log
        log = []
        while h:
            h = False
            b = []
            unprocessed.sort(key=lambda x: (vc.sort_key(x.ts), x.time))
            for u in unprocessed:
                if u.id in seen:
                    continue
                if vc.geq(self.ts, vc.decrement(u.ts, u.node_id)):
                    h = True
                    u.apply(self.db)
                    self.ts = vc.merge(self.ts, u.ts)
                    seen.add(u.id)
                    log.append(u)
                    continue
                b.append(u)
            unprocessed = b
        log.extend(unprocessed)
        self.log = log
        print(self.id, self.ts, len(self.log))

    # exposed methods

    @Pyro4.expose
    def available(self):
        return not self.busy and random() <= 0.75

    @Pyro4.expose
    def sync(self, log, ts):
        with self.lock:
            self.sync_ts = vc.merge(self.sync_ts, ts)
            self.log.extend(Update(*u) for u in log)
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
                if vc.geq(self.ts, ts) or guarantee <= 0:
                    return self.db.get(user_id, {}), self.ts
            sleep(self.sync_period)
            guarantee -= 1

    @Pyro4.expose
    def update(self, update, ts):
        with self.lock:
            prev = ts.copy()
            self.sync_ts = vc.increment(self.sync_ts, self.id)
            ts[self.id] = self.sync_ts[self.id]
            u = Update(generate_id(15), *update, self.id, ts, time())

            # apply update immediately if possible
            self.log.append(u)
            if vc.geq(self.ts, prev):
                u.apply(self.db)
                self.ts = vc.merge(u.ts, self.ts)
            return ts


if __name__ == '__main__':
    r = Replica()
    with Pyro4.Daemon() as daemon:
        uri = daemon.register(r)
        with Pyro4.locateNS() as ns:
            ns.register("replica:%s" % r.id, uri, metadata={"replica"})
        Thread(target=r.gossip).start()
        daemon.requestLoop()
