from time import sleep
from itertools import chain, islice
from contextlib import contextmanager
from random import random

import Pyro4
from models import Update
from threading import Lock, Thread
from utils import generate_id, find_random_peers, ignore_disconnects, merge
import vector_clock as vc


class Replica:
    def __init__(self):
        self.id = generate_id()
        self.ns = Pyro4.locateNS()
        # state and updates
        self.db = {}
        self.log = []
        self.buffer = []
        self.ts = vc.create() # timestamp of state
        self._lock = Lock()
        # gossip
        self.busy = False
        self.sync_period = 2
        self.sync_ts = vc.create() # timestamp of replica
        self.seen = set()

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
            sleep(self.sync_period)
            logs = []
            for peer in islice(self.peers(), 5):
                with ignore_disconnects():
                    t = peer.get_timestamp()
                    with self.lock:
                        events = []
                        # only scan log if the other replica hasn't caught up
                        if t != self.sync_ts:
                            log = chain(self.log, self.buffer) if vc.geq(self.ts, t) else self.buffer
                            events = [u for u in log if
                                vc.is_concurrent(u.ts, t) or
                                vc.greater_than(u.ts, t)
                                ][:100]
                        logs.append((peer, events))
            # now send events
            for peer, log in logs:
                with peer, ignore_disconnects():
                    if log:
                        ts = log[-1].ts
                        peer.sync(log, ts)

    # exposed methods

    @Pyro4.expose
    def available(self):
        return not self.busy and random() <= 0.75

    @Pyro4.expose
    def sync(self, log, ts):
        with self.lock:
            self.sync_ts = vc.merge(self.sync_ts, ts)
            self.buffer = merge(self.buffer, [Update(*u) for u in log])
            # apply updates if possible
            n = 0
            for u in self.buffer:
                if u.id in self.seen:
                    n += 1
                    continue
                if vc.geq(self.ts, vc.decrement(u.ts, u.node_id)):
                    u.apply(self.db)
                    self.ts = vc.merge(self.ts, u.ts)
                    self.seen.add(u.id)
                    self.log.append(u)
                    n += 1
                    continue
                break
            # trim
            self.buffer = self.buffer[n:]

    @Pyro4.expose
    def get_log(self):
        return self.ts, self.log

    @Pyro4.expose
    def get_timestamp(self):
        return self.ts

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
            u = Update(generate_id(15), *update, self.id, ts)

            # apply update immediately if possible
            if vc.geq(self.ts, prev):
                u.apply(self.db)
                self.ts = vc.merge(u.ts, self.ts)
                self.log.append(u)
                self.seen.add(u.id)
            else:
                self.buffer.append(u)
            return ts


if __name__ == '__main__':
    r = Replica()
    with Pyro4.Daemon() as daemon:
        uri = daemon.register(r)
        with Pyro4.locateNS() as ns:
            ns.register("replica:%s" % r.id, uri, metadata={"replica"})
        Thread(target=r.gossip).start()
        daemon.requestLoop()
