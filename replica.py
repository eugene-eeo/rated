import time
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
        self.ts = vc.create() # timestamp of state
        self._lock = Lock()
        # gossip
        self.busy = False
        self.buffer = []
        self.sync_period = 2
        self.sync_ts = vc.create() # timestamp of replica

    @property
    @contextmanager
    def lock(self):
        with self._lock:
            self.busy = True
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
            time.sleep(self.sync_period)
            # filter relevant events
            logs = []
            for peer in islice(self.peers(), 2):
                with ignore_disconnects():
                    t = peer.get_timestamp()
                    with self.lock:
                        events = []
                        # only scan log if the other replica hasn't caught up
                        if t != self.sync_ts:
                            log = chain(self.log, self.buffer) if vc.geq(self.ts, t) else self.buffer
                            events = [(u, tt) for u, tt in log if
                                    vc.is_concurrent(tt, t) or
                                    vc.greater_than(tt, t)]
                        logs.append((peer, self.sync_ts, events))
            # now send events
            for peer, ts, log in logs:
                with peer, ignore_disconnects():
                    if log:
                        peer.sync(log, ts)

    # exposed methods

    @Pyro4.expose
    def available(self):
        return not self.busy and random() <= 0.75

    @Pyro4.expose
    def sync(self, log, ts):
        with self.lock:
            self.buffer, is_stable = merge(self.buffer, [(Update(*u), ts) for u, ts in log])
            self.sync_ts = vc.merge(self.sync_ts, ts)
            # apply updates if possible
            if is_stable:
                for u, ts in self.buffer:
                    u.apply(self.db)
                    self.ts = vc.merge(self.ts, ts)
                self.log.extend(self.buffer)
                self.buffer = []

    @Pyro4.expose
    def get_timestamp(self):
        return self.sync_ts

    @Pyro4.expose
    def get(self, user_id, ts):
        while True:
            with self.lock:
                # can respond
                if vc.geq(self.ts, ts):
                    return self.db.get(user_id, {}), self.ts
            time.sleep(self.sync_period)

    @Pyro4.expose
    def update(self, update, ts):
        u = Update(*update)

        with self.lock:
            prev = ts.copy()
            self.sync_ts = vc.increment(self.sync_ts, self.id)
            ts[self.id] = self.sync_ts[self.id]

            # apply update immediately if possible
            if vc.geq(self.ts, prev):
                u.apply(self.db)
                self.ts = vc.merge(ts, self.ts)
                self.log.append((u, ts))
            else:
                self.buffer.append((u, ts))
            return ts


if __name__ == '__main__':
    r = Replica()
    with Pyro4.Daemon() as daemon:
        uri = daemon.register(r)
        with Pyro4.locateNS() as ns:
            ns.register("replica:%s" % r.id, uri, metadata={"replica"})
        Thread(target=r.gossip).start()
        daemon.requestLoop()
