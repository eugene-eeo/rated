import os
import signal
from time import sleep, time
from itertools import chain, islice
from contextlib import contextmanager
from random import random

import Pyro4
from models import Update, Entry, Delete
from threading import Lock, Thread
from utils import generate_id, find_random_peers, ignore_disconnects, apply_updates, sort_buffer
import vector_clock as vc


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
                        events = [e.to_raw() for e in log if vc.compare(e.ts, t) >= 0]
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

    @contextmanager
    def spin(self, ts, guarantee=10):
        while True:
            with self.lock:
                # can respond
                if vc.geq(self.ts, ts):
                    yield
                    return
                if guarantee == 0:
                    raise RuntimeError("Cannot retrieve value!")
            sleep(self.sync_period)
            guarantee -= 1

    def add_update(self, op, prev):
        ts = prev.copy()
        new_sync_ts = vc.increment(self.sync_ts, self.id)
        ts[self.id] = new_sync_ts[self.id]
        # commit update immediately if possible
        self.buffer.append(Entry(generate_id(5), op, prev, ts, time()))
        self.apply_updates()
        self.need_reconstruct = True
        self.sync_ts = new_sync_ts
        return ts

    # exposed methods

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
            self.buffer.extend(Entry.from_raw(u) for u in log)
            self.has_new_gossip = True

    @Pyro4.expose
    def get_log(self):
        return self.ts, self.log

    @Pyro4.expose
    def get_timestamp(self):
        return self.sync_ts

    @Pyro4.expose
    def get_aggregated(self, movie_id, ts):
        with self.spin(ts):
            ratings = [r[movie_id] for r in self.db.values() if movie_id in r]
            avg = lambda r: (sum(r) / len(r))
            stats = {
                "avg": avg(ratings) if ratings else None,
                "min": min(ratings) if ratings else None,
                "max": max(ratings) if ratings else None,
            }
            return stats, self.ts

    @Pyro4.expose
    def get(self, user_id, ts):
        with self.spin(ts):
            return self.db.get(user_id, {}), self.ts

    @Pyro4.expose
    def delete(self, pair, ts):
        with self.lock:
            return self.add_update(Delete(*pair), ts)

    @Pyro4.expose
    def update(self, update, ts):
        with self.lock:
            return self.add_update(Update(*update), ts)


if __name__ == '__main__':
    r = Replica()
    with Pyro4.Daemon() as daemon:
        uri = daemon.register(r, objectId=r.id)
        with Pyro4.locateNS() as ns:
            ns.register("replica:%s" % r.id, uri, metadata={"replica"})

        def unregister():
            Pyro4.locateNS().remove("replica:%s" % r.id)
            os._exit(0)

        signal.signal(signal.SIGTERM, lambda *_: (unregister()))
        signal.signal(signal.SIGINT,  lambda *_: (unregister()))

        Thread(target=r.gossip).start()
        daemon.requestLoop()
