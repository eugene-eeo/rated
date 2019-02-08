import time
import random
import threading
from contextlib import contextmanager

import Pyro4
import vector_clock
from models import Update, json_to_op
from utils import merge, generate_id, find_random_peers


class Replica:
    def __init__(self, ns):
        self.id = generate_id()
        self.ns = ns
        self.lock = threading.RLock()
        self.time = vector_clock.create()
        self.ratings = {}
        self.updates = []
        self.sync_period = 1  # seconds until next sync

    def active_peers(self, limit=2):
        n = 0
        for uri in find_random_peers(self.ns, self.id, "replica"):
            peer = Pyro4.Proxy(uri)
            if not peer.available():
                peer._pyroRelease()
                continue
            yield peer
            n += 1
            if n == limit:
                break

    def get_updates_since(self, t0):
        # here we need to use strict=False to prevent concurrent writes
        # from getting lost, since compare(strict=True) imposes a global order
        return [(t, op.to_json()) for (t, op) in self.updates
                if vector_clock.compare(t0, t, strict=False) < 1]

    def bg_sync(self):
        while True:
            time.sleep(self.sync_period)
            to_sync = []
            # fetch current time
            with self.lock:
                end = self.time
            for peer in self.active_peers():
                # this is ok since get_time() is monotone increasing
                t = peer.get_time()
                if t != end:
                    with self.lock:
                        to_sync.append((peer, self.get_updates_since(t)))
            # move out of lock to prevent deadlock from happening
            for peer, updates in to_sync:
                if updates:
                    peer.sync(end, updates)
                peer._pyroRelease()

    def increment(self):
        self.time = vector_clock.increment(self.time, self.id, time.time())

    @contextmanager
    def spin_until(self, t, guarantee=10):
        for _ in range(guarantee):
            with self.lock:
                if self.time == t or vector_clock.greater_than(self.time, t):
                    yield
                    return
            time.sleep(self.sync_period)
        with self.lock:
            yield

    @Pyro4.expose
    def sync(self, end, updates):
        with self.lock:
            updates = [(t, json_to_op(op)) for t, op in updates]
            self.updates = list(merge(self.updates, updates))
            self.ratings = {}
            for t, op in self.updates:
                op.apply(self.ratings)
            self.time = vector_clock.merge(self.time, end)

    @Pyro4.expose
    def available(self):
        if random.random() <= 0.75:
            return True
        return False

    @Pyro4.expose
    def get_time(self):
        with self.lock:
            return self.time

    @Pyro4.expose
    def get_ratings(self, user_id, time):
        with self.spin_until(time):
            return (
                self.ratings.get(user_id, {}),
                self.time,
                )

    @Pyro4.expose
    def add_rating_sync(self, user_id, movie_id, value, t):
        with self.spin_until(t):
            self.increment()
            op = Update(user_id, movie_id, value)
            self.updates.append((self.time, op))
            op.apply(self.ratings)
            return self.time

    @Pyro4.expose
    def add_rating(self, user_id, movie_id, value):
        with self.lock:
            self.increment()
            op = Update(user_id, movie_id, value)
            self.updates.append((self.time, op))
            op.apply(self.ratings)
            return self.time


if __name__ == '__main__':
    ns = Pyro4.locateNS()
    with Pyro4.Daemon() as daemon:
        replica = Replica(ns)
        name = "replica:%s" % replica.id
        uri = daemon.register(replica)
        ns.register(name, uri, metadata={"replica"})
        try:
            threading.Thread(target=replica.bg_sync).start()
            daemon.requestLoop()
        except KeyboardInterrupt:
            ns.remove(name)
