import time
import random
import threading
import Pyro4
import vector_clock
from uuid import uuid4
from contextlib import contextmanager
from models import Update, json_to_op


def merge(a, b):
    i = 0
    j = 0
    while i < len(a) and j < len(b):
        x = a[i]
        y = b[j]
        if x == y:
            yield x
            i += 1
            j += 1
            continue
        c = vector_clock.compare(x[0], y[0])
        if c == -1: yield x; i += 1  # x < y
        if c ==  1: yield y; j += 1  # x > y
        if c ==  0: yield y; j += 1
    # one of the sequences must be empty
    yield from a[i:]
    yield from b[j:]


class Replica:
    def __init__(self, ns):
        self.id = uuid4().hex
        self.ns = ns
        self.lock = threading.RLock()
        self.time = vector_clock.create()
        self.ratings = {}
        self.updates = []
        self.sync_period = 0.25  # seconds until next sync

    def active_peers(self, limit=5):
        choices = [
            uri for name, uri in self.ns.list(metadata_all={"replica"}).items()
            if self.id not in name
        ]
        random.shuffle(choices)
        n = 0
        for uri in choices:
            peer = Pyro4.Proxy(uri)
            if not peer.available():
                peer._pyroRelease()
                continue
            yield peer
            n += 1
            if n == limit:
                break

    def get_updates_since(self, t0):
        return [
            (t, op.to_json()) for (t, op) in self.updates
            if vector_clock.greater_than(t, t0)
        ]

    def bg_sync(self):
        while True:
            time.sleep(self.sync_period)
            to_sync = []
            for peer in self.active_peers():
                # this is ok since get_time() is monotone increasing
                t = peer.get_time()
                with self.lock:
                    updates = self.get_updates_since(t)
                    if updates:
                        to_sync.append((peer, self.time, updates))
            # avoid deadlock
            for peer, end, updates in to_sync:
                peer.sync(end, updates)
                peer._pyroRelease()

    @contextmanager
    def spin_until_can_reply(self, t, guarantee=10):
        for _ in range(guarantee):
            with self.lock:
                if self.time == t or vector_clock.greater_than(self.time, t):
                    yield
                    return
            time.sleep(self.sync_period)
        yield

    @Pyro4.expose
    def sync(self, end, updates):
        with self.lock:
            updates = [(t, json_to_op(op)) for t, op in updates]
            self.updates = list(merge(self.updates, updates))
            ratings = {}
            for _, op in self.updates:
                op.apply(ratings)
            self.ratings = ratings
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
        with self.spin_until_can_reply(time):
            return (
                self.ratings.get(user_id, {}),
                self.time,
                )

    @Pyro4.expose
    def add_rating(self, user_id, movie_id, value, t):
        with self.spin_until_can_reply(t):
            op = Update(user_id, movie_id, value)
            self.time = vector_clock.increment(self.time, self.id, time.time())
            self.updates.append((self.time, op))
            op.apply(self.ratings)
            return self.time


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
