import time
import random
import threading
from collections import namedtuple

import Pyro4
import vector_clock
from utils import find_random_peers, generate_id, merge


class Update(namedtuple("Update", ("id","name"))):
    def apply(self, db):
        db[self.id] = self.name


class MovieNameReplica:
    def __init__(self, ns):
        self.id = generate_id()
        self.ns = ns
        self.lock = threading.RLock()
        self.time = vector_clock.create()
        self.db = {}
        self.updates = []
        self.sync_period = 1
        self.has_updates = False

    @property
    def random_peers(self):
        for uri in find_random_peers(self.ns, self.id, "movie_name"):
            peer = Pyro4.Proxy(uri)
            yield peer

    def bg_sync(self):
        while True:
            time.sleep(self.sync_period)
            # check if we have any updates that need to be synced
            should_sync = True
            with self.lock:
                should_sync = self.has_updates
            if not should_sync:
                continue

            updates = []
            for peer in self.random_peers:
                t = peer.get_time()
                with self.lock:
                    updates.append((peer, self.time, self.get_updates_since(t)))

            for peer, t, updates in updates:
                with peer:
                    peer.sync(t, updates)
