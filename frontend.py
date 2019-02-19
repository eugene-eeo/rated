import Pyro4
import time
import random
from threading import Lock
from vector_clock import merge, create
from utils import ignore_disconnects


PRIMARY_ID = "_"


class PrimaryFinder:
    def __init__(self):
        self.ns = Pyro4.locateNS()
        self.ts = 0
        self.lock = Lock()
        self._primary = None
        self._find_primary_ts()

    def _find_primary_ts(self):
        with self.ns:
            uris = list(self.ns.list(metadata_all={"replica"}).values())
            uris.sort()
        for uri in uris:
            with ignore_disconnects():
                replica = Pyro4.Proxy(uri)
                if replica.status() == 'online':
                    self.merge(replica.get_timestamp().get(PRIMARY_ID, 0))
                    if self._primary is None:
                        self._primary = replica

    def get_ts(self):
        with self.lock:
            return {PRIMARY_ID: self.ts}

    def merge(self, ts):
        with self.lock:
            self.ts = max(ts, self.ts)

    @property
    def primary(self):
        with self.lock:
            with ignore_disconnects():
                if self._primary and self._primary.status() != 'offline':
                    return self._primary
            for _ in range(3):
                with self.ns:
                    uris = list(self.ns.list(metadata_all={"replica"}).values())
                    uris.sort()
                for uri in uris:
                    with ignore_disconnects():
                        replica = Pyro4.Proxy(uri)
                        if replica.status() != 'offline':
                            self._primary = replica
                            return replica
                time.sleep(0.05)
        raise RuntimeError("Cannot find primary!")


@Pyro4.behavior(instance_mode="session")
class Frontend:
    pf = PrimaryFinder()

    def __init__(self):
        self.ns = self.pf.ns
        self.ts = create()
        self._replica = None

    def list_replicas(self):
        with self.ns:
            return list(self.ns.list(metadata_all={"replica"}).values())

    @property
    def replica(self):
        # try to get primary
        with ignore_disconnects():
            if self._replica is not None and self._replica.status() == 'online':
                return self._replica
        for _ in range(3):
            # try random replicas
            uris = self.list_replicas()
            random.shuffle(uris)
            for uri in uris:
                with ignore_disconnects():
                    replica = Pyro4.Proxy(uri)
                    if replica.status() == 'online':
                        self._replica = replica
                        return self._replica
            time.sleep(0.05)
        raise RuntimeError("No replica available")

    @Pyro4.expose
    def get_timestamp(self):
        return self.ts

    @Pyro4.expose
    def get_ratings(self, user_id):
        data, ts = self.replica.get(user_id, self.ts)
        self.ts = merge(ts, self.ts)
        return data

    @Pyro4.expose
    def add_rating(self, user_id, movie_id, value):
        ts = self.replica.update((user_id, movie_id, value), self.ts)
        self.ts = merge(ts, self.ts)

    @Pyro4.expose
    def add_rating_sync(self, user_id, movie_id, value):
        ts = self.pf.primary.update(
            (user_id, movie_id, value),
            merge(self.ts, self.pf.get_ts()),
            forced=True,
            )
        self.ts = merge(ts, self.ts)
        self.pf.merge(ts[PRIMARY_ID])


if __name__ == '__main__':
    with Pyro4.Daemon() as daemon:
        uri = daemon.register(Frontend)
        with Pyro4.locateNS() as ns:
            ns.register("frontend", uri)
        daemon.requestLoop()
