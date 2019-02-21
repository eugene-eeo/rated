import Pyro4
import time
import random
from threading import Lock
from vector_clock import merge, create
from utils import ignore_disconnects


@Pyro4.behavior(instance_mode="session")
class Frontend:
    def __init__(self):
        self.ns = Pyro4.locateNS()
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
    def forget(self):
        self.ts = {}

    @Pyro4.expose
    def get_timestamp(self):
        return self.ts

    @Pyro4.expose
    def get_ratings(self, user_id):
        data, ts = self.replica.get(user_id, self.ts)
        self.ts = merge(ts, self.ts)
        return data

    @Pyro4.expose
    def get_aggregated_rating(self, movie_id):
        data, ts = self.replica.get_aggregated(movie_id, self.ts)
        self.ts = merge(ts, self.ts)
        return data

    @Pyro4.expose
    def add_rating(self, user_id, movie_id, value):
        ts = self.replica.update((user_id, movie_id, value), self.ts)
        self.ts = merge(ts, self.ts)

    @Pyro4.expose
    def delete_rating(self, user_id, movie_id):
        ts = self.replica.delete((user_id, movie_id), self.ts)
        self.ts = merge(ts, self.ts)


if __name__ == '__main__':
    with Pyro4.Daemon() as daemon:
        uri = daemon.register(Frontend)
        with Pyro4.locateNS() as ns:
            ns.register("frontend", uri)
        daemon.requestLoop()
