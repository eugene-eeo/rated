import Pyro4
import random
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
            return self.ns.list(metadata_all={"replica"}).values()

    @property
    def replica(self):
        uris = list(self.list_replicas())
        random.shuffle(uris)
        for uri in uris:
            with ignore_disconnects():
                replica = Pyro4.Proxy(uri)
                self._replica = replica
                return self._replica
        # with ignore_disconnects():
        #     if self._replica and self._replica.available():
        #         return self._replica
        # for uri in self.list_replicas():
        #     with ignore_disconnects():
        #         replica = Pyro4.Proxy(uri)
        #         if replica.available():
        #             self._replica = replica
        #             return self._replica
        # raise RuntimeError("No replica available")

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


if __name__ == '__main__':
    with Pyro4.Daemon() as daemon:
        uri = daemon.register(Frontend)
        with Pyro4.locateNS() as ns:
            ns.register("frontend", uri)
        daemon.requestLoop()
