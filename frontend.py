import Pyro4
import vector_clock


class Frontend:
    def __init__(self, ns):
        self.ns = ns
        self.time = vector_clock.create()
        self._replica = None

    def list_replicas(self):
        with self.ns:
            return self.ns.list(metadata_all={"replica"}).values()

    @property
    def replica(self):
        if self._replica and self._replica.available():
            return self._replica
        for uri in self.list_replicas():
            replica = Pyro4.Proxy(uri)
            if replica.available():
                self._replica = replica
                return self._replica
        raise RuntimeError("No replica available")

    @Pyro4.expose
    def get_time(self):
        return self.time

    @Pyro4.expose
    def get_ratings(self, user_id, t=None):
        if t is None:
            t = self.time
        rating, time = self.replica.get_ratings(user_id, t)
        self.time = vector_clock.merge(time, self.time)
        return rating

    @Pyro4.expose
    def add_rating(self, user_id, movie_id, value):
        t = self.replica.add_rating(user_id, movie_id, value)
        self.time = vector_clock.merge(t, self.time)

    @Pyro4.expose
    def add_rating_sync(self, user_id, movie_id, value):
        t = self.replica.add_rating_sync(user_id, movie_id, value, self.time)
        self.time = vector_clock.merge(t, self.time)


if __name__ == '__main__':
    ns = Pyro4.locateNS()
    with Pyro4.Daemon() as daemon:
        frontend = Frontend(ns)
        uri = daemon.register(frontend)
        ns.register("frontend", uri)
        try:
            daemon.requestLoop()
        except KeyboardInterrupt:
            ns.remove("frontend")
