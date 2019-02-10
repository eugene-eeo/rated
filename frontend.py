import Pyro4
import vector_clock
from utils import ignore_disconnects


@Pyro4.behavior(instance_mode="session")
class Frontend:
    def __init__(self):
        self.ns = Pyro4.locateNS()
        self.time = vector_clock.create()
        self._replica = None

    def list_replicas(self):
        with self.ns:
            return self.ns.list(metadata_all={"replica"}).values()

    @property
    def replica(self):
        with ignore_disconnects():
            if self._replica and self._replica.available():
                return self._replica
        for uri in self.list_replicas():
            with ignore_disconnects():
                replica = Pyro4.Proxy(uri)
                if replica.available():
                    self._replica = replica
                    return self._replica
        raise RuntimeError("No replica available")

    @Pyro4.expose
    def get_time(self):
        return self.time

    @Pyro4.expose
    def get_all_ratings(self):
        for (user, ratings), time in self.replica.get_all_ratings(self.time):
            self.time = vector_clock.merge(time, self.time)
            yield user, ratings

    @Pyro4.expose
    def get_ratings(self, user_id):
        ratings, time = self.replica.get_ratings(user_id, self.time)
        self.time = vector_clock.merge(time, self.time)
        return ratings

    @Pyro4.expose
    def add_rating(self, user_id, movie_id, value):
        t = self.replica.add_rating(user_id, movie_id, value)
        self.time = vector_clock.merge(t, self.time)


if __name__ == '__main__':
    with Pyro4.Daemon() as daemon:
        uri = daemon.register(Frontend)
        with Pyro4.locateNS() as ns:
            ns.register("frontend", uri)
        daemon.requestLoop()
