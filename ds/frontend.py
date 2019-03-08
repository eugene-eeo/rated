import Pyro4
import time
import random
from vector_clock import merge
from utils import ignore_disconnects, unregister_at_exit, generate_id, ignore_status_errors
from models import AddTag, RemoveTag, Delete, Update, UpdateMovie


@Pyro4.behavior(instance_mode="session")
class Frontend:
    def __init__(self):
        self.ns = Pyro4.locateNS()
        self.ts = {}
        self._replica = None

    def list_replicas(self):
        with self.ns:
            return list(self.ns.list(metadata_all={"replica"}).values())

    def replicas(self, patience=3):
        # try to give our previous replica if we can
        with ignore_disconnects(), ignore_status_errors():
            if self._replica and self._replica.status() == 'online':
                yield self._replica
        # otherwise we try to find random replicas that are online
        for _ in range(patience):
            uris = self.list_replicas()
            random.shuffle(uris)
            for uri in uris:
                with ignore_disconnects(), ignore_status_errors():
                    replica = Pyro4.Proxy(uri)
                    if replica.status() == 'online':
                        self._replica = replica
                        yield self._replica
            time.sleep(0.05)
        # no replicas accepted => raise exception
        raise RuntimeError("No replica available")

    def execute_on_majority(self, f):
        uris = self.list_replicas()
        sent = set()
        patience = 5
        majority = (len(uris) // 2) + 1
        while True:
            for uri in set(uris) - sent:
                with ignore_status_errors():
                    # if f(replica) proceeds without error, then we have
                    # successfully completed some operation on the replica
                    f(Pyro4.Proxy(uri))
                    sent.add(uri)
            patience -= 1
            if len(sent) >= majority:
                return sent
            if patience == 0:
                raise RuntimeError("cannot get consensus")
            # otherwise relax
            time.sleep(0.05)

    def get_max_timestamp(self):
        ts = {}
        @self.execute_on_majority
        def find_max_ts(replica):
            nonlocal ts
            ts = merge(ts, replica.get_timestamp())
        return ts

    @Pyro4.expose
    def forget(self):
        self.ts = {}

    @Pyro4.expose
    def get_timestamp(self):
        return self.ts

    def update_ts(self, ts):
        self.ts = merge(ts, self.ts)

    def forced_update(self, update):
        dep = self.get_max_timestamp()
        uid = generate_id()
        # prepare
        sent = self.execute_on_majority(
            lambda r: r.accept_update(uid, update.to_raw(), dep)
        )
        # commit
        ts = None
        while sent:
            with ignore_status_errors():
                for uri in list(sent):
                    ts = Pyro4.Proxy(uri).commit_update(uid)
                    sent.discard(uri)
            time.sleep(0.05)
        self.update_ts(ts)

    def send_update(self, update, max=False):
        if max:
            return self.forced_update(update)
        for replica in self.replicas():
            # send the update to the first replica we find;
            # if the replica goes offline here then we try
            # the next replica.
            ts = replica.update(update.to_raw(), self.ts)
            self.update_ts(ts)
            return

    @Pyro4.expose
    def get_user_data(self, user_id):
        for replica in self.replicas():
            data, ts = replica.get(user_id, self.ts)
            self.update_ts(ts)
            return data

    @Pyro4.expose
    def list_movies(self):
        dep = self.get_max_timestamp()
        for replica in self.replicas():
            data, ts = replica.list_movies(dep)
            self.update_ts(ts)
            return data

    @Pyro4.expose
    def search(self, name, genres):
        for replica in self.replicas():
            data, ts = replica.search(name, genres, self.ts)
            self.update_ts(ts)
            return data

    @Pyro4.expose
    def get_movie(self, movie_id):
        for replica in self.replicas():
            data, ts = replica.get_movie(movie_id, self.ts)
            self.update_ts(ts)
            return data

    @Pyro4.expose
    def add_rating(self, user_id, movie_id, value):
        self.send_update(Update(user_id, movie_id, value))

    @Pyro4.expose
    def delete_rating(self, user_id, movie_id):
        self.send_update(Delete(user_id, movie_id))

    @Pyro4.expose
    def add_tag(self, user_id, movie_id, tag):
        self.send_update(AddTag(user_id, movie_id, tag))

    @Pyro4.expose
    def remove_tag(self, user_id, movie_id, tag):
        self.send_update(RemoveTag(user_id, movie_id, tag))

    @Pyro4.expose
    def add_movie(self, name, genres):
        id = generate_id(5)
        self.send_update(UpdateMovie(id, {"name": name, "genres": genres}), max=True)
        return id


if __name__ == '__main__':
    with Pyro4.Daemon() as daemon:
        uri = daemon.register(Frontend, "frontend")
        with Pyro4.locateNS() as ns:
            ns.register("frontend", uri)
        unregister_at_exit("frontend")
        daemon.requestLoop()
