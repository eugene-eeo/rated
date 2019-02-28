import Pyro4
import time
import random
from queue import Queue
from threading import Lock, Thread
from vector_clock import merge, create
from utils import ignore_disconnects, unregister_at_exit, generate_id
from models import AddTag, RemoveTag, Delete, Update, UpdateMovie


def queue_task():
    while True:
        ts = Frontend.queue.get()
        with Frontend.lock:
            Frontend.max_ts = merge(Frontend.max_ts, ts)


@Pyro4.behavior(instance_mode="session")
class Frontend:
    # use this for creating/updating movies
    max_ts = {}
    lock = Lock()
    queue = Queue()

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
    def get_max(self):
        with self.lock:
            return Frontend.max_ts

    def update_ts(self, ts):
        self.ts = merge(ts, self.ts)
        self.queue.put_nowait(self.ts)

    def send_update(self, update, max=False):
        dep = self.ts
        if max:
            with self.lock:
                dep = Frontend.max_ts
        ts = self.replica.update(update.to_raw(), dep)
        self.update_ts(ts)

    @Pyro4.expose
    def get_user_data(self, user_id):
        data, ts = self.replica.get(user_id, self.ts)
        self.update_ts(ts)
        return data

    @Pyro4.expose
    def list_movies(self, maximal=False):
        dep = self.ts
        if maximal:
            with self.lock:
                dep = Frontend.max_ts
        data, ts = self.replica.list_movies(dep)
        self.update_ts(ts)
        return data

    @Pyro4.expose
    def search(self, name, genres):
        data, ts = self.replica.search(name, genres, self.ts)
        self.update_ts(ts)
        return data

    @Pyro4.expose
    def get_movie(self, movie_id):
        data, ts = self.replica.get_movie(movie_id, self.ts)
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
    def update_movie(self, movie_id, name, genres):
        self.send_update(UpdateMovie(movie_id, {"name": name, "genres": genres}), max=True)

    @Pyro4.expose
    def add_movie(self, name, genres):
        id = generate_id(5)
        self.send_update(UpdateMovie(id, {"name": name, "genres": genres}), max=True)
        return id


if __name__ == '__main__':
    Thread(target=queue_task).start()
    with Pyro4.Daemon() as daemon:
        uri = daemon.register(Frontend, "frontend")
        with Pyro4.locateNS() as ns:
            ns.register("frontend", uri)
        unregister_at_exit("frontend")
        daemon.requestLoop()
