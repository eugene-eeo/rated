import Pyro4
import time
import random
from queue import Queue
from threading import Lock, Thread
from vector_clock import merge, create
from utils import ignore_disconnects, unregister_at_exit, generate_id, ignore_status_errors
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
        # no replicas => raise exception
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

    def forced_update(self, update):
        with self.lock:
            dep = Frontend.max_ts
        uid = generate_id()
        uris = self.list_replicas()
        sent = set()
        majority = (len(uris) // 2) + 1
        for _ in range(5):
            # 2PC: majority should accept update
            for uri in set(uris) - sent:
                with ignore_status_errors():
                    replica = Pyro4.Proxy(uri)
                    replica.accept_update(uid, update.to_raw(), dep)
                    sent.add(uri)
            if len(sent) >= majority:
                break
            time.sleep(0.05)
        # make sure that we reach majority
        if len(sent) < majority:
            raise RuntimeError("cannot perform update")
        # commit
        ts = None
        while sent:
            with ignore_status_errors():
                for uri in list(sent):
                    replica = Pyro4.Proxy(uri)
                    ts = replica.commit_update(uid)
                    sent.discard(uri)
            time.sleep(0.05)
        self.update_ts(ts)

    def send_update(self, update, max=False):
        if max:
            self.forced_update(update)
            return
        for replica in self.replicas():
            # send the update to the first replica we find;
            # if the replica goes offline here then we try
            # the next replica.
            ts = replica.update(update.to_raw(), self.ts)
            self.update_ts(ts)
            break

    @Pyro4.expose
    def get_user_data(self, user_id):
        for replica in self.replicas():
            data, ts = replica.get(user_id, self.ts)
            self.update_ts(ts)
            return data

    @Pyro4.expose
    def list_movies(self, maximal=False):
        dep = self.ts
        if maximal:
            with self.lock:
                dep = Frontend.max_ts
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
