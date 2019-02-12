import time
import Pyro4
from models import Update
from threading import Lock, Thread
from utils import generate_id, find_random_peers, ignore_disconnects, merge
import vector_clock as vc


class Replica:
    def __init__(self):
        self.id = generate_id()
        self.ns = Pyro4.locateNS()
        # state and updates
        self.db = {}
        self.log = []
        self.ts = vc.create() # timestamp of state
        self.lock = Lock()
        # gossip
        self.sync_period = 2
        self.sync_ts = vc.create() # timestamp of replica

    def is_stable(self):
        if len(self.log) <= 1:
            return True
        it = iter(self.log)
        next(it)
        return all(vc.greater_than(b[1], a[1]) for a, b in zip(self.log, it))

    def peers(self):
        with self.ns:
            peers = find_random_peers(self.ns, self.id, "replica")
        n = 0
        for peer in peers:
            yield Pyro4.Proxy(peer)
            n += 1
            if n == 2:
                break

    def gossip(self):
        while True:
            time.sleep(self.sync_period)
            # filter relevant events
            logs = []
            for peer in self.peers():
                # if peer.available():
                t = peer.get_timestamp()
                with self.lock:
                    logs.append((peer, self.sync_ts, [
                        (u, tt) for u, tt in self.log if
                            vc.is_concurrent(tt, t) or
                            vc.greater_than(tt, t)
                        ]))
            # now send events
            for peer, ts, log in logs:
                with peer:
                    if len(log) == 0:
                        continue
                    peer.sync(log, ts)

    # exposed methods

    @Pyro4.expose
    def sync(self, log, ts):
        with self.lock:
            self.log = list(merge(self.log, [(Update(*u), ts) for u, ts in log]))
            self.sync_ts = vc.merge(self.sync_ts, ts)
            # apply updates if possible
            if self.is_stable():
                self.db = {}
                for u, ts in self.log:
                    u.apply(self.db)
                    self.ts = vc.merge(self.ts, ts)

    @Pyro4.expose
    def get_timestamp(self):
        return self.sync_ts

    @Pyro4.expose
    def get(self, user_id, ts):
        while True:
            with self.lock:
                print(self.ts)
                # can respond
                if vc.geq(self.ts, ts):
                    return self.db.get(user_id, {}), self.ts
            time.sleep(self.sync_period)

    @Pyro4.expose
    def update(self, update, ts):
        u = Update(*update)

        with self.lock:
            prev = ts.copy()
            self.sync_ts = vc.increment(self.sync_ts, self.id)
            ts[self.id] = self.sync_ts[self.id]

            # add to log
            self.log.append((u, ts))
            self.log.sort(key=lambda x: vc.sort_key(x[1]))

            # apply update immediately if possible
            if vc.geq(self.ts, prev):
                u.apply(self.db)
                self.ts = vc.merge(ts, self.ts)
            return ts


if __name__ == '__main__':
    r = Replica()
    with Pyro4.Daemon() as daemon:
        uri = daemon.register(r)
        with Pyro4.locateNS() as ns:
            ns.register("replica:%s" % r.id, uri, metadata={"replica"})
        Thread(target=r.gossip).start()
        daemon.requestLoop()
