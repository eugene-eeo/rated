import time
from itertools import chain

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
        self.buffer = []
        self.sync_period = 2
        self.sync_ts = vc.create() # timestamp of replica

    def peers(self, limit=2):
        with self.ns:
            peers = find_random_peers(self.ns, self.id, "replica")
        n = 0
        for peer in peers:
            yield Pyro4.Proxy(peer)
            n += 1
            if n == limit:
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
                    # only perform checks if the other replica hasn't caught up
                    # with us
                    events = []
                    if t != self.sync_ts:
                        events = [(u, tt) for u, tt in chain(self.log, self.buffer) if
                                vc.is_concurrent(tt, t) or
                                vc.greater_than(tt, t)]
                    logs.append((peer, self.sync_ts, events))
            # now send events
            for peer, ts, log in logs:
                with peer:
                    if log:
                        peer.sync(log, ts)

    # exposed methods

    @Pyro4.expose
    def sync(self, log, ts):
        with self.lock:
            self.buffer, is_stable = merge(self.buffer, [(Update(*u), ts) for u, ts in log])
            self.sync_ts = vc.merge(self.sync_ts, ts)
            # apply updates if possible
            if is_stable:
                for u, ts in self.buffer:
                    u.apply(self.db)
                    self.ts = vc.merge(self.ts, ts)
                self.log.extend(self.buffer)
                self.buffer = []

    @Pyro4.expose
    def get_timestamp(self):
        return self.sync_ts

    @Pyro4.expose
    def get(self, user_id, ts):
        while True:
            with self.lock:
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

            # apply update immediately if possible
            if vc.geq(self.ts, prev):
                u.apply(self.db)
                self.ts = vc.merge(ts, self.ts)
                self.log.append((u, ts))
            else:
                self.buffer.append((u, ts))
            return ts


if __name__ == '__main__':
    r = Replica()
    with Pyro4.Daemon() as daemon:
        uri = daemon.register(r)
        with Pyro4.locateNS() as ns:
            ns.register("replica:%s" % r.id, uri, metadata={"replica"})
        Thread(target=r.gossip).start()
        daemon.requestLoop()
