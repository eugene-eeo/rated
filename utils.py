from base64 import b64encode
from contextlib import contextmanager
from copy import deepcopy
from random import shuffle
from uuid import uuid4
from Pyro4.errors import ConnectionClosedError, CommunicationError, TimeoutError
import vector_clock as vc


def generate_id(l=10):
    return b64encode(uuid4().bytes).decode()[:-2][:l]


def find_random_peers(ns, id, metadata):
    peers = ns.list(metadata_all={metadata})
    choices = [uri for name, uri in peers.items() if id not in name]
    shuffle(choices)
    return choices


@contextmanager
def ignore_disconnects():
    try:
        yield
    except (ConnectionClosedError, CommunicationError, TimeoutError):
        pass


def apply_updates(ts, db, log):
    log.sort(key=lambda x: (vc.sort_key(x.ts), x.id))

    # remove duplicate events
    for i in range(len(log) - 1, 0, -1):
        if log[i].id == log[i-1].id:
            del log[i]

    order = []

    while True:
        has_event = False
        next_log = []
        for u in log:
            if vc.geq(ts, vc.decrement(u.ts, u.node_id)):
                has_event = True
                u.apply(db)
                ts = vc.merge(ts, u.ts)
                order.append(u)
                continue
            next_log.append(u)
        log = next_log
        if not has_event:
            break

    return ts, order, log
