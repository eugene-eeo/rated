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


def sort_buffer(buffer):
    buffer.sort(key=lambda u: (vc.sort_key(u.ts), u.time))
    # remove duplicate events
    for i in range(len(buffer) - 1, 0, -1):
        if buffer[i].id == buffer[i-1].id:
            del buffer[i]


def need_reconstruction(log, buffer):
    if not log:
        return False
    return (
        vc.is_concurrent(log[-1].ts, buffer[0].ts)
        and log[-1].time > buffer[0].time
        )


def apply_updates(ts, db, log):
    order = []
    while True:
        has_event = False
        next_log = []
        for u in log:
            # we've seen this value before, throw away!
            if vc.geq(ts, u.ts):
                continue
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
