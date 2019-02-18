from base64 import b64encode
from contextlib import contextmanager
from copy import deepcopy
from random import shuffle
from uuid import uuid4
from Pyro4.errors import ConnectionClosedError, CommunicationError, TimeoutError
import vector_clock as vc


def generate_id(l=10):
    return b64encode(uuid4().bytes, altchars=b"+-").decode()[:-2][:l]


def find_random_peers(ns, id, metadata):
    peers = ns.list(metadata_all={metadata})
    choices = [uri for name, uri in peers.items() if id not in name]
    shuffle(choices)
    return choices


@contextmanager
def ignore_disconnects():
    try:
        yield
    except (ConnectionError, ConnectionClosedError, CommunicationError, TimeoutError):
        pass


def sort_buffer(buffer):
    buffer.sort(key=lambda u: (u.time, u.id))


def apply_updates(ts, db, executed, log):
    order = []
    while True:
        has_event = False
        next_log = []
        for u in log:
            # we've seen this value before, throw away!
            if u.id in executed:
                continue
            if vc.geq(ts, u.prev):
                has_event = True
                u.apply(db)
                ts = vc.merge(ts, u.ts)
                order.append(u)
                executed.add(u.id)
                continue
            next_log.append(u)
        log = next_log
        if not has_event:
            break
    return ts, order, log
