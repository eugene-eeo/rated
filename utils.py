from base64 import b64encode
from contextlib import contextmanager
from copy import deepcopy
from random import shuffle
from operator import attrgetter
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
    buffer.sort(key=attrgetter("time", "id"))


def apply_updates(ts, db, executed, log, buffer):
    has_event = True
    while has_event:
        has_event = False
        next_buff = []
        for u in buffer:
            # we've seen this value before, throw away!
            if u.id in executed:
                continue
            if vc.geq(ts, u.prev):
                has_event = True
                u.apply(db)
                ts = vc.merge(ts, u.ts)
                executed.add(u.id)
                log.append(u)
                continue
            next_buff.append(u)
        buffer = next_buff
    return ts, buffer
