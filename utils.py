from base64 import b64encode
from random import shuffle
from uuid import uuid4
from contextlib import contextmanager
from itertools import islice

from vector_clock import compare
from Pyro4.errors import ConnectionClosedError, CommunicationError, TimeoutError


def generate_id():
    return b64encode(uuid4().bytes).decode()[:-2]


def merge(a, b):
    # Merges two sorted event logs together
    stable = True
    u = []
    i = 0
    j = 0
    while i < len(a) and j < len(b):
        x = a[i]
        y = b[j]
        # duplicate event
        if x == y:
            u.append(x)
            i += 1
            j += 1
            continue
        c = compare(x[1], y[1])
        if c == -1: u.append(x); i += 1  # x < y
        if c == +1: u.append(y); j += 1  # x > y
        if c == 0:
            stable = False
            u.append(x)
            u.append(y)
            i += 1
            j += 1
    # one of the sequences must be empty
    u.extend(islice(a, i, None))
    u.extend(islice(b, j, None))
    return u, stable


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
