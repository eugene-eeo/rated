from random import shuffle
from uuid import uuid4
from itertools import islice
from vector_clock import compare


def generate_id():
    return uuid4().hex


def merge(a, b):
    # Merges two sorted event logs together
    i = 0
    j = 0
    while i < len(a) and j < len(b):
        x = a[i]
        y = b[j]
        # duplicate event
        if x == y:
            yield x
            i += 1
            j += 1
            continue
        c = compare(x[0], y[0])
        # don't need to handle c == 0 because
        # c == 0 never happens under strict comparison
        if c == -1: yield x; i += 1  # x < y
        if c == +1: yield y; j += 1  # x > y
    # one of the sequences must be empty
    yield from islice(a, i, None)
    yield from islice(b, j, None)


def find_random_peers(ns, id, metadata):
    peers = ns.list(metadata_all={metadata})
    choices = [uri for name, uri in peers.items() if id not in name]
    shuffle(choices)
    return choices
