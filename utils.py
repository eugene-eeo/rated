from base64 import b64encode
from random import shuffle
from uuid import uuid4
from contextlib import contextmanager

from Pyro4.errors import ConnectionClosedError, CommunicationError, TimeoutError


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
