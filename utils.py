from base64 import b64encode
from contextlib import contextmanager
from random import shuffle
from operator import attrgetter
from uuid import uuid4
import os
import signal

import Pyro4
from Pyro4.errors import ConnectionClosedError, CommunicationError, TimeoutError
import vector_clock as vc


def unregister_at_exit(name):
    def u():
        Pyro4.locateNS().remove(name)
        os._exit(0)
    signal.signal(signal.SIGTERM, lambda *_: u())
    signal.signal(signal.SIGINT,  lambda *_: u())


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


@contextmanager
def ignore_status_errors():
    try:
        yield
    except RuntimeError as exc:
        if exc.args[0] == 'replica offline':
            return
        raise


def sort_buffer(buffer):
    buffer.sort(key=attrgetter("time", "id"))


def apply_updates(ts, db, executed_ids, executed_uids, log, buffer):
    has_event = True
    while has_event:
        has_event = False
        next_buff = []
        for e in buffer:
            # we've seen this value before, don't execute
            # but merge the timestamps
            if e.id in executed_ids:
                ts = vc.merge(ts, e.ts)
                if (e.id, e.node_id) not in executed_uids:
                    # we've seen our copy (or some copy) of
                    # this update before, so just pretend we've
                    # executed it and put it in the log
                    log.append(e)
                    executed_uids.add((e.id, e.node_id))
                continue
            # if we can apply this update
            if vc.geq(ts, e.prev):
                has_event = True
                e.op.apply(db)
                ts = vc.merge(ts, e.ts)
                executed_ids.add(e.id)
                executed_uids.add((e.id, e.node_id))
                log.append(e)
                continue
            # otherwise we put it in the next buffer
            next_buff.append(e)
        buffer = next_buff
    return ts, buffer
