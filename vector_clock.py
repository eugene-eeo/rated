import time
from functools import cmp_to_key


def compare(p1, p2, strict=True):
    v1, t1 = p1
    v2, t2 = p2
    lt = False
    gt = False
    for key in set(v1) | set(v2):
        a = v1.get(key, 0)
        b = v2.get(key, 0)
        lt |= a < b
        gt |= a > b
        if lt and gt:
            break
    # strict = force global ordering
    if not (lt ^ gt) and strict:
        # if lt and gt both = True or False, then compare
        # by timestamp instead
        return 1 if t1 >= t2 else -1
    return gt - lt


sort_key = cmp_to_key(compare)


def greater_than(v1, v2):
    if not v2[0]:
        return True
    return compare(v1, v2) == 1


def merge(p1, p2):
    v1, t1 = p1
    v2, t2 = p2
    v3 = v1.copy()
    for key, value in v2.items():
        v3[key] = max(value, v1.get(key, 0))
    return (v3, max(t1, t2))


def increment(p, id, t):
    v, _ = p
    u = v.copy()
    u[id] = u.get(id, 0) + 1
    return (u, t)


def create():
    return ({}, time.time())
