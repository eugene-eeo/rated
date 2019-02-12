from functools import cmp_to_key


def compare(v1, v2):
    lt = False
    gt = False
    for key in set(v1) | set(v2):
        a = v1.get(key, 0)
        b = v2.get(key, 0)
        lt |= a < b
        gt |= a > b
        if lt and gt:
            break
    return gt - lt


sort_key = cmp_to_key(compare)


def is_concurrent(v1, v2):
    return v1 != v2 and compare(v1, v2) == 0


def greater_than(v1, v2):
    return compare(v1, v2) == 1


def geq(v1, v2):
    return v1 == v2 or compare(v1, v2) == 1


def merge(v1, v2):
    v3 = v1.copy()
    for key, value in v2.items():
        v3[key] = max(value, v1.get(key, 0))
    return v3


def increment(v, id):
    u = v.copy()
    u[id] = u.get(id, 0) + 1
    return u


def create():
    return {}
