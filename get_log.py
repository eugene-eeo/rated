import sys
from Pyro4 import Proxy, locateNS
from models import Update


def d_sort(v):
    # python 3: dicts preserve key insertion
    # order so use this to our advantage
    d = {}
    for k in sorted(v.keys()):
        d[k] = v[k]
    return d


r = Proxy(locateNS().lookup("replica:%s" % sys.argv[1])).get_log()[1]
m = min(u[-1] for u in r) if r else 0
for u in r:
    u = Update(*u)
    print(u.id, d_sort(u.ts), format(u.time - m, ".2f"))
