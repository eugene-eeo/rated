import sys
import json
from Pyro4 import Proxy, locateNS


r = Proxy(locateNS().lookup("replica:%s" % sys.argv[1])).get_log()[1]
m = min(u[-1] for u in r) if r else 0
for u in r:
    print(json.dumps({
        "id":   u[0],
        "prev": u[-3],
        "ts":   u[-2],
        "time": u[-1],
        }, sort_keys=True))
