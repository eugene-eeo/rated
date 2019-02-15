import sys
from Pyro4 import Proxy, locateNS
from models import Update

r = Proxy(locateNS().lookup("replica:%s" % sys.argv[1])).get_log()[1]
for u in r:
    u = Update(*u)
    print(u.id, u.ts, u.prev)
