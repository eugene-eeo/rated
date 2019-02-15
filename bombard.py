import sys
import random
import time
from Pyro4 import Proxy, locateNS

r = Proxy(locateNS().lookup("replica:%s" % sys.argv[1]))
t = {}
for i in range(1000):
    t = r.update((1, 1, random.choice([0, 1])), t)
    time.sleep(random.random() / 100)
