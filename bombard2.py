from Pyro4 import locateNS, Proxy
fr = Proxy(locateNS().lookup("frontend"))
for _ in range(100):
    fr.add_rating(1, 1, 0)
