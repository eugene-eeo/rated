import Pyro4

fr = Pyro4.Proxy(Pyro4.locateNS().lookup("frontend"))
for i in range(100):
    fr.add_rating_sync(1, 2, i)
