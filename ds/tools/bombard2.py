from Pyro4 import locateNS, Proxy
fr = Proxy(locateNS().lookup("frontend"))
for _ in range(100):
    while True:
        try:
            fr.add_rating(1, '1', 0)
            break
        except RuntimeError:
            pass
