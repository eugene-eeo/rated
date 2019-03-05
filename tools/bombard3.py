import Pyro4

fr = Pyro4.Proxy(Pyro4.locateNS().lookup("frontend"))
for i in range(100):
    while True:
        try:
            fr.delete_rating(1, 2)
            break
        except RuntimeError:
            pass
