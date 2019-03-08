import Pyro4

fr = Pyro4.Proxy(Pyro4.locateNS().lookup("frontend"))
for i in range(10):
    while True:
        try:
            fr.add_movie('abc', {'abc', 'def'})
            break
        except RuntimeError:
            pass
