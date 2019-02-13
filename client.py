import Pyro4


def get_rating(prompt):
    while True:
        x = input(prompt).strip()
        try:
            x = float(x)
            if 0 <= x <= 5:
                return x
        except ValueError:
            pass


def get_integer(prompt):
    while True:
        x = input(prompt).strip()
        if x.isnumeric():
            return int(x)


class Session:
    def __init__(self, frontend, user_id):
        self.frontend = frontend
        self.user_id = user_id

    def help(self):
        print("-" * 40)
        print("Help")
        print(" [O] read Other user's ratings")
        print(" [R] Read ratings")
        print(" [U] Update ratings")
        print(" [?] Help")
        print(" [Q] Quit")
        print("-" * 40)

    def loop(self):
        self.help()
        while True:
            option = input("> ").strip().lower()
            if option == "o":
                self.read_other()
            elif option == "r":
                self.read_own()
            elif option == "u":
                self.update_own()
            elif option == "?":
                self.help()
            elif option == "q":
                break

    def read_other(self):
        print("-" * 40)
        print("read Other user's ratings:")
        user_id = get_integer("User ID (Integer): ")
        print("Movie ID     Rating")
        print("========     ======")
        for movie_id, rating in self.frontend.get_ratings(user_id).items():
            print("{0: >8}     {1: >6}".format(movie_id, rating))
        print("-" * 40)

    def read_own(self):
        print("-" * 40)
        print("Read ratings:")
        print("Movie ID     Rating")
        print("========     ======")
        for movie_id, rating in self.frontend.get_ratings(self.user_id).items():
            print("{0: >8}     {1: >6}".format(movie_id, rating))
        print("-" * 40)

    def update_own(self):
        print("-" * 40)
        print("Update ratings:")
        movie_id = get_integer("Movie ID (Integer): ")
        rating   = get_rating("Rating (0-5): ")
        self.frontend.add_rating(self.user_id, movie_id, rating)
        print("-" * 40)


def main():
    user_id = get_integer("User ID (Integer): ")
    frontend = Pyro4.Proxy(Pyro4.locateNS().lookup("frontend"))
    Session(frontend, user_id).loop()


if __name__ == '__main__':
    main()
