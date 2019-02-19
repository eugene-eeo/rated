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
        print("Help")
        print(" [L] List ratings")
        print(" [C] Create/update rating")
        print(" [R] Read other's ratings")
        print(" [G] Get aggregated rating")
        print(" [D] Delete rating")
        print(" [Q] Quit")
        print(" [?] Help")

    def loop(self):
        self.help()
        while True:
            option = input("> ").strip().lower()
            if   option == "l": self.list_ratings(self.user_id)
            elif option == "c": self.create_rating()
            elif option == "d": self.delete_rating()
            elif option == "r": self.read_ratings()
            elif option == "g": self.get_aggregated_rating()
            elif option == "?": self.help()
            elif option == "q": break

    def get_aggregated_rating(self):
        movie_id = get_integer("Movie ID: ")
        for key, val in self.frontend.get_aggregated_rating(movie_id).items():
            print("{0}  {1:.2f}".format(key.capitalize(), val))

    def delete_rating(self):
        self.list_ratings(self.user_id)
        movie_id = get_integer("Movie ID: ")
        self.frontend.delete_rating(self.user_id, movie_id)

    def read_ratings(self):
        user_id = get_integer("User ID: ")
        self.list_ratings(user_id)

    def create_rating(self):
        self.list_ratings(self.user_id)
        movie_id = get_integer("Movie ID: ")
        rating = get_rating("Rating: ")
        self.frontend.add_rating(self.user_id, movie_id, rating)

    def list_ratings(self, id):
        ratings = self.frontend.get_ratings(id)
        print("Movie ID    Rating")
        print("========    ======")
        for movie_id in sorted(ratings):
            print("{0: >8}    {1: >6.2f}".format(movie_id, ratings[movie_id]))
        return ratings


def main():
    user_id = get_integer("User ID (Integer): ")
    frontend = Pyro4.Proxy(Pyro4.locateNS().lookup("frontend"))
    Session(frontend, user_id).loop()


if __name__ == '__main__':
    main()
