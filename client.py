import Pyro4
import textwrap


def get_confirm(prompt):
    while True:
        c = input(prompt).strip().lower()
        if c:
            if c[0] == 'y':
                return True
            if c[0] == 'n':
                return False


def get_movie_id(prompt, movies):
    order = {i: [id, name] for i, (id,name) in enumerate(movies.items())}
    print("Movies:")
    print("=======")
    for key, (_, name) in order.items():
        print(" [%d] %s" % (key, name))
    while True:
        x = input(prompt).strip()
        try:
            x = int(x)
            if x in order:
                return order[x][0]
            print("Invalid selection")
        except ValueError:
            pass


def get_tag(prompt):
    while True:
        x = input(prompt).strip()
        if x:
            return x


def get_rating(prompt):
    while True:
        x = input(prompt).strip()
        try:
            x = float(x)
            if 0 <= x <= 5:
                return x
            print("Please enter a number between 0 and 5")
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
        self.movies  = frontend.list_movies()
        self.own_data = {"ratings": {}, "tags": {}}

    def help(self):
        print()
        print(" [CM] Create Movie")
        print(" [GM] Get Movie")
        print()
        print(" [V]  View my ratings")
        print(" [AR] Add/update Rating")
        print(" [AT] Add Tag")
        print(" [RR] Remove Rating")
        print(" [RT] Remove Tag")
        print()
        print(" [Q]  Quit")
        print(" [?]  Help")
        print()

    def loop(self):
        self.help()
        while True:
            try:
                option = input("> ").strip().lower()
            except KeyboardInterrupt:
                print()
                break
            try:
                if   option == "v": self.get_user_data()
                elif option == "cm": self.create_movie()
                elif option == "gm": self.get_movie()
                elif option == "at": self.add_tag()
                elif option == "rt": self.remove_tag()
                elif option == "ar": self.add_rating()
                elif option == "rr": self.remove_rating()
                elif option == "?": self.help()
                elif option == "q": break
            except RuntimeError as exc:
                print(" [!] Error: Cannot perform operation:", exc.args[0])
                print(" Please try again later.")

    def select_movie(self):
        self.movies = self.frontend.list_movies()
        return get_movie_id("Movie Selection: ", self.movies)

    def add_tag(self):
        movie_id = self.select_movie()
        tags = set()
        while True:
            tag = get_tag("Add Tag: [Type / to stop] ")
            if tag == '/':
                break
            tags.add(tag)
        self.frontend.add_tag(self.user_id, movie_id, tags)

    def remove_tag(self):
        movie_id = self.select_movie()
        tags = set()
        while True:
            tag = get_tag("Remove Tag: [Type / to stop] ")
            if tag == '/':
                break
            tags.add(tag)
        self.frontend.remove_tag(self.user_id, movie_id, tags)

    def remove_rating(self):
        movie_id = self.select_movie()
        self.frontend.delete_rating(self.user_id, movie_id)

    def add_rating(self):
        movie_id = self.select_movie()
        self.own_data = self.frontend.get_user_data(self.user_id)
        rating = get_rating("Rating: ")
        if movie_id in self.own_data["ratings"]:
            if not get_confirm("Confirm update? [y/n]"):
                return
        self.frontend.add_rating(self.user_id, movie_id, rating)

    def create_movie(self):
        name = get_tag("Movie Name: ")
        self.movies = self.frontend.list_movies(maximal=True)
        words = set(name.lower().split())
        for _, other_name in self.movies.items():
            if words == set(other_name.lower().split()):
                print("Similar movie found:", name)
                if get_confirm("Is this the same? [y/n]"):
                    return

        genres = set()
        while True:
            g = get_tag("Genre: [Type / to stop] ")
            if g == '/':
                break
            genres.add(g)

        self.frontend.add_movie(name, genres)

    def get_movie(self):
        movie_id = self.select_movie()
        movie = self.frontend.get_movie(movie_id)
        print()
        print(movie["name"])
        print("-" * len(movie["name"]))
        print("  Genres: ", ", ".join(movie["genres"]))
        print("  Tags:   ", ", ".join(movie["tags"]))
        print("  Ratings:")
        print("      Avg:", movie["ratings"]["avg"] or "-")
        print("      Min:", movie["ratings"]["min"] or "-")
        print("      Max:", movie["ratings"]["max"] or "-")
        print()

    def get_user_data(self):
        try:
            self.movies = self.frontend.list_movies()
            self.own_data = self.frontend.get_user_data(self.user_id)
        except RuntimeError as exc:
            if exc.args[0] == "Cannot retrieve value!":
                print("Warning: data might be out of date.")
                self.frontend.forget()
            else:
                raise

        data = self.own_data
        keys = set(data["ratings"]) | set(data["tags"])
        if not self.movies or not keys:
            return

        print("Movie                Rating    Tags")
        print("=====                ======    ====")
        for movie_id in sorted(keys):
            rating = "  -   "
            if movie_id in data["ratings"]:
                rating = format(data["ratings"][movie_id], ">6.2f")
            print("{0: <17}    {1}    {2}".format(
                textwrap.shorten(self.movies[movie_id], 17),
                rating,
                ', '.join(data["tags"].get(movie_id, [])),
                ))
        return


def main():
    user_id = get_integer("User ID (Integer): ")
    frontend = Pyro4.Proxy(Pyro4.locateNS().lookup("frontend"))
    Session(frontend, user_id).loop()


if __name__ == '__main__':
    main()
