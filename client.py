import textwrap
import Pyro4
from Pyro4.errors import ConnectionClosedError, CommunicationError, TimeoutError


def similar(a, b):
    # jaccard index
    return len(a & b) / len(a | b)


def get_confirm(prompt):
    while True:
        c = input(prompt).strip().lower()
        if c:
            if c[0] == 'y':
                return True
            if c[0] == 'n':
                return False


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
        print(" [S] Search")
        print(" [CM] Create Movie")
        print(" [GM] Get Movie")
        print()
        print(" [V]  View my ratings")
        print(" [AR] Add/update Rating")
        print(" [RR] Remove Rating")
        print(" [AT] Add Tag")
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
                elif option == "s": self.search_movie()
                elif option == "cm": self.create_movie()
                elif option == "gm": self.get_movie()
                elif option == "ar": self.add_rating()
                elif option == "rr": self.remove_rating()
                elif option == "at": self.add_tag()
                elif option == "rt": self.remove_tag()
                elif option == "?": self.help()
                elif option == "q": break
            except RuntimeError as exc:
                print(" [!] Error: Cannot perform operation:", exc.args[0])
                print(" Please try again later.")
            except (ConnectionError, ConnectionClosedError, CommunicationError, TimeoutError):
                print(" [!] Error: Cannot connect to frontend.")
                print(" [!] Retrying...")
                try:
                    self.frontend = Pyro4.locateNS().lookup("frontend")
                    self.frontend.get_timestamp()
                    print(" OK.")
                    print(" Warning: Data might be stale.")
                except:
                    print(" [!] Error: Cannot reconnect.")
                    print(" [!] Bye.")
                    exit(1)

    def search_movie(self):
        name   = input("Name: ").strip()
        genres = set()
        while True:
            genre = get_tag("Genre: [Type / to stop] ")
            if genre == '/':
                break
            genres.add(genre)
        movies = self.frontend.search(name, genres)
        print("ID      Movie                            Genres")
        print("==      =====                            ======")
        for movie_id, movie in movies.items():
            print("{0: <6}  {1: <29}    {2}".format(
                movie_id,
                textwrap.shorten(movie["name"], 29, placeholder='...'),
                ', '.join(movie["genres"]),
                ))

    def add_tag(self):
        movie_id = get_integer("Movie ID: ")
        tags = set()
        while True:
            tag = get_tag("Add Tag: [Type / to stop] ")
            if tag == '/':
                break
            tags.add(tag)
        self.frontend.add_tag(self.user_id, movie_id, tags)

    def remove_tag(self):
        movie_id = get_integer("Movie ID: ")
        tags = set()
        while True:
            tag = get_tag("Remove Tag: [Type / to stop] ")
            if tag == '/':
                break
            tags.add(tag)
        self.frontend.remove_tag(self.user_id, movie_id, tags)

    def remove_rating(self):
        movie_id = get_integer("Movie ID: ")
        self.frontend.delete_rating(self.user_id, movie_id)

    def add_rating(self):
        movie_id = get_integer("Movie ID: ")
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
            if similar(words, set(other_name.lower().split())) > 0.5:
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
        movie_id = get_integer("Movie ID: ")
        movie = self.frontend.get_movie(movie_id)
        if not movie:
            return
        print()
        print(movie["name"])
        print("-" * len(movie["name"]))
        print("  Genres: ", ", ".join(movie["genres"]))
        print("  Tags:   ", ", ".join(movie["tags"]))
        print("  Ratings: (%d)" % movie["ratings"]["len"])
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
                print("  [!] Warning: data might be out of date.")
                self.frontend.forget()
            else:
                raise

        data = self.own_data
        keys = set(data["ratings"]) | set(data["tags"])
        if not self.movies or not keys:
            return

        print("ID      Movie                            Rating  Tags")
        print("==      =====                            ======  ====")
        for movie_id in sorted(keys):
            rating = "  -   "
            if movie_id in data["ratings"]:
                rating = format(data["ratings"][movie_id], ">6.2f")
            print("{0: <6}  {1: <29}    {2}  {3}".format(
                movie_id,
                textwrap.shorten(self.movies[movie_id], 29, placeholder='...'),
                rating,
                ', '.join(data["tags"].get(movie_id, [])),
                ))


def main():
    user_id = get_integer("User ID (Integer): ")
    try:
        frontend = Pyro4.Proxy(Pyro4.locateNS().lookup("frontend"))
    except:
        print(" [!] Error: Cannot connect to frontend.")
        print(" [!] Bye.")
        exit(1)
    Session(frontend, user_id).loop()


if __name__ == '__main__':
    main()
