import csv
from collections import namedtuple, defaultdict


REGISTRY = {}


class DB:
    def __init__(self):
        self.movies = {}
        self.ratings = defaultdict(lambda: defaultdict(int))
        self.tags = defaultdict(lambda: defaultdict(set))

    def clear(self):
        self.movies.clear()
        self.ratings.clear()
        self.tags.clear()

    def update_movie(self, movie_id, data):
        self.movies[movie_id] = data

    def add_tag(self, user_id, movie_id, tag):
        self.tags[user_id][movie_id].add(tag)

    def remove_tag(self, user_id, movie_id, tag):
        self.tags[user_id][movie_id].discard(tag)

    def update_rating(self, user_id, movie_id, value):
        self.ratings[user_id][movie_id] = value

    def delete_rating(self, user_id, movie_id):
        if movie_id in self.ratings[user_id]:
            del self.ratings[user_id][movie_id]

    @staticmethod
    def from_data():
        with open('data/ratings.csv', newline='') as ratings, \
                open('data/movies.csv', newline='') as movies, \
                open('data/tags.csv', newline='') as tags:
            ratings = csv.reader(ratings)
            movies = csv.reader(movies)
            tags = csv.reader(tags)
            # skip headers
            next(ratings)
            next(movies)
            next(tags)
            db = DB()
            for id, title, genres in movies:
                db.movies[id] = {
                    "name": title,
                    "genres": genres.split('|'),
                }
            for user_id, movie_id, value, _ in ratings:
                db.ratings[int(user_id)][movie_id] = float(value)
            for user_id, movie_id, tag, _ in tags:
                db.tags[int(user_id)][movie_id].add(tag)
            return db


# Entry => some 'update' operation sent to a replica
# contains the entry ID, node ID, operation, causal dependency, logical
# timestamp, and physical timestamp
#
# We don't need `time` to establish a strict ordering, but
# using `time` is better than using `id` or `node_id` to avoid
# the entries from jumping around too much.
#
class Entry(namedtuple('Entry', 'id,node_id,op,prev,ts,time')):
    def to_raw(self):
        return (self.id, self.node_id, self.op.to_raw(), self.prev, self.ts, self.time)

    @classmethod
    def from_raw(cls, t):
        e = Entry(*t)
        return Entry(e.id, e.node_id, op_from_raw(e.op), e.prev, e.ts, e.time)


def op_from_raw(raw):
    op, params = raw
    return REGISTRY[op](*params)


def register(tag):
    def decorator(cls):
        REGISTRY[tag] = cls

        def to_raw(self):
            return (tag, self)
        cls.to_raw = to_raw
        return cls
    return decorator


@register("U")
class Update(namedtuple('Update', 'user_id,movie_id,value')):
    def apply(self, db):
        db.update_rating(self.user_id, self.movie_id, self.value)


@register("D")
class Delete(namedtuple('Delete', 'user_id,movie_id')):
    def apply(self, db):
        db.delete_rating(self.user_id, self.movie_id)


@register("M")
class UpdateMovie(namedtuple('UpdateMovie', 'movie_id,data')):
    def apply(self, db):
        db.update_movie(self.movie_id, self.data)


@register("A")
class AddTag(namedtuple('AddTag', 'user_id,movie_id,tags')):
    def apply(self, db):
        for tag in self.tags:
            db.add_tag(self.user_id, self.movie_id, tag)


@register("R")
class RemoveTag(namedtuple('RemoveTag', 'user_id,movie_id,tags')):
    def apply(self, db):
        for tag in self.tags:
            db.remove_tag(self.user_id, self.movie_id, tag)
