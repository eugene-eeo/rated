import csv
from collections import namedtuple, defaultdict


__all__ = ('DB', 'Entry', 'update_from_raw')
REGISTRY = {}


class DB:
    def __init__(self):
        self.movies  = {}
        self.ratings = defaultdict(lambda: defaultdict(int))
        self.tags    = defaultdict(lambda: defaultdict(set))

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
            del self.ratings[user_id]

    @staticmethod
    def from_data():
        with open('data/ratings.csv', newline='') as ratings, \
                open('data/movies.csv', newline='') as movies, \
                open('data/tags.csv', newline='') as tags:
            ratings = csv.reader(ratings)
            movies  = csv.reader(movies)
            tags    = csv.reader(tags)
            # skip headers
            next(ratings)
            next(movies)
            next(tags)
            db = DB()
            for id, title, genres in movies:
                db.movies[int(id)] = {
                    "name": title,
                    "genres": genres.split('|'),
                }
            for user_id, movie_id, value, _ in ratings:
                db.ratings[int(user_id)][int(movie_id)] = float(value)
            for user_id, movie_id, tag, _ in tags:
                db.tags[int(user_id)][int(movie_id)].add(tag)
            return db


class Entry(namedtuple('Entry', 'id,op,prev,ts,time')):
    def to_raw(self):
        return (self.id, self.op.to_raw(), self.prev, self.ts, self.time)

    @classmethod
    def from_raw(cls, t):
        e = Entry(*t)
        return Entry(e.id, update_from_raw(e.op), e.prev, e.ts, e.time)


def update_from_raw(raw):
    op, params = raw
    return REGISTRY[op](*params)


def register(tag):
    def decorator(cls):
        REGISTRY[tag] = cls
        def func(self):
            return (tag, self)
        cls.to_raw = func
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
