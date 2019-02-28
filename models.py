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
class AddTag(namedtuple('AddTag', 'user_id,movie_id,tag')):
    def apply(self, db):
        db.add_tag(self.user_id, self.movie_id, self.tag)


@register("R")
class RemoveTag(namedtuple('RemoveTag', 'user_id,movie_id,tag')):
    def apply(self, db):
        db.remove_tag(self.user_id, self.movie_id, self.tag)
