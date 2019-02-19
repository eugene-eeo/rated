from vector_clock import decrement
from collections import namedtuple


class Entry(namedtuple('Entry', 'id,op,prev,ts,time')):
    def to_raw(self):
        return (self.id, self.op.to_raw(), self.prev, self.ts, self.time)

    @classmethod
    def from_raw(cls, t):
        e = Entry(*t)
        r = None
        op, params = e.op
        if op[0] == "U": r = Update(*params)
        if op[0] == "D": r = Delete(*params)
        return Entry(e.id, r, e.prev, e.ts, e.time)


class Update(namedtuple('Update', 'user_id,movie_id,value')):
    def to_raw(self):
        return ("U", self)

    def apply(self, ratings):
        if self.user_id not in ratings:
            ratings[self.user_id] = {}
        ratings[self.user_id][self.movie_id] = self.value


class Delete(namedtuple('Delete', 'user_id,movie_id')):
    def to_raw(self):
        return ("D", self)

    def apply(self, ratings):
        if self.user_id in ratings:
            if self.movie_id in ratings[self.user_id]:
                del ratings[self.user_id][self.movie_id]
