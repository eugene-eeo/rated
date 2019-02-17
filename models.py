from vector_clock import decrement
from collections import namedtuple


class Update(namedtuple('Update', 'id,user_id,movie_id,value,prev,ts,time')):
    def apply(self, ratings):
        if self.user_id not in ratings:
            ratings[self.user_id] = {}
        ratings[self.user_id][self.movie_id] = self.value
