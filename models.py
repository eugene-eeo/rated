from collections import namedtuple


def json_to_op(d):
    if d[0] == "Update":
        return Update(*d[1])


class Update(namedtuple('Update', 'user_id,movie_id,value')):
    def apply(self, ratings):
        if self.user_id not in ratings:
            ratings[self.user_id] = {}
        ratings[self.user_id][self.movie_id] = self.value

    def to_json(self):
        return ("Update", self)
