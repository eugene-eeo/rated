import pprint
import sys
from Pyro4 import Proxy, locateNS


db = Proxy(locateNS().lookup("replica:%s" % sys.argv[1])).get_state()[1]

# convert to JSON friendly output
for movie in db["movies"].values():
    movie["genres"] = list(sorted(movie["genres"]))
for user_tags in db["tags"].values():
    for user_id, tags in user_tags.items():
        user_tags[user_id] = list(sorted(tags))

pprint.pprint(db, width=100)
