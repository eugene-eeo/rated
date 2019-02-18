from utils import sort_buffer, apply_updates
from models import Update

x = [
    Update('a', 1, 1, 1, {}, {'x': 1}, 0),
    Update('b', 1, 1, 1, {}, {'y': 1}, 0),
    Update('c', 1, 1, 1, {"x": 1, 'y': 1}, {'y': 2}, 0),
    Update('d', 1, 1, 1, {"x": 1, 'y': 1}, {'x': 2}, 0),
]
