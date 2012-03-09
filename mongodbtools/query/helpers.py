import bson, struct
import itertools
from bson.errors import InvalidBSON


# Helper functions work working with bson files created using mongodump

def bson_iter(bson_file):
    """
    Takes a file handle to a .bson file and returns an iterator for each
    doc in the file.  This will not load all docs into memory.

    with open('User.bson', 'rb') as bs:
        active_users = filter(bson_iter(bs), "type", "active")

    """
    while True:
        size_str = bson_file.read(4)
        if not len(size_str):
            break

        obj_size = struct.unpack("<i", size_str)[0]
        obj = bson_file.read(obj_size - 4)
        if obj[-1] != "\x00":
            raise InvalidBSON("bad eoo")
        yield bson._bson_to_dict(size_str + obj, dict, True)[0]

def _deep_get(obj, field):
    parts = field.split(".")
    if len(parts) == 1:
        return obj.get(field)

    last_value = {}
    for part in parts[0:-1]:
        last_value  = obj.get(part)

    if not last_value:
        return False

    if isinstance(last_value, dict):
        return last_value.get(parts[-1])
    else:
        return getattr(last_value, parts[-1])

def groupby(iterator, field):
    """
    Returns dictionary with the keys beign the field to group by
    and the values a list of the group docs.

    This is useful for converting a list of docs into dict by _id
    for example.
    """
    groups = {}
    for k, g in itertools.groupby(iterator, lambda x: _deep_get(x, field)):
        items = groups.setdefault(k, [])
        for item in g:
            items.append(item)
    return groups

def filter(iterator, field, value):
    """
    Takes an iterator and returns only the docs that have a field == value.

    The field can be a nested field like a.b.c and it will descend into the
    embedded documents.
    """

    return itertools.ifilter(lambda x: _deep_get(x, field) == value, iterator)
