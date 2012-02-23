import bson, struct
import itertools
from bson.errors import InvalidBSON


# Helper functions work working with bson files created using mongodump

def bson_iter(bson_file):
    """
    Takes a file handle to a .bson file and returns an iterator for each
    doc in the file.  This will not load all docs into memory.

    with open('User.bson', 'rb') as bs:
        file_map = mmap.mmap(bs.fileno(), 0, prot=mmap.PROT_READ)
        active_users = filter(bson_iter(file_map), "type", "active")

    """
    while True:
        size_str = bson_file.read(4)
        if not len(size_str):
            break

        obj_size = struct.unpack("<i", size_str)[0]
        obj = bson_file.read(obj_size - 4)
        if obj[-1] != "\x00":
            raise InvalidBSON("bad eoo")
        yield bson._elements_to_dict(obj[:-1], dict, True)

def groupby(iterator, field):
    """
    Returns dictionary with the keys beign the field to group by
    and the values a list of the group docs.

    This is useful for converting a list of docs into dict by _id
    for example.
    """
    groups = {}
    for k, g in itertools.groupby(iterator, lambda x: x.get(field)):
        groups.setdefault(k, []).append(g)
    return groups

def filter(iterator, field, value):
    """
    Takes an iterator and returns only the docs that have a field == value.

    The field can be a nested field like a.b.c and it will descend into the
    embedded documents.
    """
    def deep_get(obj, field, value):
        parts = field.split(".")
        if len(parts) == 1:
            return obj.get(field) == value

        last_value = {}
        for part in parts[0:-1]:
            last_value  = obj.get(part)

        if not last_value:
            return False

        return last_value.get(parts[-1]) == value

    return itertools.ifilter(lambda x: deep_get(x, field, value), iterator)
