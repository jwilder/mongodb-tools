#!/usr/bin/env python

"""
This script prints some basic collection stats about the size of the
collections and their indexes.
"""

from prettytable import PrettyTable
import psutil
from pymongo import MongoClient
from pymongo import ReadPreference
from optparse import OptionParser

def compute_signature(index):
    signature = index["ns"]
    for key in index["key"]:
        signature += "%s_%s" % (key, index["key"][key])
    return signature

def get_collection_stats(database, collection):
    print "Checking DB: %s" % collection.full_name
    return database.command("collstats", collection.name)

# From http://www.5dollarwhitebox.org/drupal/node/84
def convert_bytes(bytes):
    bytes = float(bytes)
    magnitude = abs(bytes)
    if magnitude >= 1099511627776:
        terabytes = bytes / 1099511627776
        size = '%.2fT' % terabytes
    elif magnitude >= 1073741824:
        gigabytes = bytes / 1073741824
        size = '%.2fG' % gigabytes
    elif magnitude >= 1048576:
        megabytes = bytes / 1048576
        size = '%.2fM' % megabytes
    elif magnitude >= 1024:
        kilobytes = bytes / 1024
        size = '%.2fK' % kilobytes
    else:
        size = '%.2fb' % bytes
    return size

def get_cli_options():
    parser = OptionParser(usage="usage: python %prog [options]",
                          description="""This script prints some basic collection stats about the size of the collections and their indexes.""")

    parser.add_option("-H", "--host",
                      dest="host",
                      default="localhost",
                      metavar="HOST",
                      help="MongoDB host")
    parser.add_option("-p", "--port",
                      dest="port",
                      default=27017,
                      metavar="PORT",
                      help="MongoDB port")
    parser.add_option("-d", "--database",
                      dest="database",
                      default="",
                      metavar="DATABASE",
                      help="Target database to generate statistics. All if omitted.")
    parser.add_option("-u", "--user",
                      dest="user",
                      default="",
                      metavar="USER",
                      help="Admin username if authentication is enabled")
    parser.add_option("--password",
                      dest="password",
                      default="",
                      metavar="PASSWORD",
                      help="Admin password if authentication is enabled")

    (options, args) = parser.parse_args()

    return options

def get_client(host, port, username, password):
    userPass = ""
    if username and password:
        userPass = username + ":" + password + "@"

    mongoURI = "mongodb://" + userPass + host + ":" + str(port)
    return MongoClient(mongoURI)

def main(options):
    summary_stats = {
        "count" : 0,
        "size" : 0,
        "indexSize" : 0
    }
    all_stats = []

    client = get_client(options.host, options.port, options.user, options.password)

    all_db_stats = {}

    databases = []
    if options.database:
        databases.append(options.database)
    else:
        databases = client.database_names()

    for db in databases:
        # FIXME: Add an option to include oplog stats.
        if db == "local":
            continue

        database = client[db]
        all_db_stats[database.name] = []
        for collection_name in database.collection_names():
            stats = get_collection_stats(database, database[collection_name])
            all_stats.append(stats)
            all_db_stats[database.name].append(stats)

            summary_stats["count"] += stats["count"]
            summary_stats["size"] += stats["size"]
            summary_stats["indexSize"] += stats.get("totalIndexSize", 0)

    x = PrettyTable(["Collection", "Index","% Size", "Index Size"])
    x.align["Collection"] = "l"
    x.align["Index"] = "l"
    x.align["% Size"] = "r"
    x.align["Index Size"] = "r"
    x.padding_width = 1

    print

    index_size_mapping = {}
    for db in all_db_stats:
        db_stats = all_db_stats[db]
        count = 0
        for stat in db_stats:
            count += stat["count"]
            for index in stat["indexSizes"]:
                index_size = stat["indexSizes"].get(index, 0)
                row = [stat["ns"], index,
                          "%0.1f%%" % ((index_size / float(summary_stats["indexSize"])) * 100),
                  convert_bytes(index_size)]
                index_size_mapping[index_size] = row
                x.add_row(row)


    print "Index Overview"
    print x.get_string(sortby="Collection")

    print
    print "Top 5 Largest Indexes"
    x = PrettyTable(["Collection", "Index","% Size", "Index Size"])
    x.align["Collection"] = "l"
    x.align["Index"] = "l"
    x.align["% Size"] = "r"
    x.align["Index Size"] = "r"
    x.padding_width = 1

    top_five_indexes = sorted(index_size_mapping.keys(), reverse=True)[0:5]
    for size in top_five_indexes:
        x.add_row(index_size_mapping.get(size))
    print x
    print

    print "Total Documents:", summary_stats["count"]
    print "Total Data Size:", convert_bytes(summary_stats["size"])
    print "Total Index Size:", convert_bytes(summary_stats["indexSize"])

    # this is only meaningful if we're running the script on localhost
    if options.host == "localhost":
        ram_headroom = psutil.virtual_memory().total - summary_stats["indexSize"]
        print "RAM Headroom:", convert_bytes(ram_headroom)
        print "RAM Used: %s (%s%%)" % (convert_bytes(psutil.virtual_memory().used), psutil.virtual_memory().percent)
        print "Available RAM Headroom:", convert_bytes((100 - psutil.virtual_memory().percent) / 100 * ram_headroom)

if __name__ == "__main__":
    options = get_cli_options()
    main(options)
