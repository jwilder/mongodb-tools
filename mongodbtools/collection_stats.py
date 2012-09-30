#!/usr/bin/env python

"""
This script prints some basic collection stats about the size of the
collections and their indexes.
"""

from prettytable import PrettyTable
import psutil
from pymongo import Connection
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

    (options, args) = parser.parse_args()
        
    return options

def get_connection(host, port): 
    return Connection(host, port, read_preference=ReadPreference.SECONDARY)

# From http://www.5dollarwhitebox.org/drupal/node/84
def convert_bytes(bytes):
    bytes = float(bytes)
    if bytes >= 1099511627776:
        terabytes = bytes / 1099511627776
        size = '%.2fT' % terabytes
    elif bytes >= 1073741824:
        gigabytes = bytes / 1073741824
        size = '%.2fG' % gigabytes
    elif bytes >= 1048576:
        megabytes = bytes / 1048576
        size = '%.2fM' % megabytes
    elif bytes >= 1024:
        kilobytes = bytes / 1024
        size = '%.2fK' % kilobytes
    else:
        size = '%.2fb' % bytes
    return size

def main(options):
    summary_stats = {
        "count" : 0,
        "size" : 0,
        "indexSize" : 0
    }
    all_stats = []
    
    connection = get_connection(options.host, options.port)

    all_db_stats = {}
    
    if options.database: 
        databases.append(options.database)
    else:
        databases = connection.database_names()
    
    for db in databases:
        # FIXME: Add an option to include oplog stats.
        if db == "local":
            continue

        database = connection[db]
        all_db_stats[database.name] = []
        for collection_name in database.collection_names():
            stats = get_collection_stats(database, database[collection_name])
            all_stats.append(stats)
            all_db_stats[database.name].append(stats)

            summary_stats["count"] += stats["count"]
            summary_stats["size"] += stats["size"]
            summary_stats["indexSize"] += stats.get("totalIndexSize", 0)

    x = PrettyTable(["Collection", "Count", "% Size", "DB Size", "Avg Obj Size", "Indexes", "Index Size"])
    x.set_field_align("Collection", "l")
    x.set_field_align("% Size", "r")
    x.set_field_align("Count", "r")
    x.set_field_align("DB Size", "r")
    x.set_field_align("Avg Obj Size", "r")
    x.set_field_align("Index Size", "r")
    x.set_padding_width(1)

    print

    for db in all_db_stats:
        db_stats = all_db_stats[db]
        count = 0
        for stat in db_stats:
            count += stat["count"]
            x.add_row([stat["ns"], stat["count"], "%0.1f%%" % ((stat["size"] / float(summary_stats["size"])) * 100),
                       convert_bytes(stat["size"]),
                       convert_bytes(stat.get("avgObjSize", 0)),
                       stat.get("nindexes", 0),
                       convert_bytes(stat.get("totalIndexSize", 0))])

    print
    x.printt(sortby="% Size")
    print "Total Documents:", summary_stats["count"]
    print "Total Data Size:", convert_bytes(summary_stats["size"])
    print "Total Index Size:", convert_bytes(summary_stats["indexSize"])

    # this is only meaningful if we're running the script on localhost
    if options.host == "localhost":
        ram_headroom = psutil.phymem_usage()[0] - summary_stats["indexSize"]
        print "RAM Headroom:", convert_bytes(ram_headroom)
        print "RAM Used: %s (%s%%)" % (convert_bytes(psutil.phymem_usage()[1]), psutil.phymem_usage()[3])
        print "Available RAM Headroom:", convert_bytes((100 - psutil.phymem_usage()[3]) / 100 * ram_headroom)

if __name__ == "__main__":
    options = get_cli_options()
    main(options)