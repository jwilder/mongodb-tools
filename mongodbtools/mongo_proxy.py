#!/usr/bin/env python
"""
This is a MongoDB proxy that works at the level of the MongoDB wire protocol [1].  It will log
every request and response sent to MongoDB.  Because this work at the wire protocol level,
it should be compatible with all existing MongoDB drivers.

[1] http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol
"""
from collections import namedtuple

import json
from optparse import OptionParser
import time
import bson
import gevent
from gevent.server import StreamServer
from gevent import socket as gsocket
import struct
import logging
from gevent.monkey import patch_all
from log_format import ColorFormatter

patch_all(thread=False)

CONNECTIONS = {}

class RequestFilter(logging.Filter):
    def filter(self, record):

        connection_data = CONNECTIONS.get(gevent.getcurrent(), {})
        address = connection_data.get("address")
        if address:
            record.connection = "%s:%s" % address
        else:
            record.connection = ""
            record.op = ""
        return True

logging.basicConfig(level=logging.DEBUG)


class BsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (bson.ObjectId, bson.DBRef, bson.Timestamp)):
            return str(obj)
        else:
            return json.JSONEncoder.default(self, obj)


logging.getLogger().addFilter(RequestFilter())
logging.getLogger().handlers[0].setFormatter(
    ColorFormatter('%(asctime)s %(levelname)-1s %(connection)s %(message)s'))

OP_REPLY = 1
OP_MSG = 1000
OP_UPDATE = 2001
OP_INSERT = 2002
OP_RESERVED = 2003
OP_QUERY = 2004
OP_GETMORE = 2005
OP_DELETE = 2006
OP_KILL_CURSORS = 2007

OP_NAMES = {
    OP_REPLY: "REPLY",
    OP_MSG: "MSG",
    OP_UPDATE: "UPDATE",
    OP_INSERT: "INSERT",
    OP_RESERVED: "RESERVED",
    OP_QUERY: "QUERY",
    OP_GETMORE: "GET_MORE",
    OP_DELETE: "DELETE",
    OP_KILL_CURSORS: "KILL_CURSOR"
}

MessageHeader = namedtuple('MessageHeader', 'length, request_id, response_to, operation')
QueryMessage = namedtuple('QueryMessage', 'header, collection, flags, skip, to_return, query, fields')
ReplyMessage = namedtuple('ReplyMessage', 'header, flags, cursor_id, starting_from, number_returned, documents')
InsertMessage = namedtuple('InsertMessage', 'header, flags, collection, documents')
MoreMessage = namedtuple('MoreMessage', 'header, collection, to_return, cursor_id')
UpdateMessage = namedtuple('UpdateMessage', 'header, collection, flags, selector, update')
DeleteMessage = namedtuple('DeleteMessage', 'header, collection, flags, selector')

class MongoListener(object):

    def on_open(self, address):
        pass

    def on_close(self, address):
        pass

    def before_query(self, msg):
        pass

    def after_query(self, msg):
        pass

    def before_query_send(self, msg):
        pass

    def after_query_send(self, msg):
        pass

    def before_query_reply(self, msg):
        pass

    def after_query_reply(self, msg):
        pass

    def before_reply(self, msg):
        pass

    def after_reply(self, msg):
        pass

    def before_insert(self, msg):
        pass

    def after_insert(self, msg):
        pass

    def before_more(self, msg):
        pass

    def after_more(self, msg):
        pass

    def before_more_send(self, msg):
        pass

    def after_more_send(self, msg):
        pass

    def before_more_reply(self, msg):
        pass

    def after_more_reply(self, msg):
        pass

    def before_update(self, msg):
        pass

    def after_update(self, msg):
        pass

    def before_delete(self, msg):
        pass

    def after_delete(self, msg):
        pass

class RawLoggingListener(MongoListener):
    def before_query(self, msg):
        flags = msg.flags
        flag_strs = []
        if flags & 1 == 1:
            flag_strs.append("TAILABLE")

        if flags & 4 == 4:
            flag_strs.append("SLAVEOK")

        if flags & 8 == 8:
            flag_strs.append("OPLOG-REPLAY")

        if flags & 16 == 16:
            flag_strs.append("NOTIMEOUT")

        if flags & 32 == 32:
            flag_strs.append("AWAIT")

        if flags & 64 == 64:
            flag_strs.append("EXHAUST")

        if flags & 128 == 128:
            flag_strs.append("PARTIAL")

        logging.info("%s QUERY %s flags=[%s] skip=%s limit=%s selector=%s fields=%s" % (
            msg.header.request_id,
            msg.collection, "|".join(flag_strs), msg.skip, msg.to_return, msg.query, msg.fields))

    def before_reply(self, msg):
        flags = msg.flags
        flag_strs = []
        if flags & 1 == 1:
            flag_strs.append("NOTFOUND")

        if flags & 2 == 2:
            flag_strs.append("FAILURE")

        if flags & 4 == 4:
            flag_strs.append("CFG-STATE")

        if flags & 8 == 8:
            flag_strs.append("AWAIT-CAPABLE")

        logging.info("%s REPLY flags=[%s] id=%s from=%s count=%s %s" % (
            msg.header.response_to,
            "|".join(flag_strs), msg.cursor_id,
            msg.starting_from, msg.number_returned, msg.documents))

    def before_insert(self, msg):
        flags = msg.flags
        flag_strs = []
        if flags & 1 == 1:
            flag_strs.append("CONTINUE")

        logging.info("%s INSERT %s flags=[%s] %s" % (msg.header.request_id, msg.collection, "|".join(flag_strs),
                                                  msg.documents if len(msg.documents) == 1 else len(msg.documents)))

    def before_more(self, msg):
        logging.info("%s GETMORE %s %s %s" % (
            msg.header.request_id,
            msg.collection, msg.to_return, msg.cursor_id))

    def before_update(self, msg):
        flags = msg.flags
        flag_strs = []
        if flags & 1 == 1:
            flag_strs.append("UPSERT")

        if flags & 2 == 2:
            flag_strs.append("MULTI")

        logging.info("%s UPDATE flags=[%s] %s %s %s" % (
            msg.header.request_id,
            "|".join(flag_strs),
            msg.collection,
            msg.selector, msg.update))

    def before_delete(self, msg):
        logging.info("%s DELETE %s %s" % (msg.header.request_id, msg.collection, msg.selector))

class TimingListener(MongoListener):


    def __init__(self):
        super(TimingListener, self).__init__()
        self.requests = {}
        self.stats = {}

    def on_open(self, address):
        self.stats[address] = {}

    def on_close(self, address):
        del self.stats[address]

    def _before(self, msg):
        self.requests[msg.header.request_id] = time.time()

    def _json(self, msg):
        try:
            return json.dumps(msg, cls=BsonEncoder)
        except Exception, e:
            logging.warning(e)
            return "{..<error>..}"

    def _after(self, msg):
        db = msg.collection[0:msg.collection.index(".")]
        col = msg.collection[len(db)+1:]
        query = ""
        if msg.header.operation == OP_QUERY:

            selector = msg.query.get("$query", None)
            if selector is None:
                selector = msg.query
            query = "$GREENdb.%s.find(%s)" % (col, self._json(selector))

            if msg.to_return > 0:
                query += ".limit(%s)" % msg.to_return

            order_by = msg.query.get("$orderby")
            if order_by:
                query += ".sort(%s)" % self._json(order_by)

            query += "$RESET"
        elif msg.header.operation == OP_GETMORE:
            query = "$REDdb.%s.MORE(%s)$BOLD" % (col, self._json(msg.documents))

        elif msg.header.operation == OP_INSERT:
            query = "$REDdb.%s.insert(%s)$BOLD" % (col, self._json(msg.documents))
        elif msg.header.operation == OP_UPDATE:

            selector_json = self._json(msg.selector)
            update_json = self._json(msg.update)

            query = "$REDdb.%s.update(%s, %s)$RESET" % (col, selector_json,
                                              update_json)
        elif msg.header.operation == OP_DELETE:
            query = "$REDdb.%s.remove(%s)$RESET" % (col, self._json(msg.selector))

        logging.info("%s [%s] %s: $BOLD%.03fms$RESET" % (msg.header.request_id,
                                           db, query,
                                           time.time() - self.requests[msg.header.request_id]))
        del self.requests[msg.header.request_id]


    def before_update(self, msg):
        self._before(msg)

    def before_insert(self, msg):
        self._before(msg)

    def before_delete(self, msg):
        self._before(msg)

    def before_more(self, msg):
        self._before(msg)

    def before_query(self, msg):
        self._before(msg)

    def after_query(self, msg):
        self._after(msg)

    def after_insert(self, msg):
        self._after(msg)

    def after_delete(self, msg):
        self._after(msg)

    def after_update(self, msg):
        self._after(msg)


class MongoProxy(object):

    def __init__(self, options):
        super(MongoProxy, self).__init__()
        self.options = options
        self.listeners = []

    def add_listener(self, listener):
        if listener not in self.listeners:
            self.listeners.append(listener)

    def _invoke_callback(self, fname, msg):
        for listener in self.listeners:
            try:
                getattr(listener, fname)(msg)
            except Exception, e:
                logging.exception(e)

    def extract_header(self, resp):
        if len(resp) != 16:
            raise Exception("Bad header")
        length, request_id, response_to, operation =  struct.unpack("<iiii", resp[0:16])
        return MessageHeader(length, request_id, response_to, operation)


    def _extract_cstring(self, buf, ns_end):
        while ord(buf[ns_end]) and ns_end < len(buf):
            ns_end += 1
        return ns_end


    def handle_delete(self, header, buf, s):
        ns_end = 20
        ns_end = self._extract_cstring(buf, ns_end)
        namespace = buf[20:ns_end]
        flags = struct.unpack("<i", buf[ns_end:ns_end + 4])[0]
        selector = bson.decode_all(buf[ns_end + 4 + 1:])

        msg = DeleteMessage(header, namespace, flags, selector)

        self._invoke_callback("before_delete", msg)
        s.sendall(buf)
        self._invoke_callback("after_delete", msg)

    def handle_insert(self, header, buf, s):

        flags = struct.unpack("<i", buf[16:20])[0]
        ns_end = self._extract_cstring(buf, 20)
        namespace = buf[20:ns_end]
        documents = bson.decode_all(buf[ns_end+1:])

        msg = InsertMessage(header, flags, namespace, documents)

        self._invoke_callback("before_insert", msg)
        s.sendall(buf)
        self._invoke_callback("after_insert", msg)

    def handle_update(self,header, buf, s):
        ns_end = 20
        ns_end = self._extract_cstring(buf, ns_end)
        namespace = buf[20:ns_end]
        flags = struct.unpack("<i", buf[ns_end:ns_end+4])[0]

        selector_fields = bson.decode_all(buf[ns_end + 4 + 1:])
        selector = selector_fields[0]
        update = {}
        if len(selector_fields) == 2:
            update = selector_fields[1]

        msg = UpdateMessage(header, namespace, flags, selector, update)
        self._invoke_callback("before_update", msg)

        s.sendall(buf)
        self._invoke_callback("after_update", msg)

    def handle_query(self, header, buf, s, socket):

        flags = struct.unpack("<i", buf[16:20])[0]
        ns_end = self._extract_cstring(buf, 20)
        namespace = buf[20:ns_end]
        ns_end += 1
        skip = struct.unpack("<i", buf[ns_end:ns_end + 4])[0]
        count = struct.unpack("<i", buf[ns_end+4:ns_end + 8])[0]
        selector_fields = bson.decode_all(buf[ns_end + 8:])
        selector = selector_fields[0]
        fields = {}
        if len(selector_fields) == 2:
            fields = selector_fields[1]

        msg = QueryMessage(header, namespace, flags, skip, count, selector, fields)

        self._invoke_callback("before_query", msg)

        self._invoke_callback("before_query_send", msg)
        s.sendall(buf)
        self._invoke_callback("after_query_send", msg)

        self._invoke_callback("before_query_reply", msg)
        self.handle_reply(s, socket)
        self._invoke_callback("after_query_reply", msg)

        self._invoke_callback("after_query", msg)
        return buf


    def handle_reply(self, s, socket):

        resp = s.recv(16)
        header = self.extract_header(resp)

        stats = CONNECTIONS[gevent.getcurrent()]["stats"]
        tot = stats.setdefault(header.operation, 0)
        stats[header.operation] = tot + 1

        resp += s.recv(header.length - 16)
        while len(resp) < header.length:
            resp += s.recv(header.length - len(resp))

        r_flags, r_cursors_id, r_from, r_count = struct.unpack("<iqii", resp[16:36])
        docs = bson.decode_all(resp[36:])

        msg = ReplyMessage(header, r_flags, r_cursors_id, r_from, r_count, docs)

        self._invoke_callback('before_reply', msg)
        socket.sendall(resp)
        self._invoke_callback('after_reply', msg)


    def handle_more(self, header, buf, s, socket):

        ns_end = self._extract_cstring(buf, 20)
        namespace = buf[20:ns_end]
        ns_end += 1
        count = struct.unpack("<i", buf[ns_end:ns_end + 4])[0]
        cursor_id = struct.unpack("<q", buf[ns_end + 4:ns_end + 12])[0]

        msg = MoreMessage(header, namespace, count, cursor_id)

        self._invoke_callback("before_more", msg)

        self._invoke_callback("before_more_send", msg)
        s.sendall(buf)
        self._invoke_callback("after_more_send", msg)


        self._invoke_callback("before_more_reply", msg)
        self.handle_reply(s, socket)
        self._invoke_callback("after_more_reply", msg)

        self._invoke_callback("after_more", msg)
        return buf


    def log_stats(self, stats):
        counts = stats.copy()
        del counts["time"]
        summary = [OP_NAMES[x] + "=" + str(stats[x]) for x in sorted(counts.keys())]
        total = sum(counts.values())
        summary.append("TOTAL=" + str(total))
        read_total = sum([counts.get(x, 0) for x in [OP_QUERY, OP_GETMORE, OP_REPLY]])
        write_total = sum([counts.get(x, 0) for x in [OP_DELETE, OP_INSERT, OP_UPDATE, OP_KILL_CURSORS]])
        summary.insert(0, "R %0.1f%% / W %0.1f%%" % (
                           float(read_total) / total * 100, float(write_total) / total * 100))
        logging.info("Summary: " + " ".join(summary)+" $BOLD%0.2fms$RESET" % (time.time() - stats["time"]))


    def proxy(self, socket, address):

        CONNECTIONS[gevent.getcurrent()] = {"address": address,
                                            "stats": {}}

        self._invoke_callback('on_open', address)
        logging.info("New Connection from %s:%s" % address)
        stats = CONNECTIONS[gevent.getcurrent()]["stats"]
        stats["time"] = time.time()

        try:
            s = gsocket.create_connection((self.options.host, self.options.port))
        except:
            logging.warning("Could not connect to Mongo at: %s:%s" % (self.options.host, self.options.port))
            socket.close()
            return

        while True:
            try:
                req = socket.recv(16)
                if not len(req):
                    break
                header = self.extract_header(req)

                buf = req
                buf += socket.recv(header.length - 16)

                count = stats.setdefault(header.operation, 0)
                stats[header.operation] = count + 1
                if header.operation == OP_DELETE:
                    self.handle_delete(header, buf, s)
                    continue
                elif header.operation == OP_QUERY:
                    self.handle_query(header, buf, s, socket)
                elif header.operation == OP_INSERT:
                    self.handle_insert(header, buf, s)
                elif header.operation == OP_UPDATE:
                    self.handle_update(header, buf, s)
                elif header.operation == OP_GETMORE:
                    self.handle_more(header, buf, s, socket)
                else:
                    s.sendall(buf)
            except Exception, e:
                s.close()
                socket.close()
                break

        self.log_stats(stats)
        self._invoke_callback('on_close', address)
        logging.info("Connection closed\n")
        del CONNECTIONS[gevent.getcurrent()]
        socket.close()
        s.close()
        return


    def start(self):
        host, port = self.options.bind_addr.split(":")
        self.server = StreamServer((host, int(port)), self.proxy)
        logging.info('Starting echo server on port %s' % port)
        try:
            self.server.serve_forever()
        except KeyboardInterrupt:
            logging.info("Shutting down")


def get_cli_options():
    parser = OptionParser(usage="usage: python %prog [options]",
                          description="""This script proxies connection to MongoDB and logs request and responses.""")

    parser.add_option("-H", "--host",
                      dest="host",
                      default="localhost",
                      metavar="HOST",
                      help="MongoDB host to proxy. Default: localhost")
    parser.add_option("-p", "--port",
                      dest="port",
                      default=27017,
                      metavar="PORT",
                      help="MongoDB port to proxy. Default: 27017")
    parser.add_option("-b", "--bind",
                      dest="bind_addr",
                      default="localhost:37017",
                      metavar="BIND_ADDRESS",
                      help="The host:port address to bind to.  Default: localhost:37017")

    parser.add_option("--raw",
                      dest="raw",
                      default=False,
                      help="Log verbose raw protocol data",
                      action="store_true")

    (options, args) = parser.parse_args()

    return options

if __name__ == '__main__':
    options = get_cli_options()

    proxy = MongoProxy(options)
    if options.raw:
        proxy.add_listener(RawLoggingListener())
    proxy.add_listener(TimingListener())
    proxy.start()
