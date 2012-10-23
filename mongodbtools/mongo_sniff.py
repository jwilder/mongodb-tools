#!/usr/bin/env python
"""
This is a PCAP based MongoDB network sniffer that works at the MongoDB wire protocol [1].
It will log every request and response sent to MongoDB and as well as print the equivalent
MongoDB JavaScript shell command.  Because this work at the wire protocol level,
it should be compatible with all existing MongoDB drivers.

[1] http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol
"""
from collections import namedtuple
import datetime
import json
from optparse import OptionParser
import struct
import sys

from colorama import Fore, Style
import bson
import pcapy
from impacket.ImpactDecoder import LinuxSLLDecoder, EthDecoder

CONNECTIONS = {}


class BsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (bson.ObjectId, bson.DBRef, bson.Timestamp, datetime.datetime)):
            return str(obj)
        else:
            return json.JSONEncoder.default(self, obj)


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

Envelope = namedtuple('Envelope', 'header, message, packet_header, packet')

MessageHeader = namedtuple('MessageHeader', 'length, request_id, response_to, operation')
QueryMessage = namedtuple('QueryMessage', 'header, collection, flags, skip, to_return, query, fields')
ReplyMessage = namedtuple('ReplyMessage', 'header, flags, cursor_id, starting_from, number_returned, documents')
InsertMessage = namedtuple('InsertMessage', 'header, flags, collection, documents')
MoreMessage = namedtuple('MoreMessage', 'header, collection, to_return, cursor_id')
UpdateMessage = namedtuple('UpdateMessage', 'header, collection, flags, selector, update')
DeleteMessage = namedtuple('DeleteMessage', 'header, collection, flags, selector')

class MongoListener(object):

    def __init__(self, options):
        super(MongoListener, self).__init__()
        self.options = options

    def on_open(self, address):
        pass

    def on_close(self, address):
        pass

    def before_query(self, envelope):
        pass

    def after_query(self, envelope):
        pass

    def before_query_send(self, envelope):
        pass

    def after_query_send(self, envelope):
        pass

    def before_query_reply(self, envelope):
        pass

    def after_query_reply(self, envelope):
        pass

    def before_reply(self, envelope):
        pass

    def after_reply(self, envelope):
        pass

    def before_insert(self, envelope):
        pass

    def after_insert(self, envelope):
        pass

    def before_more(self, envelope):
        pass

    def after_more(self, envelope):
        pass

    def before_more_send(self, envelope):
        pass

    def after_more_send(self, envelope):
        pass

    def before_more_reply(self, envelope):
        pass

    def after_more_reply(self, envelope):
        pass

    def before_update(self, envelope):
        pass

    def after_update(self, envelope):
        pass

    def before_delete(self, envelope):
        pass

    def after_delete(self, envelope):
        pass

class BaseLoggingListener(MongoListener):
    def _colorize(self, color, text):
        if self.options.nocolor:
            return text
        return color + str(text) + Fore.RESET

    def _style(self, color, text):
        if self.options.nocolor:
            return text
        return color + str(text) + Style.RESET_ALL

    def _color_for(self, value):
        if self.options.nocolor:
            return value

        return [Fore.MAGENTA, Fore.CYAN][value % 2] + str(value) + Fore.RESET if value else ""

    def _log(self, ts, src, dest, request_id, response_id, db, msg, dur):
        print datetime.datetime.utcfromtimestamp(ts).isoformat() + \
                 "  " + Style.NORMAL + self._colorize(Fore.YELLOW, src + " > " + dest) + \
                 "  [%s] %s %s %s " % (db.rjust(10),
                                       self._color_for(request_id),
                                      self._color_for(response_id),
                                      msg) + \
                 self._colorize(Style.BRIGHT + Fore.BLUE, "%0.3fms" % (dur * 1000)) + Style.RESET_ALL



class RawLoggingListener(BaseLoggingListener):
    def before_query(self, envelope):
        msg = envelope.message
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

        self.log(envelope, msg.collection, self._style(Style.BRIGHT, "QUERY") +
                                           self._colorize(Fore.GREEN, " flags=[%s] skip=%s limit=%s selector=%s fields=%s" % (
            "|".join(flag_strs), msg.skip, msg.to_return, msg.query, msg.fields)))

    def before_reply(self, envelope):
        msg = envelope.message
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

        self.log(envelope, "---", self._style(Style.BRIGHT, "REPLY") +
                                  self._colorize(Fore.GREEN, " flags=[%s] id=%s from=%s count=%s %s" % (
            "|".join(flag_strs), msg.cursor_id, msg.starting_from, msg.number_returned, msg.documents)))

    def before_insert(self, envelope):
        msg = envelope.message
        flags = msg.flags
        flag_strs = []
        if flags & 1 == 1:
            flag_strs.append("CONTINUE")

        self.log(envelope, msg.collection, self._style(Style.BRIGHT, "INSERT") +
                 self._colorize(Fore.RED, " flags=[%s] %s" % (
            "|".join(flag_strs),
            msg.documents if len(msg.documents) == 1 else len(msg.documents))))

    def before_more(self, envelope):
        self.log(envelope, envelope.collection, self._style(Style.BRIGHT, "GETMORE") +
                                                self._colorize(Fore.GREEN, "%s %s" % (
            envelope.to_return, envelope.cursor_id)))

    def before_update(self, envelope):
        msg = envelope.message
        flags = msg.flags
        flag_strs = []
        if flags & 1 == 1:
            flag_strs.append("UPSERT")

        if flags & 2 == 2:
            flag_strs.append("MULTI")

        self.log(envelope, msg.collection, self._style(Style.BRIGHT, "UPDATE") +
                                           self._colorize(Fore.RED, "flags=[%s] %s %s" % (
            "|".join(flag_strs),
            msg.selector, msg.update)))

    def before_delete(self, envelope):
        msg = envelope.message
        self.log(envelope, msg.collection, "%s %s" % (
            self._style(Style.BRIGHT, "DELETE"), msg.selector))

    def log(self, envelope, db, line):
        msg = envelope.message
        dur, start, stop = 0, 0, 0

        ts = envelope.packet_header.getts()
        stop = float(ts[0]) + float(ts[1]) / 1000000

        if start and stop:
            dur = stop - start

        ts = envelope.packet_header.getts()
        ts = float(ts[0]) + float(ts[1]) / 1000000

        src = envelope.packet.child().get_ip_src() + ":" + \
              str(envelope.packet.child().child().get_th_sport())
        dest = envelope.packet.child().get_ip_dst() + ":" + \
               str(envelope.packet.child().child().get_th_dport())


        # FIXME: Calculate duration
        self._log(ts, src, dest, msg.header.request_id, msg.header.response_to, db, line, 0)

class TimingListener(BaseLoggingListener):


    def __init__(self, options):
        super(TimingListener, self).__init__(options=options)
        self.requests = {}
        self.stats = {}

    def on_open(self, address):
        self.stats[address] = {}

    def on_close(self, address):
        del self.stats[address]

    def _before(self, envelope):
        header = envelope.header
        self.requests[header.request_id] = envelope

    def _json(self, msg):
        try:
            return json.dumps(msg, cls=BsonEncoder)
        except Exception, e:
            print "WARNING: ", e
            return "{..<error>..}"

    def _after(self, msg):
        query = ""
        src = msg.packet.child().get_ip_src() + ":" + \
              str(msg.packet.child().child().get_th_sport())
        dest = msg.packet.child().get_ip_dst() + ":" + \
               str(msg.packet.child().child().get_th_dport())

        dur, start, stop = 0, 0, 0
        if msg.header.response_to in self.requests:
            ts = self.requests[msg.header.response_to].packet_header.getts()
            start = float(ts[0]) + float(ts[1]) / 1000000


        ts = msg.packet_header.getts()
        stop = float(ts[0]) + float(ts[1]) / 1000000
        if start and stop:
            dur = stop - start

        request_id = msg.header.request_id
        response_id = msg.header.response_to

        if msg.header.operation == OP_QUERY:
            return
            #db = msg.collection[0:msg.collection.index(".")]
            #col = msg.collection[len(db)+1:]

            #selector = msg.query.get("$query", None)
            #if selector is None:
            #    selector = msg.query
            #query = Fore.GREEN + "db.%s.find(%s)" % (col, self._json(selector))

            #if msg.to_return > 0:
            #    query += ".limit(%s)" % msg.to_return

            #order_by = msg.query.get("$orderby")
            #if order_by:
            #    query += ".sort(%s)" % self._json(order_by)

            #query += Fore.RESET
        elif msg.header.operation == OP_GETMORE:
            db = msg.message.collection[0:msg.message.collection.index(".")]
            col = msg.message.collection[len(db)+1:]

            query = self._colorize(Fore.GREEN, "db.%s.MORE(%s)" % (
                col, self._json(msg.message.documents)))

        elif msg.header.operation == OP_INSERT:

            db = msg.message.collection[0:msg.message.collection.index(".")]
            col = msg.message.collection[len(db)+1:]

            query = self._colorize(Fore.RED, "db.%s.insert(%s)" % (
                col, self._json(msg.message.documents)))

        elif msg.header.operation == OP_UPDATE:
            db = msg.message.collection[0:msg.message.collection.index(".")]
            col = msg.message.collection[len(db)+1:]

            selector_json = self._json(msg.message.selector)
            update_json = self._json(msg.message.update)

            multi = "false"
            if msg.message.flags & 2 == 2:
                multi = "true"

            query = self._colorize(Fore.RED, "db.%s.update(%s, %s, %s)" % (
                col, selector_json, multi, update_json))
        elif msg.header.operation == OP_DELETE:
            db = msg.message.collection[0:msg.message.collection.index(".")]
            col = msg.message.collection[len(db)+1:]

            query = self._colorize(Fore.RED, "db.%s.remove(%s)" % (
                col, self._json(msg.message.selector)))
        elif msg.header.operation == OP_REPLY:
            reply = msg

            query_orig = self.requests[msg.header.response_to]
            query_msg = query_orig.message

            request_id = query_orig.header.request_id
            response_id = query_orig.header.response_to

            src = query_orig.packet.child().get_ip_src() + ":" + \
                  str(query_orig.packet.child().child().get_th_sport())
            dest = query_orig.packet.child().get_ip_dst() + ":" + \
                   str(query_orig.packet.child().child().get_th_dport())

            db = query_msg.collection[0:query_msg.collection.index(".")]
            col = query_msg.collection[len(db)+1:]

            if hasattr(query_msg, "query"):
                selector = query_msg.query.get("$query", None)
                if selector is None:
                    selector = query_msg.query
                query = "db.%s.find(%s)" % (col, self._json(selector))

                if query_msg.to_return > 0:
                    query += ".limit(%s)" % query_msg.to_return

                order_by = query_msg.query.get("$orderby")
                if order_by:
                    query += ".sort(%s)" % self._json(order_by)
            else:
                query = self._colorize(Fore.GREEN, "db." + db + ".cursor(" + str(query_msg.cursor_id) + ").next()")
            query = self._colorize(Fore.GREEN, query)

        self._log(stop, src, dest, "", "", db, query, dur)

        if msg.header.response_to in self.requests:
            del self.requests[msg.header.response_to]
        if msg.header.request_id in self.requests:
            del self.requests[msg.header.request_id]

    def log(self, line):
        print line

    def before_update(self, envelope):
        self._before(envelope)

    def before_insert(self, envelope):
        self._before(envelope)

    def before_delete(self, envelope):
        self._before(envelope)

    def before_more(self, envelope):
        self._before(envelope)

    def before_query(self, envelope):
        self._before(envelope)

    def after_query(self, envelope):
        self._after(envelope)

    def after_insert(self, envelope):
        self._after(envelope)

    def after_delete(self, envelope):
        self._after(envelope)

    def after_update(self, envelope):
        self._after(envelope)


class MongoProxy(object):

    def __init__(self, options):
        super(MongoProxy, self).__init__()
        self.options = options
        self.listeners = []
        self.pdus = {}
        self.requests = {}
        self.frame_number = 1

    def add_listener(self, listener):
        if listener not in self.listeners:
            self.listeners.append(listener)

    def _invoke_callback(self, fname, msg):
        for listener in self.listeners:
            try:
                getattr(listener, fname)(msg)
            except Exception, e:
                print "ERROR:", e

    def extract_header(self, resp):
        if len(resp) < 16:
            raise Exception("Bad header")
        length, request_id, response_to, operation =  struct.unpack("<IIII", resp[0:16])
        return MessageHeader(length, request_id, response_to, operation)


    def _extract_cstring(self, buf, ns_end):
        while ord(buf[ns_end]) and ns_end < len(buf):
            ns_end += 1
        return ns_end + 1


    def _parse_delete(self, header, buf):
        ns_end = 20
        ns_end = self._extract_cstring(buf, ns_end)
        namespace = buf[20:ns_end-1]
        flags = struct.unpack("<I", buf[ns_end:ns_end + 4])[0]
        selector = bson.decode_all(buf[ns_end + 4:])
        msg = DeleteMessage(header, namespace, flags, selector)
        return msg

    def _parse_insert(self, header, buf):
        flags = struct.unpack("<I", buf[16:20])[0]
        ns_end = self._extract_cstring(buf, 20)
        namespace = buf[20:ns_end-1]

        length = struct.unpack("<I", buf[ns_end:ns_end+4])[0]

        documents = bson.decode_all(buf[ns_end:ns_end+length])
        msg = InsertMessage(header, flags, namespace, documents)
        return msg

    def _parse_update(self, header, buf):
        ns_end = 20
        ns_end = self._extract_cstring(buf, ns_end)
        namespace = buf[20:ns_end-1]
        flags = struct.unpack("<I", buf[ns_end:ns_end+4])[0]
        flags >>= 8
        selector_fields = bson.decode_all(buf[ns_end + 4:])
        selector = selector_fields[0]
        update = {}
        if len(selector_fields) == 2:
            update = selector_fields[1]
        msg = UpdateMessage(header, namespace, flags, selector, update)
        return msg

    def _parse_reply(self, header, resp):
        r_flags, r_cursors_id, r_from, r_count = struct.unpack("<IQII", resp[16:36])

        docs = bson.decode_all(resp[36:])

        msg = ReplyMessage(header, r_flags, r_cursors_id, r_from, r_count, docs)
        return msg

    def _parse_query(self, header, buf):

        flags = struct.unpack("<I", buf[16:20])[0]
        ns_end = self._extract_cstring(buf, 20)
        namespace = buf[20:ns_end-1]

        skip = struct.unpack("<I", buf[ns_end:ns_end + 4])[0]
        count = struct.unpack("<I", buf[ns_end + 4:ns_end + 8])[0]
        selector_fields = bson.decode_all(buf[ns_end + 8:])
        selector = selector_fields[0]
        fields = {}
        if len(selector_fields) == 2:
            fields = selector_fields[1]
        msg = QueryMessage(header, namespace, flags, skip, count, selector, fields)
        return msg

    def _parse_getmore(self, header, buf):
        ns_end = self._extract_cstring(buf, 20)
        namespace = buf[20:ns_end-1]
        count = struct.unpack("<I", buf[ns_end:ns_end + 4])[0]
        cursor_id = struct.unpack("<Q", buf[ns_end + 4:ns_end + 12])[0]
        msg = MoreMessage(header, namespace, count, cursor_id)
        return msg

    def _reassemble_if_possible(self, eth_hdr):
        ip_hdr = eth_hdr.child()
        tcp_hdr = ip_hdr.child()
        mongo = tcp_hdr.child()
        data = mongo.get_packet()

        mongo_chunks = []
        if len(data) > 16:
            header = self.extract_header(data)

            if header.length == len(data):
                return [data]
            else:
                id = ip_hdr
                pdu_list = self.pdus.setdefault(ip_hdr.get_ip_src(), [])
                pdu_list.append((ip_hdr.get_ip_id(), data))
                pdu_list = sorted(pdu_list)

                while len(pdu_list) > 0:

                    # Re-assemble fragmented packets
                    idx = 0
                    while idx + 1 < len(pdu_list) and pdu_list[idx][0] == (pdu_list[idx + 1][0] - 1):
                        idx += 1

                    if idx > 0:
                        packets = pdu_list[0:idx+1]
                        self.pdus[ip_hdr.get_ip_src()] = pdu_list[idx+1:]
                    else:
                        packets = pdu_list
                        self.pdus[ip_hdr.get_ip_src()] = []

                    pdu_list = self.pdus[ip_hdr.get_ip_src()]
                    buf = "".join([x[1] for x in packets])

                    if len(buf) > 16:
                        header = self.extract_header(buf)
                        # we may have multiple messages in the buffer, split them out
                        while len(buf) > header.length:
                            chunk = buf[0:header.length]
                            mongo_chunks.append(chunk)
                            buf = buf[header.length:]
                            header = self.extract_header(buf)

                        if header.length == len(buf):
                            mongo_chunks.append(buf)
                        else:
                            # left-over buffer isn't a full mongo packet, save it for later
                            pdu_list.append((ip_hdr.get_ip_id(), buf))

                    if len(pdu_list) == 1 and len(pdu_list[0][1]) > 16:
                        hdr = self.extract_header(pdu_list[0][1])
                        if hdr.length == len(pdu_list[0][1]):
                            mongo_chunks.append(pdu_list[0][1])
                            self.pdus[ip_hdr.get_ip_src()] = []
                        else:
                            break

        return mongo_chunks


    def _handle_packet(self, hdr, data):

        mongo_chunks = []

        #hrd, data = self.r.next()
        eth_hdr = self.decoder.decode(data)
        ip_hdr = eth_hdr.child()
        #print "IP", ip_hdr.get_ip_src(), ip_hdr.get_ip_dst()
        tcp_hdr = ip_hdr.child()
        mongo = tcp_hdr.child()

        mongo_chunks += self._reassemble_if_possible(eth_hdr)

        for chunk in mongo_chunks:
            try:
                header = self.extract_header(chunk)
                self.requests[header.request_id] = hdr.getts()

                if header.operation == OP_QUERY:

                    msg = self._parse_query(header, chunk)
                    envelope = Envelope(header, msg, hdr, eth_hdr)
                    self._invoke_callback("before_query", envelope)
                    self._invoke_callback("after_query", envelope)
                    #print msg
                elif header.operation == OP_DELETE:

                    msg = self._parse_delete(header, chunk)
                    envelope = Envelope(header, msg, hdr, eth_hdr)
                    self._invoke_callback("before_delete", envelope)
                    self._invoke_callback("after_delete", envelope)
                    #print msg
                elif header.operation == OP_INSERT:
                    msg = self._parse_insert(header, chunk)
                    envelope = Envelope(header, msg, hdr, eth_hdr)
                    self._invoke_callback("before_insert", envelope)
                    self._invoke_callback("after_insert", envelope)
                    #print msg
                elif header.operation == OP_UPDATE:

                    msg = self._parse_update(header, chunk)
                    envelope = Envelope(header, msg, hdr, eth_hdr)
                    self._invoke_callback("before_update", envelope)
                    self._invoke_callback("after_update", envelope)
                    #print msg

                elif header.operation == OP_GETMORE:

                    msg = self._parse_getmore(header, chunk)
                    envelope = Envelope(header, msg, hdr, eth_hdr)
                    self._invoke_callback("before_more", envelope)
                    self._invoke_callback("after_more", envelope)
                    #print msg


                elif header.operation == OP_REPLY:
                    #print i, hdr.getlen(), header.length, len(mongo.get_packet()), len(data), \
                    #    [hex(ord(x)) for x in mongo.get_packet()[36:40]], \
                    #[hex(ord(x)) for x in mongo.get_packet()[header.length-100:]]

                    msg = self._parse_reply(header, chunk)
                    envelope = Envelope(header, msg, hdr, eth_hdr)
                    self._invoke_callback("before_reply", envelope)
                    self._invoke_callback("after_reply", envelope)
                    self._invoke_callback("after_query", envelope)


                    #print msg
                else:
                    print "Unhandled op: " + str(header.operation)
            except Exception, e:
                print "ERROR:", e
        #hdr, data = r.next()
            self.frame_number += 1

        return data


    def dump_pcap(self, filename=None):
        if filename:
            self.r = pcapy.open_offline(filename)
        else:
            self.r = pcapy.open_live(self.options.interface, 65533, 0, 100)

        datalink = self.r.datalink()
        self.decoder = LinuxSLLDecoder()
        if pcapy.DLT_EN10MB == datalink:
            self.decoder = EthDecoder()

        self.r.setfilter('tcp port ' + str(self.options.port))

        try:
            while True:
                read = self.r.dispatch(1, self._handle_packet)
                if read == 0 and self.options.file:
                    break

        except KeyboardInterrupt:
            pass
        except pcapy.PcapError, e:
            print e
            pass

def get_cli_options():
    parser = OptionParser(usage="usage: python %prog [options]",
                          description="""Sniffs MongoDB traffic and displays equivalent JS console commands.""")

    parser.add_option("-i",
                      dest="interface",
                      metavar="INTERFACE",
                      default="any",
                      help="Interface to capture. Default: any")
    parser.add_option("-p", "--port",
                      dest="port",
                      default=27017,
                      metavar="PORT",
                      help="MongoDB port. Default: 27017")

    parser.add_option("--raw",
                      dest="raw",
                      default=False,
                      help="Log raw protocol data",
                      action="store_true")

    parser.add_option("--file",
                      dest="file",
                      help="Dump contents of a pcap file")

    parser.add_option("--live",
                      dest="live",
                      default=False,
                      help="Dump live packets",
                      action="store_true")

    parser.add_option("--nocolor",
                      dest="nocolor",
                      default=False,
                      help="Don't colorize output",
                      action="store_true")

    (options, args) = parser.parse_args()

    return options, args, parser


def main():
    options, args, parser = get_cli_options()
    proxy = MongoProxy(options)
    if options.raw:
        proxy.add_listener(RawLoggingListener(options=options))

    proxy.add_listener(TimingListener(options=options))
    if options.file is not None:
        proxy.dump_pcap(options.file)
        sys.exit(0)
    if options.live:
        proxy.dump_pcap()
        sys.exit(0)
    parser.print_help()

if __name__ == '__main__':
    main()
