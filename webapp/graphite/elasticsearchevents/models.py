from django.conf import settings
from elasticsearch import Elasticsearch
from datetime import timedelta, datetime
import calendar
from dateutil import parser
from collections import defaultdict


def to_millis(datetime_):
    """ Convert a datetime object to milliseconds since epoch. """
    return datetime_.strftime("%s000")


def datetime_from_something(something):
    """ Convert something to to a datetime.

    Something can be either a millisecond timestamp or some kind of date string.
    """
    try:
        timestamp = int(something) / 1000
    except ValueError:
        timestamp = calendar.timegm(parser.parse(something).timetuple())
    return datetime.fromtimestamp(timestamp)


class Event(object):
    """ Event Object. """

    def __init__(self, when=None, what=None, data=None, tags=None):
        self.id   = -1
        self.when = when
        self.what = what
        self.data = data
        self.tags = tags

    def __str__(self):
        return "%s: %s" % (self.when, self.what)

    def __eq__(self, other):
        return (self.id == other.id and
                self.when == other.when and
                self.data == other.data and
                self.tags == other.tags)

    @staticmethod
    def _build_tag_query_string(tags):
        queries = []
        if tags is not None:
            luceneQuery = []
            for tag in tags:
                if tag.find(":") > -1:
                    luceneQuery.append(tag)
                else:
                    luceneQuery.append("tags:%s" % tag)
            queries.append({"query":
                            {"query_string":
                             {"query": " AND ".join(luceneQuery)}
                             }
                            })
        return queries

    @staticmethod
    def build_query(tags, time_from=None, time_until=None):
        """ Build Lucene query string to find events based on tags. """
        queries = Event._build_tag_query_string(tags)

        queries.append({"range":
                        {"@timestamp":
                         {"gte": to_millis(time_from),
                          "lte": to_millis(time_until)
                          }
                         }
                        })

        if len(queries) > 0:
            return {
                "filter": {
                    "and": queries
                },
                "sort": {"@timestamp": {"order": "desc"}},
                "size": 500
            }
        else:
            return None

    @staticmethod
    def find_events(time_from=None, time_until=None, tags=None):
        """ Find multiple events bases on time and tags. """
        query = Event.build_query(tags, time_from, time_until)
        return Event._find_events(Event.indices(time_from, time_until), query)

    @staticmethod
    def _find_events(indices, query):
        result = elasticsearchclient.search(indices, body=query, ignore_unavailable=True)
        events = list()
        for hit in result["hits"]["hits"]:
            event = Event.from_es_dict(hit)
            events.append(event)
        return events

    @staticmethod
    def find_event(id):
        """ Find a single event based on id. """
        query = {"filter": {"ids": {"values": [id]}}}
        events = Event._find_events(settings.ELASTICSEARCH_EVENT_FALLBACK_INDEXPATTERN, query)
        if len(events) > 0:
            return events[0]

    @staticmethod
    def indices(time_from, time_until):
        """ Create list of index descriptors to search in based on time. """
        day_from = datetime(time_from.year, time_from.month, time_from.day)
        day_until = datetime(time_until.year, time_until.month, time_until.day)
        if (day_until - day_from).days > 4:
            return settings.ELASTICSEARCH_EVENT_FALLBACK_INDEXPATTERN
        date = day_from
        indices = []
        while date <= day_until:
            indices.append(Event.indexForDate(date))
            date = date + timedelta(1)
        return ",".join(indices)

    @staticmethod
    def indexForDate(date):
        """ Turn a datetime object into an index descriptor string. """
        return date.strftime(settings.ELASTICSEARCH_EVENT_INDEXPATTERN)

    def as_dict(self):
        """ Convert to python dict. """
        return dict(
            when=self.when,
            what=self.what,
            data=self.data,
            tags=self.tags,
            id=self.id,
        )

    def as_es_dict(self):
        """ Convert to dict suitable for inserting into elasticsearch. """
        result = defaultdict(list)
        result["@timestamp"] = to_millis(self.when)
        result["message"] = self.what
        result["data"] = self.data
        if self.tags is not None:
            for tag in self.tags.split(" "):
                key, sep, val = tag.partition(":")
                if not sep:
                    key, val = "tags", key
                result[key].append(val)
        return result

    @staticmethod
    def from_es_dict(dict):
        """ Convert event dict returned by Elasticsearch into an Event object.
        """
        event = Event()
        event.id = dict["_id"]
        source = dict["_source"]
        event.when = datetime_from_something(source["@timestamp"])
        event.what = source["message"]
        if source.get("data") is not None:
            event.data = source["data"]
        tags = []
        for key, val in source.iteritems():
            if key not in ["@timestamp", "message", "data"]:
                vals = val
                if isinstance(val, basestring):
                    vals = [val]
                if key == "tags":
                    prefix = ""
                else:
                    prefix = "%s:" % key
                tags.append(" ".join(['%s%s' % (prefix, val) for val in vals]))
        event.tags = " ".join(tags)
        return event

    def save(self):
        """ Save this Event object in elasticsearch. """
        elasticsearchclient.index(Event.indexForDate(self.when), "event", body=self.as_es_dict())


elasticsearchclient = Elasticsearch(settings.ELASTICSEARCH_HOST)
