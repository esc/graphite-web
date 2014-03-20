import time
import os

from django.conf import settings
from elasticsearch import Elasticsearch
from datetime import timedelta, datetime
import calendar
from dateutil import parser

def to_millis(datetime_):
    return datetime_.strftime("%s000")

def datetime_from_something(something):
    try:
        timestamp = int(something)/1000
    except ValueError:
        timestamp = calendar.timegm(parser.parse(something).timetuple())
    return datetime.fromtimestamp(timestamp)

class Event(object):

    def __init__(self, when=None, what=None, data=None, tags=None):
        self.id   = -1
        self.when = when
        self.what = what
        self.data = data
        self.tags = tags

    def __str__(self):
        return "%s: %s" % (self.when, self.what)

    @staticmethod
    def buildQuery(tags, time_from=None, time_until=None):
        queries = []
        if tags is not None:
            luceneQuery = []
            for tag in tags:
                if tag.find(":") > -1:
                    luceneQuery.append(tag)
                else:
                    luceneQuery.append("tags:%s" % tag)
            queries.append({"query": {"query_string": { "query": " AND ".join(luceneQuery)}}})

        queries.append({"range": {"@timestamp": {"gte": to_millis(time_from), "lte": to_millis(time_until)}}})

        if len(queries) > 0:
            return {
                "filter": {
                    "and": queries
                },
                "size": 500
            }
        else:
            return None

    @staticmethod
    def find_events(time_from=None, time_until=None, tags=None):
        query = Event.buildQuery(tags, time_from, time_until)
        return Event._find_events(Event.indices(time_from, time_until), query)

    @staticmethod
    def _find_events(indices, query):
        result = elasticsearchclient.search(indices, body=query, ignore_unavailable=True)
        events = list()
        for hit in result["hits"]["hits"]:
            event = Event.from_es_dict(hit)
            events.append(event)
        return events
        # if tags is not None:
        #     query = 
        # else:
        #     query = Event.objects.all()
        # 
        # if time_from is not None:
        #     query = query.filter(when__gte=time_from)
        # 
        # if time_until is not None:
        #     query = query.filter(when__lte=time_until)
        # 
        # 
        # result = list(query.order_by("when"))
        # return result

    @staticmethod
    def find_event(id):
        query = {"filter": {"ids":{"values": [id]}}}
        events = Event._find_events(settings.ELASTICSEARCH_EVENT_FALLBACK_INDEXPATTERN, query)
        if len(events) > 0:
            return events[0]

    @staticmethod
    def indices(time_from, time_until):
        day_from = datetime(time_from.year, time_from.month, time_from.day)
        day_until = datetime(time_until.year, time_until.month, time_until.day)
        if (day_until-day_from).days > 4:
            return settings.ELASTICSEARCH_EVENT_FALLBACK_INDEXPATTERN
        date = day_from
        indices = []
        while date <= day_until:
            indices.append(date.strftime(settings.ELASTICSEARCH_EVENT_INDEXPATTERN))
            date = date + timedelta(1)
        return ",".join(indices)

    def as_dict(self):
        return dict(
            when=self.when,
            what=self.what,
            data=self.data,
            tags=self.tags,
            id=self.id,
        )

    def as_es_dict(self):
        result = {}
        result["@timestamp"] = self.when
        result["message"] = self.what
        result["data"] = self.data
        return result

    @staticmethod
    def from_es_dict(dict):
        event = Event()
        event.id = dict["_id"]
        source = dict["_source"]
        event.when = datetime_from_something(source["@timestamp"])
        event.what = source["message"]
        if source.get("data") is not None:
            event.data = source["data"]
        tags = []
        for key in source.keys():
            if key not in ["@timestamp", "message", "data"]:
                if key == "tags":
                    tags.append(" ".join(source[key]))
                else:
                    tags.append("%s:%s" % (key, source[key]))
        event.tags = " ".join(tags)
        return event

    def save(self):
        pass


elasticsearchclient = Elasticsearch(settings.ELASTICSEARCH_HOST)