import time
import os

from django.db import models
from django.conf import settings
from elasticsearch import Elasticsearch

if os.environ.get('READTHEDOCS'):
    TagField = lambda *args, **kwargs: None
else:
    from tagging.fields import TagField

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
    def find_events(time_from=None, time_until=None, tags=None):
        query = {
            "filter" : {
                "query" : {
                    "query_string" : {
                        "query" : "HOST:devexp03"
                    }
                }
            },
            "size":500
        }

        events = list()

        result = elasticsearchclient.search(Event.indices(time_from, time_until), body=query)
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
    def indices(time_from, time_until):
        return "events-*"

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
        for tag in tags:
            pass

    @staticmethod
    def from_es_dict(dict):
        event = Event()
        event.id = dict["_id"]
        source = dict["_source"]
        event.when = source["@timestamp"]
        event.what = source["message"]
        if source.get("data") is not None:
            event.data = source["data"]
        tags = []
        for key in source.keys():
            if key not in ["@timestamp", "message", "data"]:
                if key == "tags":
                    tags.append(" ".join(source[key]))
                else:
                    tags.append("%s:%s"%(key, source[key]))
        event.tags = " ".join(tags)
        return event
    
    def save(self):
        pass
        

# We use this rather than tagging.register() so that tags can be exposed
# in the admin UI
# ModelTaggedItemManager().contribute_to_class(Event, 'tagged')
elasticsearchclient = Elasticsearch(settings.ELASTICSEARCH_HOST)