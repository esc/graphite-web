from datetime import datetime

from django.test import TestCase
from graphite.elasticsearchevents.models import Event


class TestEventBuildQuery(TestCase):

    @staticmethod
    def _expected(tag_string=''):
        expected = {'filter':
                    {'and':
                     [{'query': {'query_string': {'query': '%s' % tag_string}}},
                      {'range': {'@timestamp': {'gte': '21600000', 'lte': '108000000'}}}
                      ]},
                    'sort': {'@timestamp': {'order': 'desc'}},
                    'size': 500
                    }
        return expected

    def test_build_query_empty_string(self):
        query = Event._build_tag_query_string('')
        self.assertEqual(query, [{'query': {'query_string': {'query': ''}}}])

    def test_build_query_single_tag(self):
        query = Event.buildQuery(['tag1'],
                                 time_from=datetime(1970, 1, 1),
                                 time_until=datetime(1970, 1, 2))
        self.assertEqual(query, self._expected('tags:tag1'))

    def test_build_query_double_tag(self):
        query = Event.buildQuery(['tag1', 'tag2'],
                                 time_from=datetime(1970, 1, 1),
                                 time_until=datetime(1970, 1, 2))
        self.assertEqual(query, self._expected('tags:tag1 AND tags:tag2'))

    def test_build_query_single_host_tag(self):
        query = Event.buildQuery(['HOST:devfoo01'],
                                 time_from=datetime(1970, 1, 1),
                                 time_until=datetime(1970, 1, 2))
        self.assertEqual(query, self._expected('HOST:devfoo01'))

    def test_build_query_double_host_tag(self):
        query = Event.buildQuery(['HOST:devfoo01', 'HOST:devfoo02'],
                                 time_from=datetime(1970, 1, 1),
                                 time_until=datetime(1970, 1, 2))
        self.assertEqual(query, self._expected('HOST:devfoo01 AND HOST:devfoo02'))
