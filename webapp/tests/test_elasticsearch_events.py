from datetime import datetime

from django.test import TestCase
from graphite.elasticsearchevents.models import Event


class TestEventBuildQuery(TestCase):

    @staticmethod
    def _expected(tag_string=''):
        return [{'query': {'query_string': {'query': '%s' % tag_string}}}]

    def test_build_query_empty_string(self):
        query = Event._build_tag_query_string('')
        self.assertEqual(query, self._expected())

    def test_build_query_single_tag(self):
        query = Event._build_tag_query_string(['tag1'])
        self.assertEqual(query, self._expected('tags:tag1'))

    def test_build_query_tripple_tag(self):
        query = Event._build_tag_query_string(['tag1', 'tag2', 'tag3'])
        self.assertEqual(query,
            self._expected('tags:tag1 AND tags:tag2 AND tags:tag3'))

    def test_build_query_double_tag(self):
        query = Event._build_tag_query_string(['tag1', 'tag2'])
        self.assertEqual(query, self._expected('tags:tag1 AND tags:tag2'))

    def test_build_query_single_host_tag(self):
        query = Event._build_tag_query_string(['HOST:devfoo01'])
        self.assertEqual(query, self._expected('HOST:devfoo01'))

    def test_build_query_double_host_tag(self):
        query = Event._build_tag_query_string(['HOST:devfoo01', 'HOST:devfoo02'])
        self.assertEqual(query, self._expected('HOST:devfoo01 AND HOST:devfoo02'))

    def test_build_query_mixing_tag_and_host_tag(self):
        query = Event._build_tag_query_string(['HOST:devfoo01', 'tag1'])
        self.assertEqual(query, self._expected('HOST:devfoo01 AND tags:tag1'))


class TestIndices(TestCase):

    def test_indices_no_day_difference(self):
        time_from = datetime(1970, 1, 1)
        time_until = datetime(1970, 1, 1)
        indices = Event.indices(time_from, time_until)
        self.assertEqual('events-1970-01-01', indices)

    def test_indices_one_day_difference(self):
        time_from = datetime(1970, 1, 1)
        time_until = datetime(1970, 1, 2)
        indices = Event.indices(time_from, time_until)
        self.assertEqual('events-1970-01-01,events-1970-01-02', indices)

    def test_indices_four_day_difference(self):
        time_from = datetime(1970, 1, 1)
        time_until = datetime(1970, 1, 5)
        indices = Event.indices(time_from, time_until)
        self.assertEqual('events-1970-01-01,events-1970-01-02,events-1970-01-03,events-1970-01-04,events-1970-01-05', indices)

    def test_indices_beyond_four_day_difference(self):
        time_from = datetime(1970, 1, 1)
        time_until = datetime(1970, 1, 6)
        indices = Event.indices(time_from, time_until)
        self.assertEqual('events-*', indices)


class TestAsDict(TestCase):

    def test_as_dict(self):
        when = datetime(1970, 1, 1)
        what = 'title'
        data = 'message'
        tags = 'tag1 tag2'
        event = Event(when=when, what=what, data=data, tags=tags)
        expected = {'data': 'message',
                    'what': 'title',
                    'when': datetime(1970, 1, 1, 0, 0),
                    'id': -1,
                    'tags': 'tag1 tag2'}
        self.assertEqual(expected, event.as_dict())


class TestAsEsDict(TestCase):

    def test_as_es_dict(self):
        when = datetime(1970, 1, 1)
        what = 'title'
        data = 'message'
        tags = 'tag1 tag2'
        event = Event(when=when, what=what, data=data, tags=tags)
        expected = {'@timestamp': '21600000',
                    'data': 'message',
                    'message': 'title',
                    'tags': ['tag1', 'tag2']}
        self.assertEqual(expected, dict(event.as_es_dict()))

    def test_as_es_dict_with_host_tag(self):
        when = datetime(1970, 1, 1)
        what = 'title'
        data = 'message'
        tags = 'tag1 HOST:devfoo01'
        event = Event(when=when, what=what, data=data, tags=tags)
        expected = {'@timestamp': '21600000',
                    'data': 'message',
                    'message': 'title',
                    'tags': ['tag1'],
                    'HOST': ['devfoo01']}
        self.assertEqual(expected, dict(event.as_es_dict()))


class TestFromEsDict(TestCase):

    def test_from_es_dict_with_tags(self):
        input = {'_id': -1,
                 '_source': {'@timestamp': '21600000',
                             'data': 'message',
                             'message': 'title',
                             'tags': ['tag1', 'tag2']}
                 }
        expected = Event(when=datetime(1970, 1, 1),
                         what='title',
                         data='message',
                         tags='tag1 tag2')
        self.assertEqual(expected, Event.from_es_dict(input))

    def test_from_es_dict_with_host_tags(self):
        input = {'_id': -1,
                 '_source': {'@timestamp': '21600000',
                             'data': 'message',
                             'message': 'title',
                             'tags': ['tag1'],
                             'HOST': ['devfoo01']}
                 }
        expected = Event(when=datetime(1970, 1, 1),
                         what='title',
                         data='message',
                         tags='HOST:devfoo01 tag1')
        self.assertEqual(expected, Event.from_es_dict(input))
