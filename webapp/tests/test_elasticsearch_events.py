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

