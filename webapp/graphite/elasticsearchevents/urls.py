from django.conf.urls import patterns

urlpatterns = patterns('graphite.elasticsearchevents.views',
  ('^get_data?$', 'get_data'),
  (r'(?P<event_id>[^/]+)/$', 'detail'),
  ('^$', 'view_events'),
)
