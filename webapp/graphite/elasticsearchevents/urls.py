from django.conf.urls import patterns

urlpatterns = patterns('graphite.elasticsearchevents.views',
  ('^get_data?$', 'get_data'),
  (r'(?P<event_id>\w+)/$', 'detail'),
  ('^$', 'view_events'),
)
