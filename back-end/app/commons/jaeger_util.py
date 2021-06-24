import logging
from jaeger_client import Config
from opentracing.ext import tags
from opentracing.propagation import Format

class JaegerUtil:

  tracer = None
  active_span = None
  
  def start(self, service_name):
    if not JaegerUtil.tracer:
      log_level = logging.DEBUG
      logging.getLogger('').handlers = []
      logging.basicConfig(format='%(asctime)s %(message)s', level=log_level)
      config = Config(
        config={
          'sampler': {
            'type': 'const',
            'param': 1,
          },
          'logging': True,
        },
        service_name=service_name,
        validate=True,
      ) 
      JaegerUtil.tracer = config.initialize_tracer()

  def booking(self, name, content, parent, tags):
    span = None
    if JaegerUtil.tracer:
      with JaegerUtil.tracer.start_active_span(name, child_of=parent, tags=tags) as scope:
        if scope and scope.span:
          scope.span.log_kv(content)
          span = scope.span
          JaegerUtil.active_span = span
    return span
  
  def inject_header(self, url, method, headers):
    if JaegerUtil.tracer:
      span = JaegerUtil.active_span
      if span:
        span.set_tag(tags.HTTP_URL, url)
        span.set_tag(tags.HTTP_METHOD, method)
        span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
        JaegerUtil.tracer.inject(span, Format.HTTP_HEADERS, headers)
    return headers
  
  def extract_header(self, headers):
    span_tags = {}
    if JaegerUtil.tracer:
      span_ctx = JaegerUtil.tracer.extract(Format.HTTP_HEADERS, headers)
      span_tags = {tags.SPAN_KIND: tags.SPAN_KIND_RPC_SERVER}
    return span_tags