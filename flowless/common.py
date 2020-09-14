import sys
import uuid
from datetime import datetime, timezone

from mlrun.utils import create_logger


class Response(object):
    def __init__(self, headers=None, body=None, content_type=None, status_code=200):
        self.headers = headers or {}
        self.body = body
        self.status_code = status_code
        self.content_type = content_type or 'text/plain'

    def __repr__(self):
        cls = self.__class__.__name__
        items = self.__dict__.items()
        args = ('{}={!r}'.format(key, value) for key, value in items)
        return '{}({})'.format(cls, ', '.join(args))


class TaskRunContext:
    def __init__(self):
        self.state = None
        self.logger = create_logger('debug', 'human', "flow", sys.stdout)
        self.worker_id = 0
        self.Response = Response
        self.root = None


class Event(object):
    def __init__(self, body=None, content_type=None,
                 headers=None, method=None, path=None):
        self.id = uuid.uuid4().hex
        self.key = ''
        self.body = body
        self.time = None

        # optional
        self.headers = headers or {}
        self.method = method
        self.path = path or '/'
        self.content_type = content_type
        self.trigger = None
        self.end = False
        self._trace_log = None

    def __str__(self):
        return f'Event(id={self.id}, body={self.body})'

    def add_trace(self, id, step, status='ok', body=None, timestamp=None, verbosity=0):
        if verbosity == 0:
            return

        if self._trace_log is None:
            self._trace_log = []
        timestamp = timestamp or datetime.now(timezone.utc)
        self._trace_log.append({'id': id, 'step': step, 'status': status,
                                'body': str(body), 'time': timestamp})
        if verbosity > 1:
            print(f'Event: id={id}, step={step}, time={timestamp}, status={status}, body={body}')


def stream_uri(pipeline, uri, name):
    if uri:
        return uri
    if not pipeline.streams_path:
        raise ValueError('stream uri or pipeline.streams_path must be defined')
    return '/'.join([pipeline.streams_path.strip('/'), name])
