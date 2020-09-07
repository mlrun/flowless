import sys
from datetime import datetime

from mlrun.utils import create_logger


class Response(object):
    def __init__(self, headers=None, body=None, content_type=None, status_code=200):
        self.headers = headers or {}
        self.body = body
        self.status_code = status_code
        self.content_type = content_type or 'text/plain'


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
        self.id = 0
        self.body = body
        self.content_type = content_type
        self.trigger = None
        self.headers = headers or {}
        self.method = method
        self.path = path or '/'
        self.end = False
        self._trace_log = []

    def add_trace(self, id, step, status='ok', body=None, timestamp=None):
        timestamp = timestamp or datetime.now()
        self._trace_log.append({'id': id, 'step': step, 'status': status,
                                'body': str(body), 'time': timestamp})