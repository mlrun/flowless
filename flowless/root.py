import json
import socket
import sys

from mlrun.utils import create_logger
from flowless import MLTaskFlow
from mlrun.platforms.iguazio import OutputStream


class Response(object):
    def __init__(self, headers=None, body=None, content_type=None, status_code=200):
        self.headers = headers or {}
        self.body = body
        self.status_code = status_code
        self.content_type = content_type or 'text/plain'


class TaskRunContext:
    def __init__(self):
        self.state = None
        self.logger = create_logger('info', 'human', "flow", sys.stdout)
        self.worker_id = 0
        self.Response = Response
        self.root = None


class _ServerContext:
    def __init__(self, root):
        self.worker = root.context.worker_id
        self.hostname = socket.gethostname()
        self.output_stream = None
        out_stream = root.parameters.get('log_stream', '')
        self.stream_sample = int(root.parameters.get('log_stream_sample', '1'))
        self.stream_batch = int(root.parameters.get('log_stream_batch', '1'))
        if out_stream:
            self.output_stream = OutputStream(out_stream)


class MLTaskRoot(MLTaskFlow):
    kind = 'root'
    _dict_fields = MLTaskFlow._dict_fields[1:] + ['triggers', 'default_resource', 'parameters', 'format']

    def __init__(self, name=None, states=None, start_at=None,
                 parameters=None, default_resource=None, format=None):
        super().__init__(name, states, start_at=start_at)
        self.triggers = None
        self.resources = None
        self.parameters = parameters or {}
        self.context = None
        self.default_resource = default_resource
        self.server_context = None
        self.format = format

    def start(self, resource, context=None, namespace=None):
        self.context = context or TaskRunContext()
        self.server_context = _ServerContext(self)
        setattr(self.context, 'root', self)
        self.init_objects(self.context, resource, namespace or globals(), self.default_resource)

    def add_root_params(self, params={}):
        for key, val in self.parameters.items():
            if key not in params:
                params[key] = val
        return params

    def run(self, context, event, *args, **kwargs):
        context = context or self.context
        response = super().run(context, event, *args, **kwargs)

        if self.format == 'json' and not isinstance(response, (str, bytes)):
            response = json.dumps(response)
            return self.context.Response(
                body=response, content_type='application/json', status_code=200
            )
        return response
