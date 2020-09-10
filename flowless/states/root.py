import json
import socket
import sys

from .base import ObjDict, StateResource
from .flow import SubflowState
from mlrun.platforms.iguazio import OutputStream

from ..common import TaskRunContext, Event
from ..deploy import deploy_pipline


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


class FlowRoot(SubflowState):
    kind = 'root'
    _dict_fields = SubflowState._dict_fields[1:] + ['triggers', 'resources', 'default_resource',
                                                    'parameters', 'format', 'trace']

    def __init__(self, name=None, states=None, start_at=None, triggers=None, resources=None,
                 parameters=None, default_resource=None, format=None, trace=0):
        super().__init__(name, states, start_at=start_at)
        self.triggers = triggers or {}
        self._resources = None or {}
        self.resources = resources
        self.parameters = parameters or {}
        self.context = None
        self.default_resource = default_resource
        self.server_context = None
        self.format = format
        self.trace = trace

    @property
    def resources(self):
        return self._resources

    @resources.setter
    def resources(self, resources):
        self._resources = ObjDict.from_dict(StateResource, resources)

    def add_resource(self, name, kind, uri, spec=None, endpoint=None):
        self._resources[name] = StateResource(kind, uri, spec, endpoint)

    def deploy(self):
        return deploy_pipline(self)

    def init(self, resource, context=None, namespace=None):
        self.context = context or TaskRunContext()
        self.server_context = _ServerContext(self)
        setattr(self.context, 'root', self)
        self.init_objects(self.context, resource, namespace or globals(), self.default_resource)

    def add_root_params(self, params={}):
        for key, val in self.parameters.items():
            if key not in params:
                params[key] = val
        return params

    def run(self, event, *args, context=None, **kwargs):
        context = context or self.context
        if not hasattr(event, 'id'):
            event = Event(body=event)

        event.add_trace(event.id, self.name, 'start', event.body, verbosity=self.trace)
        response = super().run(context, event, *args, **kwargs)

        if self.format == 'json' and not isinstance(response, (str, bytes)):
            response = json.dumps(response)
            return self.context.Response(
                body=response, content_type='application/json', status_code=200
            )
        return response
