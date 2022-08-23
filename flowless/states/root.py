# Copyright 2020 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import json
import os
import socket
import sys

from .base import ObjDict, StateResource, INIT_LOCAL, INIT_REMOTE_API
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
    _dict_fields = SubflowState._dict_fields[1:] + ['project', 'triggers', 'resources', 'default_resource',
                                                    'parameters', 'format', 'trace', 'streams_path']

    def __init__(self, name=None, states=None, start_at=None, project=None, triggers=None, resources=None,
                 parameters=None, default_resource=None, format=None, trace=0):
        super().__init__(name, states, start_at=start_at)
        self.project = project
        self.triggers = triggers or {}
        self._resources = None or {}
        self.resources = resources
        self.parameters = parameters or {}
        self.context = None
        self.default_resource = default_resource
        self.server_context = None
        self.format = format or 'json'
        self.trace = trace
        self.streams_path = None
        self.in_nuclio = False

    @property
    def resources(self):
        return self._resources

    @resources.setter
    def resources(self, resources):
        self._resources = ObjDict.from_dict(StateResource, resources)

    def add_resource(self, name, kind, uri, spec=None, endpoint=None):
        self._resources[name] = StateResource(kind, uri, spec, endpoint)

    def get_resource(self):
        return self.default_resource

    def deploy(self):
        return deploy_pipline(self)

    def prepare(self, current_resource=None):
        self.validate()
        prep_tree(self, self, current_resource)

    def init(self, resource, context=None, namespace=None):
        self.from_state = os.environ.get('FROM_STATE', None)
        self.context = context or TaskRunContext()
        self.server_context = _ServerContext(self)
        setattr(self.context, 'root', self)
        self.init_objects(self.context, resource, namespace or globals())

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
        body = response.body

        if self.in_nuclio and not isinstance(body, (str, bytes)):
            body = json.dumps(body)
            return self.context.Response(
                body=body, content_type='application/json',
                status_code=200
            )
        return body

    def export(self, target=""):
        target = target or "pipeline.yaml"
        if target.endswith('.yaml'):
            data = self.to_yaml()
        else:
            data = self.to_json()
        with open(target, 'w') as fp:
            fp.write(data)


def prep_tree(root, state, current_resource):
    for child in state.get_children():
        child.set_parent(state, root)
        child.validate()
        if child.get_resource() == current_resource:
            child.set_object_type(INIT_LOCAL)

    if hasattr(state, 'start_at'):
        start_obj = state[state.start_at]
        prep_next(root, state, start_obj, current_resource)

    for child in state.get_children():
        if child.kind == 'router':
            for route in child.values():
                prep_next(root, child, route, current_resource)
        for branch in child.next_branches():
            if branch not in state.keys():
                raise ValueError(f'next state ({branch} not found under {state.name}')
            next_obj = state[branch]
            child.before(next_obj)
            prep_next(root, child, next_obj, current_resource)

        prep_tree(root, child, current_resource)


def prep_next(root, source_obj, next_obj, current_resource):
    source_resource = source_obj.get_resource()
    next_resource = next_obj.get_resource()
    if source_resource != next_resource:
        root.resources[next_resource].add_input(source_resource, source_obj.name, next_obj.fullname)
        if next_resource and current_resource == source_resource:
            next_obj.set_object_type(INIT_REMOTE_API)

