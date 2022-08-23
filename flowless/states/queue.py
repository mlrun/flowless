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
from copy import copy
from .base import BaseState, INIT_REMOTE_API


class QueueState(BaseState):
    kind = 'queue'
    _shape = 'tag'
    _dict_fields = BaseState._dict_fields + ['outlets', 'resource']

    def __init__(self, name=None, outlets=None, resource=None):
        super().__init__(name, next)
        self._outlets = outlets or []
        self.resource = resource

    def add_outlet(self, next):
        self._outlets.append(next)
        return self

    @property
    def outlets(self):
        resp = []
        for next in self._outlets:
            if not isinstance(next, str):
                next = next.name
            resp.append(next)
        return resp

    @outlets.setter
    def outlets(self, outlets):
        self._outlets = outlets

    @property
    def next(self):
        return None

    @next.setter
    def next(self, next):
        pass

    def next_branches(self):
        return self.outlets

    def run(self, context, event, *args, **kwargs):
        resp = None
        context.logger.debug(f'Queue state {self.name}, event={event.body}')
        if self._object_type == INIT_REMOTE_API:
            return self._fn(event)

        for next in self.outlets:
            new_event = copy(event)
            next_obj = self._parent[next]
            resp = next_obj.run(context, new_event, *args, **kwargs)
        return resp
