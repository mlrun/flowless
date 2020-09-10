import inspect
from pprint import pprint

from mlrun.model import ModelObj

import flowless
from ..transport import new_session


class BaseState(ModelObj):
    kind = 'base'
    _dict_fields = ['kind', 'name', 'next', 'end']
    _shape = 'round-rectangle'

    def __init__(self, name=None, next=None):
        self.name = name
        self._fn = None
        self._root = None
        self._parent = None
        self.comment = None
        self.next = next
        self.end = None
        self._is_remote = False

    @property
    def parent_name(self):
        if self._parent and not self._parent.kind == 'root':
            return self._parent.fullname
        return ''

    def set_parent(self, parent):
        self._parent = parent

    def get_children(self):
        return []

    def before(self, next):
        if hasattr(next, 'kind'):
            self.next = next.name
        else:
            self.next = next
        return self

    def after(self, other):
        other.next = self.name
        return self

    def _init_object(self, context, namespace):
        pass

    def _post_init(self):
        pass

    def init_objects(self, context, current_resource, namespace, parent_resource=None):
        resource = getattr(self, 'resource', None)
        self._root = getattr(context, 'root', None)
        if resource and current_resource not in ['*', resource]:
            self._is_remote = True
            if self.resource not in self._root.resources:
                raise RuntimeError(f'resource {self.resource} not defined in root')
            self._fn = new_session(self, self._root.resources[self.resource]).do

        resource = resource or parent_resource
        if current_resource in ['*', resource]:
            self._init_object(context, namespace)

        for child in self.get_children():
            if child.next:
                if child.next not in self.keys():
                    raise ValueError(f'next state ({child.next} not found in {self.fullname}')
                child.before(self[child.next])
            child.init_objects(context, current_resource, namespace, resource)

        self._post_init()

    @property
    def fullname(self):
        name = self.name
        if self.kind == 'root':
            raise ValueError('GET FULLNAME FOR ROOT!')
        if not self._parent:
            raise ValueError('parent is not set, add this task to a flow or a router')
        if not self._parent.kind == 'root':
            name = '.'.join([self._parent.fullname, name])
        return name

    def run(self, context, event, *args, **kwargs):
        return event.body


class StateList:
    def __init__(self):
        self._children = {}

    def values(self):
        return self._children.values()

    def keys(self):
        return self._children.keys()

    def __len__(self):
        return len(self._children)

    def __getitem__(self, name):
        return self._children[name]

    def to_list(self):
        return [t.to_dict() for t in self._children.values()]

    @classmethod
    def from_list(cls, states=None, parent=None):
        if states is None:
            return cls()
        if not isinstance(states, list):
            raise ValueError('states must be a list')

        new_obj = cls()
        if states:
            for val in states:
                val = new_obj.add(val)
                val._parent = parent
        return new_obj

    def add(self, state, name=None):
        if isinstance(state, dict):
            kind = state.get('kind', 'model')
            state = flowless.task_kinds[kind].from_dict(state)
        state.name = name or state.name
        self._children[state.name] = state
        return state


class ObjDict:
    def __init__(self, child_cls):
        self._children = {}
        self._child_cls = child_cls

    def values(self):
        return self._children.values()

    def keys(self):
        return self._children.keys()

    def items(self):
        return self._children.items()

    def __len__(self):
        return len(self._children)

    def __getitem__(self, name):
        return self._children[name]

    def __setitem__(self, key, item):
        self._children[key] = item

    def to_dict(self):
        return {k: v.to_dict() for k, v in self._children.items()}

    @classmethod
    def from_dict(cls, child_cls, children=None):
        if children is None:
            return cls(child_cls)
        if not isinstance(children, dict):
            raise ValueError('children must be a dict')

        new_obj = cls(child_cls)
        fields = child_cls._dict_fields
        if not fields:
            fields = list(inspect.signature(child_cls.__init__).parameters.keys())


        if children:
            for name, struct in children.items():
                child_obj = child_cls()
                if struct:
                    for key, val in struct.items():
                        if key in fields:
                            setattr(child_obj, key, val)
                new_obj._children[name] = child_obj
        return new_obj

    def set(self, child, name=None):
        if isinstance(child, dict):
            child = self._child_cls.from_dict(child)
        self._children[name] = child
        return child


class StateResource(ModelObj):
    _dict_fields = ['kind', 'url', 'spec', 'endpoint']

    def __init__(self, kind=None, uri=None, spec=None, endpoint=None):
        self.kind = kind
        self.uri = uri
        self.spec = spec
        self.endpoint = endpoint
        self.user = None
        self.password = None
        self.token = None
        self.skip_deploy = None

