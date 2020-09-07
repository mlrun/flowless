from pprint import pprint

from mlrun.model import ModelObj

import flowless
from flowless.transport import new_session


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


class TaskList:
    def __init__(self):
        self._tasks = {}

    def values(self):
        return self._tasks.values()

    def keys(self):
        return self._tasks.keys()

    def __len__(self):
        return len(self._tasks)

    def __getitem__(self, name):
        return self._tasks[name]

    def to_list(self):
        return [t.to_dict() for t in self._tasks.values()]

    @classmethod
    def from_list(cls, tasks=None, parent=None):
        if tasks is None:
            return cls()
        if not isinstance(tasks, list):
            raise ValueError('tasks must be a list')

        new_obj = cls()
        if tasks:
            for val in tasks:
                val = new_obj.add(val)
                val._parent = parent
        return new_obj

    def add(self, task, name=None):
        if isinstance(task, dict):
            kind = task.get('kind', 'model')
            task = flowless.task_kinds[kind].from_dict(task)
        task.name = name or task.name
        self._tasks[task.name] = task
        return task
