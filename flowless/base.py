from pprint import pprint

from mlrun.model import ModelObj

# kinds: model, router, parallel, map
import flowless

resource_params = ['resource', 'transport', 'message_format', 'url']


class MLTaskSpecBase(ModelObj):
    kind = 'base'
    _dict_fields = ['kind', 'name', 'next', 'end']
    _shape = 'round-rectangle'

    def __init__(self, name=None, next=None):
        self.name = name
        self._object = None
        self._parent = None
        self.comment = None
        self._next = next
        self._next_obj = None
        self.end = None

    @property
    def next(self):
        if self._next_obj:
            return self._next_obj.fullname
        return self._next

    @next.setter
    def next(self, next):
        if hasattr(next, 'kind'):
            self._next_obj = next
        else:
            self._next = next

    def get_children(self):
        return []

    def after(self, other):
        other._next_obj = self
        return self

    def _init_object(self, context, current_resource, namespace):
        pass

    def init_objects(self, context, current_resource, namespace, parent_resource=None):
        resource = getattr(self, 'resource', None) or parent_resource
        if current_resource in ['*', resource]:
            self._init_object(context, current_resource, namespace)
        for child in self.get_children():
            child.init_objects(context, current_resource, namespace, resource)

    @property
    def fullname(self):
        name = self.name
        if self._parent and not self._parent.kind == 'root':
            name = '.'.join([self._parent.fullname, name])
        return name

    def run(self, event, *args, **kwargs):
        return event


class TaskList:
    def __init__(self):
        self._tasks = {}

    def values(self):
        return self._tasks.values()

    def keys(self):
        return self._tasks.keys()

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
