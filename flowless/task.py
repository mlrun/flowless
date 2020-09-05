import inspect

from flowless.base import MLTaskSpecBase
from flowless.transport import new_session


class MLTaskSpec(MLTaskSpecBase):
    kind = 'task'
    _default_class = None
    _dict_fields = ['kind', 'name', 'class_name', 'class_params', 'next', 'resource', 'transport', 'subpath']

    def __init__(self, name=None, class_name=None, class_params=None,
                 next=None, resource=None, transport=None, subpath=None):
        super().__init__(name, next)
        self._class_object = None
        self.class_name = class_name
        if class_name and not isinstance(class_name, str):
            self.class_name = class_name.__name__
            self._class_object = class_name

        self.class_params = class_params or {}
        self.resource = resource
        self.transport = transport
        self.subpath = subpath

    def _init_object(self, context, namespace):
        if self._is_remote:
            self._object = new_session(self, self._root.resources[self.resource])
            return

        if not self.class_name and not self._class_object:
            if self._default_class:
                self._object = init_class(self._default_class, context, self, **self.class_params)
                return
            raise ValueError(f'class_name is not defined for {self.name}')

        context.logger.debug(f'init class {self.class_name} in {self.name}')
        if self._class_object:
            self._object = init_class(self._class_object, context, self, **self.class_params)
            return

        if self.class_name not in namespace:
            raise ValueError(f'state {self.name} init failed, class {self.class_name} not found')
        self._object = init_class(namespace[self.class_name], context, self, **self.class_params)

    def _post_init(self):
        if hasattr(self._object, 'post_init'):
            self._object.post_init()

    def run(self, context, event, *args, **kwargs):
        context.logger.debug(f'running state {self.fullname}, remote: {self._is_remote}')
        if not self._object:
            raise RuntimeError(f'state {self.name} run failed, class {self.class_name}'
                               ' or remote session not initialized')
        event = self._object.do(event)
        if self.next:
            next_obj = self._parent[self.next]
            return next_obj.run(context, event, *args, **kwargs)
        return event


def init_class(object, context, state, **params):
    args = inspect.signature(object.__init__).parameters
    if 'context' in args:
        params['context'] = context
    if 'state' in args:
        params['state'] = state
    if 'name' in args:
        params['name'] = state.name
    return object(**params)


