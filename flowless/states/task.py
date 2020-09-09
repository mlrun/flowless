import inspect

from .base import BaseState
from flowless.transport import new_session


class TaskState(BaseState):
    kind = 'task'
    _dict_fields = ['kind', 'name', 'class_name', 'class_params', 'handler',
                    'next', 'resource', 'transport', 'subpath', 'full_event']

    def __init__(self, name=None, class_name=None, class_params=None, handler=None,
                 next=None, resource=None, transport=None, subpath=None, full_event=None):
        super().__init__(name, next)
        if callable(handler) and (class_name or class_params):
            raise ValueError('cannot specify function pointer (handler) and class name/params')

        self._class_object = None
        self.class_name = class_name
        if class_name and not isinstance(class_name, str):
            self.class_name = class_name.__name__
            self._class_object = class_name

        self.class_params = class_params or {}
        self._object = None
        self.full_event = None
        self.handler = handler
        self.resource = resource
        self.transport = transport
        self.subpath = subpath

    def _init_object(self, context, namespace):
        # init remote session (todo: only for next)
        if self._is_remote:
            self._fn = new_session(self, self._root.resources[self.resource]).do
            return

        # link to function
        if self.handler and not self.class_name:
            if callable(self.handler):
                self._fn = self.handler
            elif self.handler in namespace:
                self._fn = namespace[self.handler]
            else:
                raise ValueError(f'state {self.name} init failed, function {self.handler} not found')
            return

        # init and link class/function
        if not self._class_object and (not self.class_name or self.class_name not in namespace):
            raise ValueError(f'state {self.name} init failed, class {self.class_name} not found')
        context.logger.debug(f'init class {self.class_name} in {self.name}')
        self._object = init_class(self._class_object or namespace[self.class_name],
                                  context, self, **self.class_params)

        handler = self.handler or 'do'
        if not self.handler and hasattr(self._object, 'do_event'):
            handler = 'do_event'
            self.full_event = True

        if not hasattr(self._object, handler):
            raise ValueError(f'handler {handler} not found in class {self._object.__name__}')
        self._fn = getattr(self._object, handler)

    def _post_init(self):
        if self._object and hasattr(self._object, 'post_init'):
            self._object.post_init()

    def run(self, context, event, *args, **kwargs):
        context.logger.debug(f'running state {self.fullname}, remote: {self._is_remote}')
        if not self._fn:
            raise RuntimeError(f'state {self.name} run failed, function '
                               ' or remote session not initialized')
        try:
            if self.full_event:
                body = self._fn(event)
            else:
                body = self._fn(event.body)
        except Exception as e:
            fullname = self.fullname
            context.logger.error(f'step {fullname} run failed, {e}')
            event.add_trace(event.id, fullname, 'fail', e, verbosity=context.root.trace)
            raise RuntimeError(f'step {fullname} run failed, {e}')

        event.add_trace(event.id, self.fullname, 'ok', body, verbosity=context.root.trace)
        event.body = body
        if self.next and not getattr(event, 'terminated', None):
            next_obj = self._parent[self.next]
            return next_obj.run(context, event, *args, **kwargs)
        return body


def init_class(object, context, state, **params):
    args = inspect.signature(object.__init__).parameters
    if 'context' in args:
        params['context'] = context
    if 'state' in args:
        params['state'] = state
    if 'name' in args:
        params['name'] = state.name
    return object(**params)


