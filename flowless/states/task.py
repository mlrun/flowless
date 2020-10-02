import inspect
from importlib import import_module

from .base import BaseState, INIT_REMOTE_API
from ..transport import new_session


def create_class(pkg_class: str):
    """Create a class from a package.module.class string

    :param pkg_class:  full class location,
                       e.g. "sklearn.model_selection.GroupKFold"
    """
    splits = pkg_class.split(".")
    clfclass = splits[-1]
    pkg_module = splits[:-1]
    class_ = getattr(import_module(".".join(pkg_module)), clfclass)
    return class_


def create_function(pkg_func: list):
    """Create a function from a package.module.function string

    :param pkg_func:  full function location,
                      e.g. "sklearn.feature_selection.f_classif"
    """
    splits = pkg_func.split(".")
    pkg_module = ".".join(splits[:-1])
    cb_fname = splits[-1]
    pkg_module = __import__(pkg_module, fromlist=[cb_fname])
    function_ = getattr(pkg_module, cb_fname)
    return function_


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
        self.full_event = full_event
        self.handler = handler
        self.resource = resource
        self.transport = transport
        self.subpath = subpath

    def _init_object(self, context, namespace):
        # link to function
        if self.handler and not self.class_name:
            if callable(self.handler):
                self._fn = self.handler
                self.handler = self.handler.__name__
            elif self.handler in namespace:
                self._fn = namespace[self.handler]
            else:
                try:
                    self._fn = create_function(self.handler)
                except (ImportError, ValueError) as e:
                    raise ImportError(f'state {self.name} init failed, function {self.handler} not found')
            context.logger.debug(f'init function {self.handler} in {self.name}')
            return

        if not self.class_name:
            raise ValueError('valid class_name and/or handler must be specified')

        if not self._class_object:
            if self.class_name in namespace:
                self._class_object = namespace[self.class_name]
            else:
                try:
                    self._class_object = create_class(self.class_name)
                except (ImportError, ValueError) as e:
                    raise ImportError(f'state {self.name} init failed, class {self.class_name} not found')

        # init and link class/function
        context.logger.debug(f'init class {self.class_name} in {self.name}')
        self._object = init_class(self._class_object, context, self, **self.class_params)

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
        context.logger.debug(f'running state {self.fullname}, type: {self._object_type}')
        if not self._fn:
            raise RuntimeError(f'state {self.name} run failed, function '
                               ' or remote session not initialized')
        try:
            if self.full_event or self._object_type == INIT_REMOTE_API:
                event = self._fn(event)
            else:
                event.body = self._fn(event.body)
        except Exception as e:
            fullname = self.fullname
            context.logger.error(f'step {fullname} run failed, {e}')
            event.add_trace(event.id, fullname, 'fail', e, verbosity=context.root.trace)
            raise RuntimeError(f'step {fullname} run failed, {e}')

        event.add_trace(event.id, self.fullname, 'ok', event.body, verbosity=context.root.trace)
        resp_status = getattr(event.body, 'status_code', 0)
        if self.next and not getattr(event, 'terminated', None) and resp_status < 300:
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


