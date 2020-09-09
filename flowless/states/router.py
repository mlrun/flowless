import concurrent
from copy import copy

from .task import TaskState
from .base import TaskList


_fields = copy(TaskState._dict_fields)
_fields.remove('full_event')


class RouterState(TaskState):
    kind = 'router'
    _dict_fields = _fields + ['routes', 'hide_routes']

    def __init__(self, name=None, routes=None, class_name=None,
                 class_params=None, resource=None, next=None):
        super().__init__(name, class_name, class_params, next=next, resource=resource)
        self._routes = None
        self.routes = routes
        self.hide_routes = None
        self.full_event = True

    def get_children(self):
        return self._routes.values()

    def keys(self):
        return self._routes.keys()

    def values(self):
        return self._routes.values()

    @property
    def routes(self):
        return self._routes.to_list()

    @routes.setter
    def routes(self, routes):
        self._routes = TaskList.from_list(routes, self)

    def add_route(self, route):
        route = self._routes.add(route)
        route.set_parent(self)
        return route

    def __getitem__(self, name):
        return self._routes[name]

    def __iadd__(self, route):
        if isinstance(route, list):
            for r in route:
                self.add_route(r)
        else:
            self.add_route(route)
        return self


class ParallelRouter:
    def __init__(self, context, state, **kwargs):
        self.context = context
        self.state = state
        self.executor = kwargs.get('executor', None)

    def _do_parallel(self, event, *args, **kwargs):
        results = []
        children = self.state.get_children()
        if self.executor == 'process':
            executor_class = concurrent.futures.ProcessPoolExecutor
        elif self.executor == 'thread':
            executor_class = concurrent.futures.ThreadPoolExecutor
        else:
            raise ValueError(f'executor value can be "process" or "thread", not {self.executor}')
        with executor_class() as executor:
            new_event = copy(event)
            futures = {executor.submit(child.run, self.context, new_event): child for child in children}
            for future in concurrent.futures.as_completed(futures):
                child = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    print('%r generated an exception: %s' % (child.fullname, exc))
        return results

    def _do_sync(self, event, *args, **kwargs):
        results = []
        children = self.state.get_children()
        for child in children:
            resp = child.run(self.context, event, *args, **kwargs)
            if resp:
                results.append(resp)
        return results

    def do_event(self, event, *args, **kwargs):
        if self.executor:
            results = self._do_parallel(event, *args, **kwargs)
        else:
            results = self._do_sync(event, *args, **kwargs)
        return results
