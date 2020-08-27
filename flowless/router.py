from .task import MLTaskSpec
from .base import TaskList


class MLTaskRouter(MLTaskSpec):
    kind = 'router'
    _dict_fields = MLTaskSpec._dict_fields + ['routes', 'hide_routes', 'weight_table']

    def __init__(self, name=None, routes=None, class_name=None,
                 class_params=None, resource=None, next=None, weight_table=None):
        super().__init__(name, class_name, class_params, next=next, resource=resource)
        self._routes = None
        self.routes = routes
        self.hide_routes = None
        self.weight_table = weight_table

    def get_children(self):
        return self._routes.values()

    @property
    def routes(self):
        return self._routes.to_list()

    @routes.setter
    def routes(self, routes):
        self._routes = TaskList.from_list(routes, self)

    def add_route(self, route):
        route = self._routes.add(route)
        route._parent = self
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

