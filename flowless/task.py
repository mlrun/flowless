from flowless.base import MLTaskSpecBase, resource_params


class MLTaskSpec(MLTaskSpecBase):
    kind = 'task'
    _dict_fields = ['kind', 'name', 'class_name', 'class_params', 'next'] + resource_params

    def __init__(self, name=None, class_name=None,
                 class_params=None, next=None, resource=None, url=None):
        super().__init__(name, next)
        self.class_name = class_name
        self.class_params = class_params or {}
        self.resource = resource
        self.transport = None
        self.url = url
        self.message_format = None

    def _init_object(self, current_resource, namespace):
        if not self.class_name:
            raise ValueError(f'class_name is not defined for {self.name}')
        print(f'init class {self.class_name} in {self.name}')
        if self.class_name not in namespace:
            raise ValueError(f'state {self.name} init failed, class {self.class_name} not found')
        self._object = namespace[self.class_name](**self.class_params)

    def run(self, context, event, *args, **kwargs):
        print(f'running state {self.fullname}')
        if not self._object:
            raise RuntimeError(f'state {self.name} run failed, class {self.class_name} not initialized')
        context.state = self
        return self._object.do(context, event)


class MLModelSpec(MLTaskSpec):
    kind = 'model'
    _dict_fields = MLTaskSpec._dict_fields + ['model_path']
    _shape = 'round-octagon'

    def __init__(self, name=None, model_path=None, class_name=None,
                 class_params=None, next=None, resource=None, url=None):
        super().__init__(name, class_name, class_params, next=next, resource=resource, url=url)
        self.model_path = model_path


class MLTaskEndpoint(MLTaskSpecBase):
    kind = 'endpoint'
    _dict_fields = MLTaskSpecBase._dict_fields + ['url', 'protocol', 'headers', 'method']
    _shape = 'round-tag'

    def __init__(self, name=None, url=None, next=None):
        super().__init__(name, next)
        self.method = None
        self.protocol = None
        self.headers = None
        self.url = url
