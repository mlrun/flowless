import json
from io import BytesIO
from typing import Dict
from urllib.request import urlopen
from datetime import datetime

from mlrun.artifacts import get_model, ModelArtifact
from mlrun.platforms.iguazio import OutputStream


class ModelRouter:
    def __init__(self, context, state, **kwargs):
        self.context = context
        self.state = state
        self.url_prefix = kwargs.get('url_prefix', '/v1/models')

    def parse_event(self, event):
        parsed_event = {'data': []}
        try:
            if not isinstance(event.body, dict):
                body = json.loads(event.body)
            else:
                body = event.body
            if 'data_url' in body:
                # Get data from URL
                url = body['data_url']
                self.context.logger.debug_with('downloading data', url=url)
                data = urlopen(url).read()
                sample = BytesIO(data)
                parsed_event['data'].append(sample)
            else:
                parsed_event = body

        except Exception as e:
            if getattr(event, 'content_type', '').startswith('image/'):
                sample = BytesIO(event.body)
                parsed_event['data'].append(sample)
                parsed_event['content_type'] = event.content_type
            else:
                raise Exception("Unrecognized request format: %s" % e)

        return parsed_event

    def post_init(self):
        # Verify that models are loaded
        keys = self.state.keys()
        assert len(keys) > 0, (
            "No models were loaded!\n Please register child models"
        )
        self.context.logger.info(f'Loaded {list(keys)}')

    def select_child(self, event, body):
        urlpath = getattr(event, 'path', '')
        subpath = model = ''
        if urlpath:
            if not urlpath.startswith(self.url_prefix):
                raise ValueError(f'illegal path prefix {urlpath}, must start with {self.url_prefix}')
            urlpath = urlpath[len(self.url_prefix):].strip('/')
            if not urlpath:
                return None, ''
            segments = urlpath.split('/')
            model = segments[0]
            if len(segments) > 1:
                subpath = '/'.join(segments[1:])

        model = model or body.get('model', list(self.state.keys())[0])
        subpath = body.get('operation', subpath)

        if model not in self.state.keys():
            models = '| '.join(self.state.keys())
            raise ValueError(f'model {model} doesnt exist, available models: {models}')

        return self.state[model], subpath

    def do(self, event, *args, **kwargs):
        body = self.parse_event(event)

        child, subpath = self.select_child(event, body)
        if not child:
            return {'models': list(self.state.keys())}

        self.context.logger.debug(f'router run child {child.fullname}, body={body}')
        response = child.run(self.context, body, subpath=subpath)
        return response

    def preprocess(self, request: Dict) -> Dict:
        return request

    def postprocess(self, request: Dict) -> Dict:
        return request


class NewModelServer:
    def __init__(self, context, name: str, model_dir: str = None, model=None, **kwargs):
        self.name = name
        self.context = context
        self.ready = False
        self.model_dir = model_dir
        self.model_spec = None
        self._params = kwargs
        self._model_logger = None
        if context and getattr(context, 'root'):
            self._params = context.root.add_root_params(self._params)
            self._model_logger = ModelLogPusher(self, context.root.server_context)

        self.metrics = {}
        self.labels = {}
        if model:
            self.model = model
            self.ready = True

    def post_init(self):
        if not self.ready:
            self.load()
            self.ready = True

    def get_param(self, key: str, default=None):
        return self._params.get(key, default)

    def get_model(self, suffix=''):
        model_file, self.model_spec, extra_dataitems = get_model(self.model_dir, suffix)
        if self.model_spec and self.model_spec.parameters:
            for key, value in self.model_spec.parameters.items():
                self._params[key] = value
        return model_file, extra_dataitems

    def load(self):
        if not self.ready and not self.model:
            raise ValueError('please specify a load method or a model object')

    def do(self, event, *args, **kwargs):
        start = datetime.now()
        request = self.preprocess(event)
        request = self.validate(request)
        if kwargs.get('subpath', '') == 'explain':
            response = self.explain(request)
        else:
            response = self.predict(request)
        response = self.postprocess(response)
        if self._model_logger:
            self._model_logger.push(start, request, response)
        return response


    def validate(self, request):
        if "data" not in request:
            raise Exception("Expected key \"data\" in request body")

        if not isinstance(request["data"], list):
            raise Exception("Expected \"data\" to be a list")

        return request

    def preprocess(self, request: Dict) -> Dict:
        return request

    def postprocess(self, request: Dict) -> Dict:
        return request

    def predict(self, request: Dict) -> Dict:
        raise NotImplementedError

    def explain(self, request: Dict) -> Dict:
        raise NotImplementedError


class ModelLogPusher:
    def __init__(self, model, server_context, output_stream=None):
        self.model = model
        self.server_context = server_context
        self.stream_batch = server_context.stream_batch
        self.stream_sample = server_context.stream_sample
        self.output_stream = output_stream or server_context.output_stream
        self._sample_iter = 0
        self._batch_iter = 0
        self._batch = []

    def base_data(self):
        base_data = {
            'class': self.model.__class__.__name__,
            'worker': self.server_context.worker,
            'model': self.model.name,
            'host': self.server_context.hostname,
        }
        if getattr(self.model, 'labels', None):
            base_data['labels'] = self.model.labels
        return base_data

    def push(self, start, request, resp):
        self._sample_iter = (self._sample_iter + 1) % self.stream_sample
        if self.output_stream and self._sample_iter == 0:
            microsec = (datetime.now() - start).microseconds

            if self.stream_batch > 1:
                if self._batch_iter == 0:
                    self._batch = []
                self._batch.append([request, resp, str(start), microsec, self.model.metrics])
                self._batch_iter = (self._batch_iter + 1) % self.stream_batch

                if self._batch_iter == 0:
                    data = self.base_data()
                    data['headers'] = ['request', 'resp', 'when', 'microsec', 'metrics']
                    data['values'] = self._batch
                    self.output_stream.push([data])
            else:
                data = self.base_data()
                data['request'] = request
                data['resp'] = resp
                data['when'] = str(start)
                data['microsec'] = microsec
                if getattr(self.model, 'metrics', None):
                    data['metrics'] = self.model.metrics
                self.output_stream.push([data])

