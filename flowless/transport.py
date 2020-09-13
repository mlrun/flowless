import json

import requests
from mlrun.platforms.iguazio import split_path
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

http_adapter = HTTPAdapter(
    max_retries=Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
)


def new_session(state, resource):
    if resource.kind == 'stream':
        return V3ioStreamTransport(state, resource)
    return HttpTransport(state, resource)


class HttpTransport:
    def __init__(self, state, resource):
        self.url = resource.endpoint
        self.format = 'json'
        self.state_name = state.name
        self.user = resource.user or ''
        self.password = resource.password or ''
        self.token = resource.token
        self._session = requests.Session()
        self._session.mount('http://', http_adapter)
        self._session.mount('https://', http_adapter)

    def do(self, event):
        headers = event.headers or {}
        headers['next-state'] = self.state_name
        kwargs = {'headers': event.headers or {}}
        if self.user:
            kwargs['auth'] = (self.user, self.password)
        elif self.token:
            headers['Authorization'] = 'Bearer ' + self.token
        kwargs['headers'] = headers
        method = event.method or 'POST'
        if method != 'GET':
            if isinstance(event, (str, bytes)):
                kwargs['data'] = event.body
            else:
                kwargs['json'] = event.body

        url = self.url.strip('/') + event.path
        try:
            resp = self._session.request(method, url, verify=False, **kwargs)
        except OSError as err:
            raise OSError(f'error: cannot run function at url {url}, {err}')
        if not resp.ok:
            raise RuntimeError(f'bad function response {resp.text}')

        data = resp.content
        if self.format == 'json' or resp.headers['content-type'] == 'application/json':
            data = json.loads(data)
        event.body = data
        return event


class V3ioStreamTransport:
    def __init__(self, state, resource):
        import v3io
        self._v3io_client = v3io.dataplane.Client()
        self._container, self._stream_path = split_path(resource.uri)

    def do(self, event):
        data = event.body
        if not data:
            return
        if not isinstance(data, list):
            data = [data]
        records = [{"data": json.dumps(rec)} for rec in data]
        self._v3io_client.put_records(
            container=self._container, path=self._stream_path, records=records
        )
        return None
