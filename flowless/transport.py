import json

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

http_adapter = HTTPAdapter(
    max_retries=Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
)

def new_session(step, resource):
    transport = step.transport
    return HttpTransport(step, resource)


class HttpTransport:
    def __init__(self, step, resource):
        self.url = resource.get('url', '') + (step.subpath or '')
        self.headers = resource.get('headers', None)
        self.method = resource.get('method', 'POST')
        self.format = 'json'
        self.user = resource.get('user', '')
        self.password = resource.get('password', '')
        self.token = resource.get('token', '')
        self._session = requests.Session()
        self._session.mount('http://', http_adapter)
        self._session.mount('https://', http_adapter)

    def do(self, event):
        kwargs = {'headers': self.headers}
        if self.user:
            kwargs['auth'] = (self.user, self.password)
        elif self.token:
            kwargs['headers'] = {'Authorization': 'Bearer ' + self.token}
        if self.method != 'GET':
            if isinstance(event, (str, bytes)):
                kwargs['data'] = event
            else:
                kwargs['json'] = event

        try:
            resp = self._session.request(self.method, self.url, verify=False, **kwargs)
        except OSError as err:
            raise OSError(f'error: cannot run function at url {self.url}, {err}')
        if not resp.ok:
            raise RuntimeError(f'bad function response {resp.text}')

        data = resp.content
        if self.format == 'json' or resp.headers['content-type'] == 'application/json':
            data = json.loads(data)
        return data
