from flowless.serving import NewModelServer, ModelRouter
from flowless import MLTaskSpec, MLTaskRouter, MLTaskChoice, save_graph
from flowless.root import MLTaskRoot


class Event(object):

    def __init__(self, body=None, content_type=None,
                 headers=None, method=None, path=None):
        self.id = 0
        self.body = body
        self.content_type = content_type
        self.trigger = None
        self.headers = headers or {}
        self.method = method
        self.path = path or '/'
        self.end = False


class Getlist:
    def __init__(self, context, **kwargs):
        self.root = context.root

    def do(self, x):
        print('LIST ', x)
        return {'models': list(self.root['router'].keys())}

class TClass:
    def do(self, x):
        print('XXX ', x)
        return x * 3


class MClass(NewModelServer):

    def load(self):
        print('loading')

    def predict(self, request):
        print('predict:', request)
        resp = request['data'][0] * self.get_param('z')
        print('resp:', resp)
        return resp


m1 = MLTaskSpec('m1', class_name='MClass', class_params={'z': 100})
m2 = MLTaskSpec('m2', class_name='MClass', class_params={'z': 200})
m3 = MLTaskSpec('m3', class_name='MClass', class_params={'z': 300})#, resource='f2')

p = MLTaskRoot('root', start_at='if').add_states(
    MLTaskChoice('if', default='router')
        .add_choice('event.path.strip("/") == "v1/models"', 'list-models'),
    MLTaskSpec('list-models', class_name='Getlist', next=''),
    MLTaskRouter('router', routes=[m1, m2, m3], class_name=ModelRouter, class_params={}),
)

p.default_resource = 'f1'
p.resources = {'f2': {'url': 'http://localhost:5000'}}
print(p.to_yaml())
p.start('f1', namespace=globals())

e = Event('{"data": [5]}', path='/v1/models/m2')
print(p.run(None, e))

save_graph(p, "js/data.json")
