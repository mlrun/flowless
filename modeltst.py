from flowless.common import Event
from flowless.serving import NewModelServer, ModelRouter
from flowless import TaskState, RouterState, ChoiceState, FlowRoot, save_graph


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


m1 = TaskState('m1', class_name='MClass', class_params={'z': 100})
m2 = TaskState('m2', class_name='MClass', class_params={'z': 200})
m3 = TaskState('m3', class_name='MClass', class_params={'z': 300})#, resource='f2')

p = FlowRoot('root', start_at='router', trace=2).add_states(
    RouterState('router', routes=[m1, m2, m3], class_name=ModelRouter, class_params={}),
    TaskState('xyz', class_name='TClass'),
)

p.default_resource = 'f1'
p.resources = {'f2': {'url': 'http://localhost:5000'}}
print(p.to_yaml())
p.init('f1', namespace=globals())

e = Event('{"data": [5]}', path='/v1/models')
print('resp:', p.run(e))
print(e._trace_log)

save_graph(p, "js/data.json")
