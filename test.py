

import time
from multiprocessing.dummy import freeze_support
from pprint import pprint

from flowless import TaskState, RouterState, ChoiceState, FlowRoot, save_graph
from flowless.deploy import deploy_pipline
from flowless.states.router import ParallelRouter


class BaseClass:
    def __init__(self, context, state):
        self.context = context
        self.state = state


class MClass:
    def __init__(self, z=None):
        self.z = z

    def do(self, x):
        time.sleep(1)
        print('XX:', x)
        return x['data'] + self.z


class T1Class(BaseClass):
    def do(self, x):
        print('ZZ:', x)
        return x * 2


class T2Class(BaseClass):
    def xo(self, x):
        print('YY:', x)
        return x * 3


def f5(x):
    return x * 7

m1 = TaskState('m1', class_name='MClass', class_params={'z': 100})
m2 = TaskState('m2', class_name='MClass', class_params={'z': 200})
m3 = TaskState('m3', class_name='MClass', class_params={'z': 300})

p = FlowRoot('root', start_at='ingest', trace=2).add_states(
    TaskState('ingest', class_name=T1Class),
    ChoiceState('if', default='data-prep')
        .add_choice('event.body==10', 'post-process')
        .add_choice('event.body==7', 'update-db'),
    TaskState('data-prep', class_name='T1Class', resource='f2'),
    RouterState('router', routes=[m1, m2, m3], class_name=ParallelRouter, class_params={'executor': ''}),
    TaskState('post-process', class_name='T2Class', handler='xo'),
    TaskState('update-db', handler='json.dumps'),
)

p.default_resource = 'f1'
p.add_resource('f1', 'function', '')
p.add_resource('f2', 'function', 'hub://describe', endpoint= 'http://localhost:5000')
#print(p.to_yaml())
deploy_pipline(p)

p.prepare('f1')

print(p.init('f1', namespace=globals()))
save_graph(p, "js/data.json")
print(p.run(10))



# for process executor
# if __name__ == '__main__':
#     __spec__ = None
#     freeze_support()
#     print(p.run(10))

