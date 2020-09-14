

import time
from multiprocessing.dummy import freeze_support
from pprint import pprint

from flowless import TaskState, RouterState, ChoiceState, FlowRoot, save_graph, QueueState
from flowless.deploy import deploy_pipline
from flowless.states.router import ParallelRouter
from flowless.demo_states import ModelClass, Test1Class, Test2Class



def f5(x):
    return x * 7

m1 = TaskState('m1', class_name='ModelClass', class_params={'z': 100})
m2 = TaskState('m2', class_name='ModelClass', class_params={'z': 200})
m3 = TaskState('m3', class_name='ModelClass', class_params={'z': 300})

p = FlowRoot('root', start_at='ingest', trace=2).add_states(
    TaskState('ingest', class_name=Test1Class),
    ChoiceState('if', default='data-prep')
        .add_choice('event.body==10', 'stream')
        .add_choice('event.body==7', 'update-db'),
    TaskState('data-prep', class_name='Test1Class', resource='f2'),
    RouterState('router', routes=[m1, m2, m3], class_name=ParallelRouter, class_params={'executor': ''}),
    QueueState('stream', outlets=['update-db'], resource=''),
    TaskState('update-db', handler='json.dumps'),
)

p.default_resource = 'f1'
p.streams_path = 'x'
p.add_resource('st', 'stream', '')
p.add_resource('f1', 'function', '')
p.add_resource('f2', 'function', 'hub://model_server', endpoint= 'http://localhost:5000')
print(p.to_yaml())

p.export('p.json')

p.prepare('f1')
deploy_pipline(p)
exit(0)

print(p.init('f1', namespace=globals()))
save_graph(p, "js/data.json")
print(p.run(10, from_state='if'))



# for process executor
# if __name__ == '__main__':
#     __spec__ = None
#     freeze_support()
#     print(p.run(10))

