from flowless import TaskState, RouterState, ChoiceState, FlowRoot, save_graph, QueueState
from flowless.deploy import deploy_pipline
from flowless.states.router import ParallelRouter
from flowless.demo_states import *


# define model states
m1 = TaskState('m1', class_name='ModelClass', class_params={'z': 100})
m2 = TaskState('m2', class_name='ModelClass', class_params={'z': 200})
m3 = TaskState('m3', class_name='ModelClass', class_params={'z': 300})


# assume message is {"op":"predict", "data": [1, 2, 3]}
# build the pipeline, Note: by default next step is the next one in the list
pipe = FlowRoot('root', start_at='convert', trace=2)
pipe.add_states(
    TaskState('convert', handler='json.loads'),
    ChoiceState('if', default='fail')
        .add_choice('event.body["op"]=="push"', 'stream')
        .add_choice('event.body["op"]=="predict"', 'router'),
    TaskState('fail', class_name='Message', class_params={'msg': 'got err!!'}, next=''),
    RouterState('router', routes=[m1, m2, m3], class_name=ParallelRouter, class_params={'executor': ''}, next='to-json'),
    QueueState('stream', outlets=['process-stream'], resource='st1'),
    TaskState('process-stream', class_name='Echo', next=''),
    TaskState('to-json', handler='json.dumps'),
)

pipe.default_resource = 'f1'
pipe.streams_path = 'users/admin/streams'
pipe.add_resource('st1', 'stream', '')
pipe.add_resource('f1', 'function', '')
pipe.add_resource('f2', 'function', 'hub://model_server', endpoint='http://localhost:5000')
print(pipe.to_yaml())

pipe.export('p.json')

pipe.prepare('f1')
deploy_pipline(pipe)
#exit(0)
save_graph(pipe, "js/data.json")

print(pipe.init('f1', namespace=globals()))
#print(pipe.run('{"op":"predict", "data": [1, 2, 3]}'))
print(pipe.run('{"op":"push", "data": [1, 2, 3]}'))
#print(pipe.run('{"op":"x", "data": [1, 2, 3]}'))
