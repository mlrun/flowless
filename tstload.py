import os
import flowless
from flowless.common import TaskRunContext, Event
from flowless.demo_states import ModelClass, Test1Class, Test2Class
from flowless.loader import init_pipeline_context, pipeline_handler
from flowless.states.router import ParallelRouter

os.environ['FROM_STATE'] = 'router.m1'
os.environ['RESOURCE_NAME'] = 'f1'

ctx = TaskRunContext()
init_pipeline_context(ctx, globals(), 'p.json')
resp = pipeline_handler(ctx, Event(body={'data': 99}))
print(resp)


#p = flowless.load_pipeline('p.json')
#print(p.to_yaml())

#p.prepare('f1')
#deploy_pipline(p)

#print(p.init('f1', namespace=globals()))
#save_graph(p, "js/data.json")
#print('AAAAA', p.run())

