import time
from multiprocessing.dummy import freeze_support

from flowless import MLModelSpec, MLTaskSpec, MLTaskRouter, MLTaskEndpoint, build_graph
from flowless.flow import MLTaskRoot


class TaskRunContext:
    def __init__(self):
        self.state = None


class BaseClass:
    def __init__(self, context, state):
        self.context = context
        self.state = state


class MClass:
    def __init__(self, context, state, z=None):
        self.z = z

    def do(self, x):
        time.sleep(2)
        return x + self.z


class T1Class(BaseClass):
    def do(self, x):
        return x * 2


class T2Class(BaseClass):
    def do(self, x):
        return x * 3


m1 = MLModelSpec('m1', class_name='MClass', class_params={'z': 100})
m2 = MLModelSpec('m2', class_name='MClass', class_params={'z': 200})
m3 = MLModelSpec('m3', class_name='MClass', class_params={'z': 300})

p = MLTaskRoot('root', start_at='ingest').add_states(
    MLModelSpec('ingest', class_name='T1Class'),
    MLTaskSpec('data-prep', class_name='T1Class', resource='f1'),
    MLTaskRouter('router', routes=[m1, m2, m3], class_params={'executor': 'thread'}),
    MLTaskSpec('post-process', class_name='T2Class'),
    MLTaskEndpoint('update-db')
)

nodes=[]
edges=[]
print(build_graph(p, nodes, edges))
# pprint(nodes)
# pprint(edges)
#exit(0)

#print(p.to_yaml())

print(p.init_objects(None, 'f1', globals(), 'f1'))

import json, os
serve_dir = "js/data.json"
#nodes, edges = p.get_graph()
with open(serve_dir, "w", encoding="utf-8") as fp:
    fp.write(json.dumps({'nodes': nodes, 'edges': edges}, cls=None, indent=2))

if __name__ == '__main__':
    __spec__ = None
    freeze_support()
    print(p.run(5))

