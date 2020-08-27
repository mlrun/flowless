from flowless import MLModelSpec, MLTaskSpec, MLTaskRouter, MLTaskEndpoint, build_graph
from flowless.flow import MLTaskRoot


class TaskRunContext:
    def __init__(self):
        self.state = None


class MClass:
    def __init__(self, z=None):
        self.z = z

    def do(self, context, x):
        return x + self.z

class T1Class:
    def do(self, context, x):
        return x * 2

class T2Class:
    def do(self, context, x):
        return x * 3

class RClass:
    def do(self, context, x):
        resp = []
        children = context.state.get_children()
        for child in children:
            print('***', child.fullname)
            resp.append(child.run(context, x))
        return resp


m1 = MLModelSpec('m1', class_name='MClass', class_params={'z': 100})
m2 = MLModelSpec('m2', class_name='MClass', class_params={'z': 200})
#myfunc = MLTaskHost('my-func', states=[prep, r1.after(prep)], resource='f1', start_at='data-prep')

p = MLTaskRoot('root', start_at='ingest').add_states(
    MLModelSpec('ingest', class_name='T1Class'),
    MLTaskSpec('data-prep', class_name='T1Class', resource='f1'),
    MLTaskRouter('router', routes=[m1, m2], class_name='RClass', resource='f1'),
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

print(p.init_objects('f1', globals(), 'f1'))

import json, os
serve_dir = "js/data.json"
#nodes, edges = p.get_graph()
with open(serve_dir, "w", encoding="utf-8") as fp:
    fp.write(json.dumps({'nodes': nodes, 'edges': edges}, cls=None, indent=2))

print(p.run(TaskRunContext(), 5))

