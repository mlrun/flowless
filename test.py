import time
from multiprocessing.dummy import freeze_support
from pprint import pprint

from flowless import MLModelSpec, MLTaskSpec, MLTaskRouter, MLTaskEndpoint, build_graph, save_graph
from flowless.flow import MLTaskChoice
from flowless.root import MLTaskRoot


class BaseClass:
    def __init__(self, context, state):
        self.context = context
        self.state = state


class MClass:
    def __init__(self, z=None):
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
m4 = MLModelSpec('m4', class_name='MClass', class_params={'z': 300})

p = MLTaskRoot('root', start_at='ingest').add_states(
    MLModelSpec('ingest', class_name=T1Class),
    # MLTaskChoice('if', default='data-prep')
    #     .add_choice('event==10', 'post-process')
    #     .add_choice('event==7', m4),
    MLTaskSpec('data-prep', class_name='T1Class', resource='f1'),
    MLTaskRouter('router', routes=[m1, m2, m3], class_params={'executor': 'thread'}),
    MLTaskSpec('post-process', class_name='T2Class'),
    MLTaskEndpoint('update-db'),
)


print(p.to_yaml())

print(p.start('*', namespace=globals()))


save_graph(p, "js/data.json")

if __name__ == '__main__':
    __spec__ = None
    freeze_support()
    print(p.run(5))

