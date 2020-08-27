from flowless.base import _get_node_obj, _new_edge
from flowless.flow import MLTaskHost
from flowless.router import MLTaskRouter
from flowless.task import MLTaskSpec, MLModelSpec, MLTaskEndpoint

task_kinds = {'task': MLTaskSpec,
              'model': MLModelSpec,
              'router': MLTaskRouter,
              'taskhost': MLTaskHost,
              'endpoint': MLTaskEndpoint}


def build_graph(step, nodes=[], edges=[], parent=None):
    print(step)
    if parent == 'root':
        parent = None

    if hasattr(step, 'states'):
        if parent:
            nodes.append(_get_node_obj(name=step.fullname, text=step.name,
                                       shape=step._shape, parent=parent))
        if step.next:
            edges.append(_new_edge(step.fullname, step.next))
        for state in step._states.values():
            build_graph(state, nodes, edges, step.fullname)

    elif hasattr(step, 'routes'):
        switch_name = step.fullname + '$'
        edges += _new_edge(switch_name, step.next)
        nodes.append(_get_node_obj(name=switch_name, text=step.name, parent=parent))
        nodes.append(_get_node_obj(name=step.fullname, text='', shape='star', parent=switch_name))
        for route in step._routes.values():
            build_graph(route, nodes, edges, switch_name)
            edges += _new_edge(step.fullname, route.fullname)

    else:
        edges += _new_edge(step.fullname, step.next)
        nodes.append(_get_node_obj(name=step.fullname, text=step.name,
                                   shape=step._shape, parent=parent))
