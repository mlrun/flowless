from flowless.flow import MLTaskFlow, MLTaskChoice
from flowless.router import MLTaskRouter
from flowless.task import MLTaskSpec, MLModelSpec, MLTaskEndpoint

task_kinds = {'task': MLTaskSpec,
              'model': MLModelSpec,
              'router': MLTaskRouter,
              'subflow': MLTaskFlow,
              'choice': MLTaskChoice,
              'endpoint': MLTaskEndpoint}

default_shape = 'round-rectangle'


def _get_node_obj(name=None, text=None, shape=None, parent=None):
    if text is None:
        text = name
    data = {"id": name,
            "text": text,
            "shape": shape or default_shape}
    if parent:
        data['parent'] = parent
    return {"data": data}


def _new_edge(source, target, edges=None):
    edges = edges or []
    if source and target:
        edges += [{"data": {"source": source, "target": target}}]
    return edges


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
        nodes.append(_get_node_obj(name=step.fullname, text=step.class_name, shape='star', parent=switch_name))
        for route in step._routes.values():
            build_graph(route, nodes, edges, switch_name)
            edges += _new_edge(step.fullname, route.fullname)

    elif hasattr(step, 'choices'):
        nodes.append(_get_node_obj(name=step.fullname, text=step.name,
                                   shape=step._shape, parent=parent))
        for choice in step.choices:
            next = choice.get('next', '')
            if next:
                if parent:
                    next = '.'.join([parent, next])
                edges += _new_edge(step.fullname, next)
        if step.default:
            edges += _new_edge(step.fullname, step.default)

    else:
        edges += _new_edge(step.fullname, step.next)
        nodes.append(_get_node_obj(name=step.fullname, text=step.name,
                                   shape=step._shape, parent=parent))
