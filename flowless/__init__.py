import json

from .states import (SubflowState, ChoiceState, RouterState,
                             TaskState, FlowRoot, QueueState)
from .loader import load_pipeline

__version__ = "0.0.1"

task_kinds = {'task': TaskState,
              'router': RouterState,
              'subflow': SubflowState,
              'choice': ChoiceState,
              'queue': QueueState}

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


def _next_fullname(state):
    if state.parent_name and state.next:
        return '.'.join([state.parent_name, state.next])
    return state.next


def build_graph(step, nodes=[], edges=[], parent=None):
    if hasattr(step, 'states'):
        if parent:
            nodes.append(_get_node_obj(name=step.fullname, text=step.name,
                                       shape=step._shape, parent=parent))
        if step.next:
            edges.append(_new_edge(step.fullname, _next_fullname(step)))

        for state in step.values():
            parent_name = None if step.kind == 'root' else step.fullname
            build_graph(state, nodes, edges, parent_name)

    elif hasattr(step, 'routes'):
        switch_name = step.fullname + '$'
        edges += _new_edge(switch_name, _next_fullname(step))
        nodes.append(_get_node_obj(name=switch_name, text=step.name, parent=parent))
        nodes.append(_get_node_obj(name=step.fullname, text=step.class_name, shape='star', parent=switch_name))
        for route in step.values():
            build_graph(route, nodes, edges, switch_name)
            edges += _new_edge(step.fullname, route.fullname)

    elif step.kind in ['choice', 'queue']:
        nodes.append(_get_node_obj(name=step.fullname, text=step.name,
                                   shape=step._shape, parent=parent))
        for next in step.next_branches():
            if next:
                if parent:
                    next = '.'.join([parent, next])
                edges += _new_edge(step.fullname, next)

    else:
        edges += _new_edge(step.fullname, _next_fullname(step))
        nodes.append(_get_node_obj(name=step.fullname, text=step.name,
                                   shape=step._shape, parent=parent))


def save_graph(root, target):
    nodes = []
    edges = []
    build_graph(root, nodes, edges)
    with open(target, "w", encoding="utf-8") as fp:
        fp.write(json.dumps({'nodes': nodes, 'edges': edges}, cls=None, indent=2))
