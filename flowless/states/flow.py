from .base import BaseState, StateList


class SubflowState(BaseState):
    kind = 'subflow'
    _dict_fields = BaseState._dict_fields + ['states', 'start_at']

    def __init__(self, name=None, states=None, next=None, start_at=None):
        super().__init__(name, next)
        self._children = None
        self.states = states
        self.start_at = start_at
        self.from_state = None

    def get_children(self):
        return self._children.values()

    def keys(self):
        return self._children.keys()

    def values(self):
        return self._children.values()

    @property
    def states(self):
        return self._children.to_list()

    @states.setter
    def states(self, states):
        self._children = StateList.from_list(states, self)

    def add_state(self, state, after=None):
        state = self._children.add(state)
        if after and after.next is None:
            state.after(after)
        state.set_parent(self)
        return state

    def add_states(self, *states, chain=True):
        after = None
        for r in states:
            self.add_state(r, after)
            if chain:
                after = r
        return self

    def __getitem__(self, name):
        return self._children[name]

    def __setitem__(self, name, state):
        state.set_parent(self)
        self._children[name] = state

    def __iadd__(self, state):
        if isinstance(state, list):
            self.add_states(*state)
        else:
            self.add_state(state)
        return self

    def run(self, context, event, *args, **kwargs):
        from_state = kwargs.get('from_state', None) or self.from_state or self.start_at
        if not from_state:
            raise ValueError(f'start step {from_state} was not specified in {self.name}')
        tree = from_state.split('.')
        next_obj = self
        for state in tree:
            if state not in next_obj.keys():
                raise ValueError(f'start step {from_state} doesnt exist in {self.name}')
            next_obj = next_obj[state]
        return next_obj.run(context, event, *args, **kwargs)


