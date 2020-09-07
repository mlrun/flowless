from .base import BaseState, TaskList


class SubflowState(BaseState):
    kind = 'subflow'
    _dict_fields = BaseState._dict_fields + ['states', 'start_at']

    def __init__(self, name=None, states=None, next=None, start_at=None):
        super().__init__(name, next)
        self._states = None
        self.states = states
        self.start_at = start_at

    def get_children(self):
        return self._states.values()

    def keys(self):
        return self._states.keys()

    def values(self):
        return self._states.values()

    @property
    def states(self):
        return self._states.to_list()

    @states.setter
    def states(self, states):
        self._states = TaskList.from_list(states, self)

    def add_state(self, state, after=None):
        state = self._states.add(state)
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
        return self._states[name]

    def __iadd__(self, state):
        if isinstance(state, list):
            self.add_states(*state)
        else:
            self.add_state(state)
        return self

    def run(self, context, event, *args, **kwargs):
        if not self.start_at or self.start_at not in self.keys():
            raise ValueError(f'start_at step {self.start_at} was not specified or doesnt exist in {self.name}')
        next_obj = self[self.start_at]
        return next_obj.run(context, event, *args, **kwargs)


