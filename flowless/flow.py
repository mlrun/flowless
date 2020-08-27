from flowless.base import MLTaskSpecBase, resource_params, TaskList


class MLTaskFlow(MLTaskSpecBase):
    kind = 'subflow'
    _dict_fields = MLTaskSpecBase._dict_fields + ['states', 'start_at']

    def __init__(self, name=None, states=None, next=None, start_at=None):
        super().__init__(name, next)
        self._states = None
        self.states = states
        self.start_at = start_at

    def get_children(self):
        return self._states.values()

    def first_step(self):
        return '.'.join([self.fullname, self.start_at])

    @property
    def states(self):
        return self._states.to_list()

    @states.setter
    def states(self, states):
        self._states = TaskList.from_list(states, self)

    def add_state(self, state, after=None):
        state = self._states.add(state)
        if after:
            state.after(after)
        state._parent = self
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

    @property
    def state_objects(self):
        return self._states

    def run(self, event, *args, **kwargs):
        if not self.start_at:
            raise ValueError(f'flow {self.fullname} missing start_at')

        next = self.start_at
        while next:
            idx = next.rfind('.')
            if idx >= 0:
                next = next[idx + 1:]
            if next not in self._states.keys():
                raise ValueError(f'flow {self.fullname} next state {next} doesnt exist in {self._states.keys()}')
            next_obj = self._states[next]
            print(f'running {next_obj.fullname}')
            event = next_obj.run(event, *args, **kwargs)
            next = next_obj.next
        return event


class MLTaskRoot(MLTaskFlow):
    kind = 'root'
    _dict_fields = MLTaskFlow._dict_fields[1:] + ['triggers']

    def __init__(self, name=None, states=None, start_at=None):
        super().__init__(name, states, start_at=start_at)
        self.triggers = None
        self.resources = None
