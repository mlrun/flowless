import sys

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

    def run(self, context, event, *args, **kwargs):
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
            context.logger.debug(f'running {next_obj.fullname}')
            if next_obj.kind == 'choice':
                next = next_obj.choose(context, event)
            else:
                event = next_obj.run(context, event, *args, **kwargs)
                next = next_obj.next
        return event


class MLTaskChoice(MLTaskSpecBase):
    kind = 'choice'
    _shape = 'diamond'
    _dict_fields = MLTaskSpecBase._dict_fields + ['choices', 'default']

    def __init__(self, name=None, choices=None, default=None):
        super().__init__(name, next)
        self._choices = choices or []
        self.default = default

    def add_choice(self, condition, next):
        self._choices.append({'condition': condition, 'next': next})
        return self

    def choose(self, context, event):
        for choice in self.choices:
            condition = choice.get('condition', '')
            value = eval(condition, {'event': event, 'context': context})
            context.logger.debug(f'Choice event {event}, condition: {condition}, value: {value}')
            if value:
                return choice.get('next', '')
        return self.default

    @property
    def choices(self):
        resp = []
        for choice in self._choices:
            next = choice.get('next')
            if not isinstance(next, str):
                next = next.name
            resp.append({'condition': choice['condition'], 'next': next})
        return resp

    @choices.setter
    def choices(self, choices):
        self._choices = choices

    @property
    def next(self):
        return None

    @next.setter
    def next(self, next):
        pass
