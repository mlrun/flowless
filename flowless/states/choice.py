from .base import BaseState


class ChoiceState(BaseState):
    kind = 'choice'
    _shape = 'diamond'
    _dict_fields = BaseState._dict_fields + ['choices', 'default']

    def __init__(self, name=None, choices=None, default=None):
        super().__init__(name, next)
        self._choices = choices or []
        self.default = default

    def add_choice(self, condition, next):
        self._choices.append({'condition': condition, 'next': next})
        return self

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

    def next_branches(self):
        resp = []
        if self.default:
            resp.append(self.default)
        for choice in self._choices:
            next = choice.get('next')
            if not isinstance(next, str):
                next = next.name
            resp.append(next)
        return resp

    def _choose(self, context, event):
        for choice in self.choices:
            condition = choice.get('condition', '')
            value = eval(condition, {'event': event, 'context': context})
            context.logger.debug(f'Choice event {event.body}, condition: {condition}, value: {value}')
            if value:
                return choice.get('next', '')
        return self.default

    def run(self, context, event, *args, **kwargs):
        next = self._choose(context, event)
        next_obj = self._parent[next]
        return next_obj.run(context, event, *args, **kwargs)
