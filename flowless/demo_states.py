import time


class BaseClass:
    def __init__(self, context, state):
        self.context = context
        self.state = state


class ModelClass:
    def __init__(self, z=None):
        self.z = z

    def do(self, x):
        time.sleep(1)
        print('XX:', x)
        return x['data'] + self.z


class Test1Class(BaseClass):
    def do(self, x):
        print('ZZ:', x)
        return x * 2


class Test2Class(BaseClass):
    def xo(self, x):
        print('YY:', x)
        return x * 3


def func_test(x):
    return x * 7
