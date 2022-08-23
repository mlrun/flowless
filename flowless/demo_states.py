# Copyright 2020 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import time


class BaseClass:
    def __init__(self, context, state):
        self.context = context
        self.state = state


class ModelClass:
    def __init__(self, z=None):
        self.z = z or 0

    def do(self, x):
        time.sleep(1)
        print('Model:', x)
        x['result'] = [y * self.z for y in x['data']]
        return x


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


class Echo(BaseClass):
    def do(self, x):
        print('Echo:', x)
        return x


class Message(BaseClass):
    def __init__(self, msg=''):
        self.msg = msg

    def do(self, x):
        print('Messsage:')
        return self.msg


class Add:
    def __init__(self, val=None):
        self.val = val

    def do(self, x):
        print('Add:', x, self.val)
        return x + self.val


class Eval:
    def __init__(self, fn=None):
        self.fn = eval('lambda event: ' + fn)

    def do(self, x):
        print('Eval:', x)
        return self.fn(x)
