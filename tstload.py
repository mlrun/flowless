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
import os
import flowless
from flowless.common import TaskRunContext, Event
from flowless.demo_states import *
from flowless.loader import init_pipeline_context, pipeline_handler
from flowless.states.router import ParallelRouter

os.environ['FROM_STATE'] = 'router.m1'
os.environ['RESOURCE_NAME'] = 'f1'

ctx = TaskRunContext()
init_pipeline_context(ctx, globals(), 'p.json')
resp = pipeline_handler(ctx, Event(body={'data': [99]}))
print(resp)


#p = flowless.load_pipeline('p.json')
#print(p.to_yaml())

#p.prepare('f1')
#deploy_pipline(p)

#print(p.init('f1', namespace=globals()))
#save_graph(p, "js/data.json")
#print('AAAAA', p.run())

