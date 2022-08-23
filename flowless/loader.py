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
import json
import os

import mlrun

from flowless.states import FlowRoot


def load_pipeline(specpath=''):
    specpath = specpath or os.environ.get('PIPELINE_SPEC_PATH', '')
    if specpath:
        data = mlrun.get_object(specpath)
    else:
        data = os.environ.get('PIPELINE_SPEC_ENV', '')
        if not data:
            raise ValueError('failed to find spec file or env var')
    spec = json.loads(data)
    pipeline = FlowRoot.from_dict(spec)
    return pipeline


def init_pipeline_context(context, namespace=None, specpath=None):
    pipeline = load_pipeline(specpath=specpath)
    pipeline.in_nuclio = True
    resource = os.environ.get('RESOURCE_NAME', '')
    resource = resource or os.environ.get('NUCLIO_FUNCTION_NAME', '')
    pipeline.prepare(resource)
    pipeline.init(resource, namespace=namespace)
    setattr(context, 'pipeline', pipeline)


def pipeline_handler(context, event):
    from_state = None
    if event.headers:
        from_state = event.headers.get('from-state', None)
        event.id = event.id or event.headers.get('event-id', '')

    return context.pipeline.run(event, context, from_state=from_state)



