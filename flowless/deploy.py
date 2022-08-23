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

from mlrun import import_function
from mlrun.platforms.iguazio import split_path, mount_v3io

from .common import stream_uri


def deploy_pipline(pipeline):
    print('deploy:')

    for name, resource in pipeline.resources.items():

        print(name, resource.get_inputs())

        if resource.skip_deploy:
            continue

        kind = resource.kind

        if kind == 'function':
            if not resource.uri:
                continue
            function = import_function(url=resource.uri)
            function.metadata.project = pipeline.project
            function.metadata.name = name

            if resource.spec:
                for attribute in [
                    "volumes",
                    "volume_mounts",
                    "env",
                    "resources",
                    "image_pull_policy",
                    "replicas",
                ]:
                    value = resource.spec.get(attribute, None)
                    if value and hasattr(function.spec, attribute):
                        setattr(function.spec, attribute, value)

            function.set_env('V3IO_API', os.environ.get("V3IO_API", 'v3io-webapi:8081'))
            function.set_env('V3IO_ACCESS_KEY', os.environ.get("V3IO_ACCESS_KEY", ''))
            function.set_env('RESOURCE_NAME', name)
            function.set_env('PIPELINE_SPEC_ENV', pipeline.to_json())
            if resource.spec and resource.spec.get('with_v3io', ''):
                function.apply(mount_v3io())

            for source, source_state, target_state in resource.get_inputs():
                source_resource = pipeline.resources[source]
                if source_resource.kind == 'stream':
                    uri = stream_uri(pipeline, source_resource.uri, source)
                    function.set_env('FROM_STATE', target_state)
                    add_stream_trigger(function, uri)

            print(name, resource)
            print(function.to_yaml())

        elif kind == 'stream':
            import v3io

            v3io_client = v3io.dataplane.Client()
            container, stream_path = split_path(stream_uri(pipeline, resource.uri, name))
            print(f'stream path: {container}{stream_path}')
            response = v3io_client.create_stream(
                container=container,
                path=stream_path,
                shard_count=resource.spec.get('shards', 1),
                retention_period_hours=resource.spec.get('retention_hours', 24),
                raise_for_status=v3io.dataplane.RaiseForStatus.never,
            )
            print(response.status_code, response.body)
            if not (response.status_code == 400 and "ResourceInUse" in str(response.body)):
                response.raise_for_status([409, 204])


def add_stream_trigger(fn, path, web_api=None, consumer_group='', max_workers=4):
    if not web_api:
        web_api = 'http://' + os.environ.get("V3IO_API", 'v3io-webapi:8081')
    url = f'{web_api}/{path}'
    if consumer_group:
        web_api += '@' + consumer_group

    trigger_spec = {
        'kind': 'v3ioStream',
        'url': url,
        "password": os.getenv('V3IO_ACCESS_KEY'),
        "maxWorkers": max_workers,
        'attributes': {"pollingIntervalMs": 500,
                       "seekTo": "latest",
                       "readBatchSize": 100,
                       }
    }
    fn.add_trigger('input-stream', trigger_spec)
