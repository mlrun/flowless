import os

from mlrun import import_function
from mlrun.platforms.iguazio import split_path, mount_v3io


def deploy_pipline(pipeline):
    print('deploy:')

    for name, resource in pipeline.resources.items():

        print(name, resource.get_inputs())

        if not resource.uri or resource.skip_deploy:
            continue

        kind = resource.kind

        if kind == 'function':
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
            response = v3io_client.create_stream(
                container=container,
                path=stream_path,
                shard_count=resource.spec.get('shards', 1),
                retention_period_hours=resource.spec.get('retention_hours', 24),
                raise_for_status=v3io.dataplane.RaiseForStatus.never,
            )
            if not (response.status_code == 400 and "ResourceInUse" in str(response.body)):
                response.raise_for_status([409, 204])


def stream_uri(pipeline, uri, name):
    if uri:
        return uri
    if not pipeline.streams_path:
        raise ValueError('stream uri or pipeline.streams_path must be defined')
    return '/'.join([pipeline.streams_path.strip('/'), name])


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
