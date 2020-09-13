from mlrun import import_function
from mlrun.platforms.iguazio import split_path


def deploy_pipline(pipeline):
    print('deploy:')

    for name, resource in pipeline.resources.items():

        print(name, resource.get_inputs())

        if not resource.uri or resource.skip_deploy:
            continue

        kind = resource.kind

        if kind == 'function':
            function = import_function(url=resource.uri)

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

            print(name, resource)
            #print(function.to_yaml())

        elif kind == 'stream':
            import v3io

            v3io_client = v3io.dataplane.Client()
            container, stream_path = split_path(resource.uri)
            response = v3io_client.create_stream(
                container=container,
                path=stream_path,
                shard_count=resource.spec.get('shards', 1),
                raise_for_status=v3io.dataplane.RaiseForStatus.never,
            )
            if not (response.status_code == 400 and "ResourceInUse" in str(response.body)):
                response.raise_for_status([409, 204])
