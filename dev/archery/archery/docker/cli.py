# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os

import click

from ..utils.cli import validate_arrow_sources
from .core import DockerCompose, UndefinedImage


def _mock_compose_calls(compose):
    from types import MethodType
    from subprocess import CompletedProcess

    def _mock(compose, executable):
        def _execute(self, *args, **kwargs):
            params = ['{}={}'.format(k, v)
                      for k, v in self.config.params.items()]
            command = ' '.join(params + [executable] + list(args))
            click.echo(command)
            return CompletedProcess([], 0)
        return MethodType(_execute, compose)

    compose._execute_docker = _mock(compose, executable='docker')
    compose._execute_compose = _mock(compose, executable='docker-compose')


@click.group()
@click.option("--src", metavar="<arrow_src>", default=None,
              callback=validate_arrow_sources,
              help="Specify Arrow source directory.")
@click.option('--dry-run/--execute', default=False,
              help="Display the docker-compose commands instead of executing "
                   "them.")
@click.pass_context
def docker(ctx, src, dry_run):
    """
    Interact with docker-compose based builds.
    """
    ctx.ensure_object(dict)

    config_path = src.path / 'docker-compose.yml'
    if not config_path.exists():
        raise click.ClickException(
            "Docker compose configuration cannot be found in directory {}, "
            "try to pass the arrow source directory explicitly.".format(src)
        )

    # take the docker-compose parameters like PYTHON, PANDAS, UBUNTU from the
    # environment variables to keep the usage similar to docker-compose
    compose = DockerCompose(config_path, params=os.environ)
    if dry_run:
        _mock_compose_calls(compose)
    ctx.obj['compose'] = compose


@docker.command("check-config")
@click.pass_obj
def check_config(obj):
    """
    Validate docker-compose configuration.
    """
    # executes the body of the docker function above which does the validation
    # during the configuration loading


@docker.command('build')
@click.argument('image')
@click.option('--force-pull/--no-pull', default=True,
              help="Whether to force pull the image and its ancestor images")
@click.option('--using-docker-cli', default=False, is_flag=True,
              envvar='ARCHERY_USE_DOCKER_CLI',
              help="Use docker CLI directly for building instead of calling "
                   "docker-compose. This may help to reuse cached layers.")
@click.option('--using-docker-buildx', default=False, is_flag=True,
              envvar='ARCHERY_USE_DOCKER_BUILDX',
              help="Use buildx with docker CLI directly for building instead "
                   "of calling docker-compose or the plain docker build "
                   "command. This option makes the build cache reusable "
                   "across hosts.")
@click.option('--use-cache/--no-cache', default=True,
              help="Whether to use cache when building the image and its "
                   "ancestor images")
@click.option('--use-leaf-cache/--no-leaf-cache', default=True,
              help="Whether to use cache when building only the (leaf) image "
                   "passed as the argument. To disable caching for both the "
                   "image and its ancestors use --no-cache option.")
@click.pass_obj
def docker_build(obj, image, *, force_pull, using_docker_cli,
                 using_docker_buildx, use_cache, use_leaf_cache):
    """
    Execute docker-compose builds.
    """
    compose = obj['compose']

    using_docker_cli |= using_docker_buildx
    try:
        if force_pull:
            compose.pull(image, pull_leaf=use_leaf_cache,
                         using_docker=using_docker_cli)
        compose.build(image, use_cache=use_cache,
                      use_leaf_cache=use_leaf_cache,
                      using_docker=using_docker_cli,
                      using_buildx=using_docker_buildx,
                      pull_parents=force_pull)
    except UndefinedImage as e:
        raise click.ClickException(
            "There is no service/image defined in docker-compose.yml with "
            "name: {}".format(str(e))
        )
    except RuntimeError as e:
        raise click.ClickException(str(e))


@docker.command('run')
@click.argument('image')
@click.argument('command', required=False, default=None)
@click.option('--env', '-e', multiple=True,
              help="Set environment variable within the container")
@click.option('--user', '-u', default=None,
              help="Username or UID to run the container with")
@click.option('--force-pull/--no-pull', default=True,
              help="Whether to force pull the image and its ancestor images")
@click.option('--force-build/--no-build', default=True,
              help="Whether to force build the image and its ancestor images")
@click.option('--build-only', default=False, is_flag=True,
              help="Pull and/or build the image, but do not run it")
@click.option('--using-docker-cli', default=False, is_flag=True,
              envvar='ARCHERY_USE_DOCKER_CLI',
              help="Use docker CLI directly for building instead of calling "
                   "docker-compose. This may help to reuse cached layers.")
@click.option('--using-docker-buildx', default=False, is_flag=True,
              envvar='ARCHERY_USE_DOCKER_BUILDX',
              help="Use buildx with docker CLI directly for building instead "
                   "of calling docker-compose or the plain docker build "
                   "command. This option makes the build cache reusable "
                   "across hosts.")
@click.option('--use-cache/--no-cache', default=True,
              help="Whether to use cache when building the image and its "
                   "ancestor images")
@click.option('--use-leaf-cache/--no-leaf-cache', default=True,
              help="Whether to use cache when building only the (leaf) image "
                   "passed as the argument. To disable caching for both the "
                   "image and its ancestors use --no-cache option.")
@click.option('--resource-limit', default=None,
              help="A CPU/memory limit preset to mimic CI environments like "
                   "GitHub Actions. Implies --using-docker-cli. Note that "
                   "exporting ARCHERY_DOCKER_BIN=\"sudo docker\" is likely "
                   "required, unless Docker is configured with cgroups v2 "
                   "(else Docker will silently ignore the limits).")
@click.option('--volume', '-v', multiple=True,
              help="Set volume within the container")
@click.pass_obj
def docker_run(obj, image, command, *, env, user, force_pull, force_build,
               build_only, using_docker_cli, using_docker_buildx, use_cache,
               use_leaf_cache, resource_limit, volume):
    """
    Execute docker-compose builds.

    To see the available builds run `archery docker images`.

    Examples:

    # execute a single build
    archery docker run conda-python

    # execute the builds but disable the image pulling
    archery docker run --no-cache conda-python

    # pass a docker-compose parameter, like the python version
    PYTHON=3.8 archery docker run conda-python

    # disable the cache only for the leaf image
    PANDAS=master archery docker run --no-leaf-cache conda-python-pandas

    # entirely skip building the image
    archery docker run --no-pull --no-build conda-python

    # pass runtime parameters via docker environment variables
    archery docker run -e CMAKE_BUILD_TYPE=release ubuntu-cpp

    # set a volume
    archery docker run -v $PWD/build:/build ubuntu-cpp

    # starting an interactive bash session for debugging
    archery docker run ubuntu-cpp bash
    """
    compose = obj['compose']
    using_docker_cli |= using_docker_buildx

    env = dict(kv.split('=', 1) for kv in env)
    try:
        if force_pull:
            compose.pull(image, pull_leaf=use_leaf_cache,
                         using_docker=using_docker_cli)
        if force_build:
            compose.build(image, use_cache=use_cache,
                          use_leaf_cache=use_leaf_cache,
                          using_docker=using_docker_cli,
                          using_buildx=using_docker_buildx)
        if build_only:
            return
        compose.run(
            image,
            command=command,
            env=env,
            user=user,
            using_docker=using_docker_cli,
            resource_limit=resource_limit,
            volumes=volume
        )
    except UndefinedImage as e:
        raise click.ClickException(
            "There is no service/image defined in docker-compose.yml with "
            "name: {}".format(str(e))
        )
    except RuntimeError as e:
        raise click.ClickException(str(e))


@docker.command('push')
@click.argument('image')
@click.option('--user', '-u', required=False, envvar='ARCHERY_DOCKER_USER',
              help='Docker repository username')
@click.option('--password', '-p', required=False,
              envvar='ARCHERY_DOCKER_PASSWORD',
              help='Docker repository password')
@click.option('--using-docker-cli', default=False, is_flag=True,
              help="Use docker CLI directly for building instead of calling "
                   "docker-compose. This may help to reuse cached layers.")
@click.pass_obj
def docker_compose_push(obj, image, user, password, using_docker_cli):
    """Push the generated docker-compose image."""
    compose = obj['compose']
    compose.push(image, user=user, password=password,
                 using_docker=using_docker_cli)


@docker.command('images')
@click.pass_obj
def docker_compose_images(obj):
    """List the available docker-compose images."""
    compose = obj['compose']
    click.echo('Available images:')
    for image in compose.images():
        click.echo(f' - {image}')
