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
import sys

import click

from ..utils.cli import validate_arrow_sources
from ..utils.logger import group
from .core import DockerCompose, UndefinedImage


def _mock_compose_calls(compose):
    from types import MethodType
    from subprocess import CompletedProcess

    def _mock(compose, command_tuple):
        def _execute(self, *args, **kwargs):
            params = [f'{k}={v}'
                      for k, v in self.config.params.items()]
            command = ' '.join(params + command_tuple + args)
            click.echo(command)
            return CompletedProcess([], 0)
        return MethodType(_execute, compose)

    compose._execute_docker = _mock(compose, command_tuple=('docker',))
    compose._execute_compose = _mock(compose, command_tuple=('docker', 'compose'))


@click.group()
@click.option("--src", metavar="<arrow_src>", default=None,
              callback=validate_arrow_sources,
              help="Specify Arrow source directory.")
@click.option('--dry-run/--execute', default=False,
              help="Display the docker commands instead of executing them.")
@click.option('--using-legacy-docker-compose', default=False, is_flag=True,
              envvar='ARCHERY_USE_LEGACY_DOCKER_COMPOSE',
              help="Use legacy docker-compose utility instead of the built-in "
                   "`docker compose` subcommand. This may be necessary if the "
                   "Docker client is too old for some options.")
@click.option('--using-docker-cli', default=False, is_flag=True,
              envvar='ARCHERY_USE_DOCKER_CLI',
              help="Use docker CLI directly for building instead of calling "
                   "`docker compose`. This may help to reuse cached layers.")
@click.option('--using-docker-buildx', default=False, is_flag=True,
              envvar='ARCHERY_USE_DOCKER_BUILDX',
              help="Use buildx with docker CLI directly for building instead "
                   "of calling `docker compose` or the plain docker build "
                   "command. This option makes the build cache reusable "
                   "across hosts.")
@click.pass_context
def docker(ctx, src, dry_run, using_legacy_docker_compose, using_docker_cli,
           using_docker_buildx):
    """
    Interact with Docker Compose based builds.
    """
    ctx.ensure_object(dict)

    config_path = src.path / 'docker-compose.yml'
    if not config_path.exists():
        raise click.ClickException(
            "Docker compose configuration cannot be found in directory {}, "
            "try to pass the arrow source directory explicitly.".format(src)
        )

    # take the Docker Compose parameters like PYTHON, PANDAS, UBUNTU from the
    # environment variables to keep the usage similar to docker compose
    using_docker_cli |= using_docker_buildx
    compose_bin = ("docker-compose" if using_legacy_docker_compose
                   else "docker compose")
    with group("Docker: Prepare"):
        compose = DockerCompose(config_path, params=os.environ,
                                using_docker=using_docker_cli,
                                using_buildx=using_docker_buildx,
                                debug=ctx.obj.get('debug', False),
                                compose_bin=compose_bin)
    if dry_run:
        _mock_compose_calls(compose)
    ctx.obj['compose'] = compose


@docker.command("check-config")
@click.pass_obj
def check_config(obj):
    """
    Validate Docker Compose configuration.
    """
    # executes the body of the docker function above which does the validation
    # during the configuration loading


@docker.command('pull')
@click.argument('image')
@click.option('--pull-leaf/--no-leaf', default=True,
              help="Whether to pull leaf images too.")
@click.option('--ignore-pull-failures/--no-ignore-pull-failures', default=True,
              help="Whether to ignore pull failures.")
@click.pass_obj
def docker_pull(obj, image, *, pull_leaf, ignore_pull_failures):
    """
    Execute docker compose pull.
    """
    compose = obj['compose']

    try:
        compose.pull(image, pull_leaf=pull_leaf,
                     ignore_pull_failures=ignore_pull_failures)
    except UndefinedImage as e:
        raise click.ClickException(
            "There is no service/image defined in docker-compose.yml with "
            "name: {}".format(str(e))
        )
    except RuntimeError as e:
        raise click.ClickException(str(e))


@docker.command('build')
@click.argument('image')
@click.option('--force-pull/--no-pull', default=True,
              help="Whether to force pull the image and its ancestor images")
@click.option('--use-cache/--no-cache', default=True,
              help="Whether to use cache when building the image and its "
                   "ancestor images")
@click.option('--use-leaf-cache/--no-leaf-cache', default=True,
              help="Whether to use cache when building only the (leaf) image "
                   "passed as the argument. To disable caching for both the "
                   "image and its ancestors use --no-cache option.")
@click.pass_obj
def docker_build(obj, image, *, force_pull, use_cache, use_leaf_cache):
    """
    Execute Docker Compose builds.
    """
    compose = obj['compose']

    try:
        if force_pull:
            compose.pull(image, pull_leaf=use_leaf_cache)
        compose.build(image, use_cache=use_cache,
                      use_leaf_cache=use_leaf_cache,
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
@click.option('--use-cache/--no-cache', default=True,
              help="Whether to use cache when building the image and its "
                   "ancestor images")
@click.option('--use-leaf-cache/--no-leaf-cache', default=True,
              help="Whether to use cache when building only the (leaf) image "
                   "passed as the argument. To disable caching for both the "
                   "image and its ancestors use --no-cache option.")
@click.option('--resource-limit', default=None,
              help="A CPU/memory limit preset to mimic CI environments like "
                   "GitHub Actions. Mandates --using-docker-cli. Note that "
                   "exporting ARCHERY_DOCKER_BIN=\"sudo docker\" is likely "
                   "required, unless Docker is configured with cgroups v2 "
                   "(else Docker will silently ignore the limits).")
@click.option('--volume', '-v', multiple=True,
              help="Set volume within the container")
@click.pass_obj
def docker_run(obj, image, command, *, env, user, force_pull, force_build,
               build_only, use_cache, use_leaf_cache, resource_limit,
               volume):
    """
    Execute Docker Compose builds.

    To see the available builds run `archery docker images`.

    Examples:

    # execute a single build
    archery docker run conda-python

    # execute the builds but disable the image pulling
    archery docker run --no-cache conda-python

    # pass a Docker Compose parameter, like the python version
    PYTHON=3.12 archery docker run conda-python

    # disable the cache only for the leaf image
    PANDAS=upstream_devel archery docker run --no-leaf-cache \
        conda-python-pandas

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

    env = dict(kv.split('=', 1) for kv in env)
    try:
        if force_pull:
            with group("Docker: Pull"):
                compose.pull(image, pull_leaf=use_leaf_cache)
        if force_build:
            with group("Docker: Build"):
                compose.build(image, use_cache=use_cache,
                              use_leaf_cache=use_leaf_cache)
        if build_only:
            return
        compose.run(
            image,
            command=command,
            env=env,
            user=user,
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
@click.pass_obj
def docker_compose_push(obj, image, user, password):
    """Push the generated Docker Compose image."""
    compose = obj['compose']
    compose.push(image, user=user, password=password)


@docker.command('images')
@click.pass_obj
def docker_compose_images(obj):
    """List the available Docker Compose images."""
    compose = obj['compose']
    click.echo('Available images:')
    for image in compose.images():
        click.echo(f' - {image}')


@docker.command('info')
@click.argument('service_name')
@click.option('--show', '-s', required=False,
              help="Show only specific docker compose key. Examples of keys:"
                   " command, environment, build, dockerfile")
@click.pass_obj
def docker_compose_info(obj, service_name, show):
    """Show Docker Compose definition info for service_name.

    SERVICE_NAME is the name of the docker service defined in
    docker-compose.yml. Look at `archery docker images` output for names.
    """
    compose = obj['compose']
    try:
        service = compose.config.raw_config["services"][service_name]
    except KeyError:
        click.echo(f'Service name {service_name} could not be found', err=True)
        sys.exit(1)
    else:
        click.echo(f'Service {service_name} Docker Compose config:')
        output = "\n".join(compose.info(service, show))
        click.echo(output)
