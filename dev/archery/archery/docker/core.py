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
import shlex
import subprocess
from io import StringIO

from dotenv import dotenv_values
from ruamel.yaml import YAML

from ..utils.command import Command, default_bin
from ..utils.source import arrow_path
from ..compat import _ensure_path


def flatten(node, parents=None):
    parents = list(parents or [])
    if isinstance(node, str):
        yield (node, parents)
    elif isinstance(node, list):
        for value in node:
            yield from flatten(value, parents=parents)
    elif isinstance(node, dict):
        for key, value in node.items():
            yield (key, parents)
            yield from flatten(value, parents=parents + [key])
    else:
        raise TypeError(node)


_arch_short_mapping = {
    'arm64v8': 'arm64',
}
_arch_alias_mapping = {
    'amd64': 'x86_64',
    'arm64v8': 'aarch64',
}


class UndefinedImage(Exception):
    pass


class ComposeConfig:

    def __init__(self, config_path, dotenv_path, compose_bin, params=None):
        config_path = _ensure_path(config_path)
        if dotenv_path:
            dotenv_path = _ensure_path(dotenv_path)
        else:
            dotenv_path = config_path.parent / '.env'
        self._read_env(dotenv_path, params)
        self._read_config(config_path, compose_bin)

    def _read_env(self, dotenv_path, params):
        """
        Read .env and merge it with explicitly passed parameters.
        """
        self.dotenv = dotenv_values(str(dotenv_path))
        if params is None:
            self.params = {}
        else:
            self.params = {k: v for k, v in params.items() if k in self.dotenv}

        # forward the process' environment variables
        self.env = os.environ.copy()
        # set the defaults from the dotenv files
        self.env.update(self.dotenv)
        # override the defaults passed as parameters
        self.env.update(self.params)

        # translate docker's architecture notation to a more widely used one
        arch = self.env.get('ARCH', 'amd64')
        self.env['ARCH_ALIAS'] = _arch_alias_mapping.get(arch, arch)
        self.env['ARCH_SHORT'] = _arch_short_mapping.get(arch, arch)

    def _read_config(self, config_path, compose_bin):
        """
        Validate and read the docker-compose.yml
        """
        yaml = YAML()
        with config_path.open() as fp:
            self.raw_config = yaml.load(fp)

        services = self.raw_config['services'].keys()
        self.hierarchy = dict(flatten(self.raw_config.get('x-hierarchy', {})))
        self.limit_presets = self.raw_config.get('x-limit-presets', {})
        self.with_gpus = self.raw_config.get('x-with-gpus', [])
        nodes = self.hierarchy.keys()
        errors = []

        for name in self.with_gpus:
            if name not in services:
                errors.append(
                    'Service `{}` defined in `x-with-gpus` bot not in '
                    '`services`'.format(name)
                )
        for name in nodes - services:
            errors.append(
                'Service `{}` is defined in `x-hierarchy` bot not in '
                '`services`'.format(name)
            )
        for name in services - nodes:
            errors.append(
                'Service `{}` is defined in `services` but not in '
                '`x-hierarchy`'.format(name)
            )

        # trigger docker-compose's own validation
        compose = Command('docker-compose')
        args = ['--file', str(config_path), 'config']
        result = compose.run(*args, env=self.env, check=False,
                             stderr=subprocess.PIPE, stdout=subprocess.PIPE)

        if result.returncode != 0:
            # strip the intro line of docker-compose errors
            errors += result.stderr.decode().splitlines()

        if errors:
            msg = '\n'.join([' - {}'.format(msg) for msg in errors])
            raise ValueError(
                'Found errors with docker-compose:\n{}'.format(msg)
            )

        rendered_config = StringIO(result.stdout.decode())
        self.path = config_path
        self.config = yaml.load(rendered_config)

    def get(self, service_name):
        try:
            service = self.config['services'][service_name]
        except KeyError:
            raise UndefinedImage(service_name)
        service['name'] = service_name
        service['need_gpu'] = service_name in self.with_gpus
        service['ancestors'] = self.hierarchy[service_name]
        return service

    def __getitem__(self, service_name):
        return self.get(service_name)


class Docker(Command):

    def __init__(self, docker_bin=None):
        self.bin = default_bin(docker_bin, "docker")


class DockerCompose(Command):

    def __init__(self, config_path, dotenv_path=None, compose_bin=None,
                 params=None):
        compose_bin = default_bin(compose_bin, 'docker-compose')
        self.config = ComposeConfig(config_path, dotenv_path, compose_bin,
                                    params)
        self.bin = compose_bin
        self.pull_memory = set()

    def clear_pull_memory(self):
        self.pull_memory = set()

    def _execute_compose(self, *args, **kwargs):
        # execute as a docker compose command
        try:
            result = super().run('--file', str(self.config.path), *args,
                                 env=self.config.env, **kwargs)
            result.check_returncode()
        except subprocess.CalledProcessError as e:
            def formatdict(d, template):
                return '\n'.join(
                    template.format(k, v) for k, v in sorted(d.items())
                )
            msg = (
                "`{cmd}` exited with a non-zero exit code {code}, see the "
                "process log above.\n\nThe docker-compose command was "
                "invoked with the following parameters:\n\nDefaults defined "
                "in .env:\n{dotenv}\n\nArchery was called with:\n{params}"
            )
            raise RuntimeError(
                msg.format(
                    cmd=' '.join(e.cmd),
                    code=e.returncode,
                    dotenv=formatdict(self.config.dotenv, template='  {}: {}'),
                    params=formatdict(
                        self.config.params, template='  export {}={}'
                    )
                )
            )

    def _execute_docker(self, *args, **kwargs):
        # execute as a plain docker cli command
        try:
            result = Docker().run(*args, **kwargs)
            result.check_returncode()
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                "{} exited with non-zero exit code {}".format(
                    ' '.join(e.cmd), e.returncode
                )
            )

    def pull(self, service_name, pull_leaf=True, using_docker=False,
             ignore_pull_failures=True):
        def _pull(service):
            args = ['pull']
            if service['image'] in self.pull_memory:
                return

            if using_docker:
                try:
                    self._execute_docker(*args, service['image'])
                except Exception as e:
                    if ignore_pull_failures:
                        # better --ignore-pull-failures handling
                        print(e)
                    else:
                        raise
            else:
                if ignore_pull_failures:
                    args.append('--ignore-pull-failures')
                self._execute_compose(*args, service['name'])

            self.pull_memory.add(service['image'])

        service = self.config.get(service_name)
        for ancestor in service['ancestors']:
            _pull(self.config.get(ancestor))
        if pull_leaf:
            _pull(service)

    def build(self, service_name, use_cache=True, use_leaf_cache=True,
              using_docker=False, using_buildx=False, pull_parents=True):
        def _build(service, use_cache):
            if 'build' not in service:
                # nothing to do
                return

            args = []
            cache_from = list(service.get('build', {}).get('cache_from', []))
            if pull_parents:
                for image in cache_from:
                    if image not in self.pull_memory:
                        try:
                            self._execute_docker('pull', image)
                        except Exception as e:
                            print(e)
                        finally:
                            self.pull_memory.add(image)

            if not use_cache:
                args.append('--no-cache')

            # turn on inline build cache, this is a docker buildx feature
            # used to bundle the image build cache to the pushed image manifest
            # so the build cache can be reused across hosts, documented at
            # https://github.com/docker/buildx#--cache-tonametypetypekeyvalue
            if self.config.env.get('BUILDKIT_INLINE_CACHE') == '1':
                args.extend(['--build-arg', 'BUILDKIT_INLINE_CACHE=1'])

            if using_buildx:
                for k, v in service['build'].get('args', {}).items():
                    args.extend(['--build-arg', '{}={}'.format(k, v)])

                if use_cache:
                    cache_ref = '{}-cache'.format(service['image'])
                    cache_from = 'type=registry,ref={}'.format(cache_ref)
                    cache_to = (
                        'type=registry,ref={},mode=max'.format(cache_ref)
                    )
                    args.extend([
                        '--cache-from', cache_from,
                        '--cache-to', cache_to,
                    ])

                args.extend([
                    '--output', 'type=docker',
                    '-f', arrow_path(service['build']['dockerfile']),
                    '-t', service['image'],
                    service['build'].get('context', '.')
                ])
                self._execute_docker("buildx", "build", *args)
            elif using_docker:
                # better for caching
                for k, v in service['build'].get('args', {}).items():
                    args.extend(['--build-arg', '{}={}'.format(k, v)])
                for img in cache_from:
                    args.append('--cache-from="{}"'.format(img))
                args.extend([
                    '-f', arrow_path(service['build']['dockerfile']),
                    '-t', service['image'],
                    service['build'].get('context', '.')
                ])
                self._execute_docker("build", *args)
            else:
                self._execute_compose("build", *args, service['name'])

        service = self.config.get(service_name)
        # build ancestor services
        for ancestor in service['ancestors']:
            _build(self.config.get(ancestor), use_cache=use_cache)
        # build the leaf/target service
        _build(service, use_cache=use_cache and use_leaf_cache)

    def run(self, service_name, command=None, *, env=None, volumes=None,
            user=None, using_docker=False, resource_limit=None):
        service = self.config.get(service_name)

        args = []
        if user is not None:
            args.extend(['-u', user])

        if env is not None:
            for k, v in env.items():
                args.extend(['-e', '{}={}'.format(k, v)])

        if volumes is not None:
            for volume in volumes:
                args.extend(['--volume', volume])

        if using_docker or service['need_gpu'] or resource_limit:
            # use gpus, requires docker>=19.03
            if service['need_gpu']:
                args.extend(['--gpus', 'all'])

            if service.get('shm_size'):
                args.extend(['--shm-size', service['shm_size']])

            # append env variables from the compose conf
            for k, v in service.get('environment', {}).items():
                if v is not None:
                    args.extend(['-e', '{}={}'.format(k, v)])

            # append volumes from the compose conf
            for v in service.get('volumes', []):
                if not isinstance(v, str):
                    # if not the compact string volume definition
                    v = "{}:{}".format(v['source'], v['target'])
                args.extend(['-v', v])

            # infer whether an interactive shell is desired or not
            if command in ['cmd.exe', 'bash', 'sh', 'powershell']:
                args.append('-it')

            if resource_limit:
                limits = self.config.limit_presets.get(resource_limit)
                if not limits:
                    raise ValueError(
                        f"Unknown resource limit preset '{resource_limit}'")
                cpuset = limits.get('cpuset_cpus', [])
                if cpuset:
                    args.append(f'--cpuset-cpus={",".join(map(str, cpuset))}')
                memory = limits.get('memory')
                if memory:
                    args.append(f'--memory={memory}')
                    args.append(f'--memory-swap={memory}')

            # get the actual docker image name instead of the compose service
            # name which we refer as image in general
            args.append(service['image'])

            # add command from compose if it wasn't overridden
            if command is not None:
                args.append(command)
            else:
                cmd = service.get('command', '')
                if cmd:
                    # service command might be already defined as a list
                    # on the docker-compose yaml file.
                    if isinstance(cmd, list):
                        cmd = shlex.join(cmd)
                    args.extend(shlex.split(cmd))

            # execute as a plain docker cli command
            self._execute_docker('run', '--rm', *args)
        else:
            # execute as a docker-compose command
            args.append(service_name)
            if command is not None:
                args.append(command)
            self._execute_compose('run', '--rm', *args)

    def push(self, service_name, user=None, password=None, using_docker=False):
        def _push(service):
            if using_docker:
                return self._execute_docker('push', service['image'])
            else:
                return self._execute_compose('push', service['name'])

        if user is not None:
            try:
                # TODO(kszucs): have an option for a prompt
                self._execute_docker('login', '-u', user, '-p', password)
            except subprocess.CalledProcessError:
                # hide credentials
                msg = ('Failed to push `{}`, check the passed credentials'
                       .format(service_name))
                raise RuntimeError(msg) from None

        service = self.config.get(service_name)
        for ancestor in service['ancestors']:
            _push(self.config.get(ancestor))
        _push(service)

    def images(self):
        return sorted(self.config.hierarchy.keys())

    def info(self, key_name, filters=None, prefix=' '):
        output = []
        for key, value in key_name.items():
            if hasattr(value, 'items'):
                temp_filters = filters
                if key == filters or filters is None:
                    output.append(f'{prefix} {key}')
                    # Keep showing this specific key
                    # as parent matched filter
                    temp_filters = None
                output.extend(self.info(value, temp_filters, prefix + "  "))
            else:
                if key == filters or filters is None:
                    output.append(
                        f'{prefix} {key}: ' +
                        f'{value if value is not None else "<inherited>"}'
                    )
        return output
