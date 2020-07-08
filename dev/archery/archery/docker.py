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
import re
import shlex
import subprocess
from io import StringIO

from dotenv import dotenv_values
from ruamel.yaml import YAML

from .utils.command import Command, default_bin
from .compat import _ensure_path


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


class UndefinedImage(Exception):
    pass


class Docker(Command):

    def __init__(self, docker_bin=None):
        self.bin = default_bin(docker_bin, "docker")


class DockerCompose(Command):

    def __init__(self, config_path, dotenv_path=None, compose_bin=None,
                 params=None):
        self.bin = default_bin(compose_bin, 'docker-compose')

        self.config_path = _ensure_path(config_path)
        if dotenv_path:
            self.dotenv_path = _ensure_path(dotenv_path)
        else:
            self.dotenv_path = self.config_path.parent / '.env'

        self._read_env(params)
        self._read_config()

    def _read_config(self):
        """
        Validate and read the docker-compose.yml
        """
        yaml = YAML()
        with self.config_path.open() as fp:
            config = yaml.load(fp)

        services = config['services'].keys()
        self.nodes = dict(flatten(config.get('x-hierarchy', {})))
        self.with_gpus = config.get('x-with-gpus', [])
        nodes = self.nodes.keys()
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
        result = self._execute_compose('config', check=False,
                                       stderr=subprocess.PIPE,
                                       stdout=subprocess.PIPE)

        if result.returncode != 0:
            # strip the intro line of docker-compose errors
            errors += result.stderr.decode().splitlines()[1:]

        if errors:
            msg = '\n'.join([' - {}'.format(msg) for msg in errors])
            raise ValueError(
                'Found errors with docker-compose:\n{}'.format(msg)
            )

        rendered_config = StringIO(result.stdout.decode())
        self.config = yaml.load(rendered_config)

    def _read_env(self, params):
        """
        Read .env and merge it with explicitly passed parameters.
        """
        self.dotenv = dotenv_values(str(self.dotenv_path))
        if params is None:
            self.params = {}
        else:
            self.params = {k: v for k, v in params.items() if k in self.dotenv}

        # forward the process' environment variables
        self._compose_env = os.environ.copy()
        # set the defaults from the dotenv files
        self._compose_env.update(self.dotenv)
        # override the defaults passed as parameters
        self._compose_env.update(self.params)

    def _validate_image(self, name):
        if name not in self.nodes:
            raise UndefinedImage(name)

    def _execute_compose(self, *args, **kwargs):
        # execute as a docker compose command
        try:
            return super().run('--file', str(self.config_path), *args,
                               env=self._compose_env, **kwargs)
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
                    dotenv=formatdict(self.dotenv, template='  {}: {}'),
                    params=formatdict(self.params, template='  export {}={}')
                )
            )

    def _execute_docker(self, *args, **kwargs):
        # execute as a plain docker cli command
        try:
            return Docker().run(*args, **kwargs)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                "{} exited with non-zero exit code {}".format(
                    ' '.join(e.cmd), e.returncode
                )
            )

    def pull(self, image, pull_leaf=True):
        self._validate_image(image)

        for ancestor in self.nodes[image]:
            self._execute_compose('pull', '--ignore-pull-failures', ancestor)

        if pull_leaf:
            self._execute_compose('pull', '--ignore-pull-failures', image)

    def build(self, image, use_cache=True, use_leaf_cache=True):
        self._validate_image(image)

        for ancestor in self.nodes[image]:
            if use_cache:
                self._execute_compose('build', ancestor)
            else:
                self._execute_compose('build', '--no-cache', ancestor)

        if use_cache and use_leaf_cache:
            self._execute_compose('build', image)
        else:
            self._execute_compose('build', '--no-cache', image)

    def run(self, image, command=None, *, env=None, force_pull=False,
            force_build=False, use_cache=True, use_leaf_cache=True,
            volumes=None, build_only=False, user=None):
        self._validate_image(image)

        if force_pull:
            self.pull(image, pull_leaf=use_leaf_cache)
        if force_build:
            self.build(image, use_cache=use_cache,
                       use_leaf_cache=use_leaf_cache)
        if build_only:
            return

        args = []
        if user is not None:
            args.extend(['-u', user])

        if env is not None:
            for k, v in env.items():
                args.extend(['-e', '{}={}'.format(k, v)])

        if volumes is not None:
            for volume in volumes:
                args.extend(['--volume', volume])

        if image in self.with_gpus:
            # rendered compose configuration for the image
            cc = self.config['services'][image]

            # use gpus, requires docker>=19.03
            args.extend(['--gpus', 'all'])

            # append env variables from the compose conf
            for k, v in cc.get('environment', {}).items():
                args.extend(['-e', '{}={}'.format(k, v)])

            # append volumes from the compose conf
            for v in cc.get('volumes', []):
                args.extend(['-v', v])

            # get the actual docker image name instead of the compose service
            # name which we refer as image in general
            args.append(cc['image'])

            # add command from compose if it wasn't overridden
            if command is not None:
                args.append(command)
            else:
                # replace whitespaces from the preformatted compose command
                cmd = shlex.split(cc.get('command', ''))
                cmd = [re.sub(r"\s+", " ", token) for token in cmd]
                if cmd:
                    args.extend(cmd)

            # execute as a plain docker cli command
            return self._execute_docker('run', '--rm', '-it', *args)

        # execute as a docker-compose command
        args.append(image)
        if command is not None:
            args.append(command)
        self._execute_compose('run', '--rm', *args)

    def push(self, image, user, password):
        self._validate_image(image)
        try:
            # TODO(kszucs): have an option for a prompt
            Docker().run('login', '-u', user, '-p', password)
        except subprocess.CalledProcessError:
            # hide credentials
            msg = ('Failed to push `{}`, check the passed credentials'
                   .format(image))
            raise RuntimeError(msg) from None
        else:
            for ancestor in self.nodes[image]:
                self._execute_compose('push', ancestor)
            self._execute_compose('push', image)

    def images(self):
        return sorted(self.nodes.keys())
