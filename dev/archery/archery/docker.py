#!/usr/bin/env python3
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
import subprocess

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
        self.config_path = _ensure_path(config_path)
        if dotenv_path:
            self.dotenv_path = _ensure_path(dotenv_path)
        else:
            self.dotenv_path = self.config_path.parent / '.env'

        yaml = YAML()
        with self.config_path.open() as fp:
            self.config = yaml.load(fp)

        self.bin = default_bin(compose_bin, 'docker-compose')
        self.nodes = dict(flatten(self.config['x-hierarchy']))
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

    def validate(self):
        services = self.config['services'].keys()
        nodes = self.nodes.keys()
        errors = []

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
        result = self._execute('config', check=False, stderr=subprocess.PIPE,
                               stdout=subprocess.PIPE)

        if result.returncode != 0:
            # strip the intro line of docker-compose errors
            errors += result.stderr.decode().splitlines()[1:]

        if errors:
            msg = '\n'.join([' - {}'.format(msg) for msg in errors])
            raise ValueError(
                'Found errors with docker-compose:\n{}'.format(msg)
            )

    def _validate_image(self, name):
        if name not in self.nodes:
            raise UndefinedImage(name)

    def _execute(self, *args, **kwargs):
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

    def build(self, image, cache=True, cache_leaf=True, params=None):
        self._validate_image(image)

        if cache:
            # pull
            for ancestor in self.nodes[image]:
                self._execute('pull', '--ignore-pull-failures', ancestor)
            if cache_leaf:
                self._execute('pull', '--ignore-pull-failures', image)
            # build
            for ancestor in self.nodes[image]:
                self._execute('build', ancestor)
            if cache_leaf:
                self._execute('build', image)
            else:
                self._execute('build', '--no-cache', image)
        else:
            # build
            for ancestor in self.nodes[image]:
                self._execute('build', '--no-cache', ancestor)
            self._execute('build', '--no-cache', image)

    def run(self, image, command=None, env=None, params=None):
        self._validate_image(image)

        args = []
        if env is not None:
            for k, v in env.items():
                args.extend(['-e', '{}={}'.format(k, v)])

        args.append(image)
        if command is not None:
            args.append(command)

        self._execute('run', '--rm', *args)

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
            self._execute('push', image)

    def images(self):
        return sorted(self.nodes.keys())
