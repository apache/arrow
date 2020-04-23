import os
from pathlib import Path
import subprocess

from dotenv import dotenv_values
from ruamel.yaml import YAML

from .utils.command import Command, default_bin


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


class Docker(Command):

    def __init__(self, docker_bin=None):
        self.bin = default_bin(docker_bin, "docker")


class DockerCompose(Command):

    def __init__(self, config_path, dotenv_path=None, compose_bin=None):
        config_path = Path(config_path)
        if dotenv_path:
            dotenv_path = Path(dotenv_path)
        else:
            dotenv_path = config_path.parent / '.env'

        yaml = YAML()
        with config_path.open() as fp:
            self.config = yaml.load(fp)

        self.nodes = dict(flatten(self.config['x-hierarchy']))
        self.dotenv = dotenv_values(dotenv_path)
        self.bin = default_bin(compose_bin, "docker-compose")

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
        result = super().run('ps', check=False, stderr=subprocess.PIPE,
                             stdout=subprocess.PIPE)

        if result.returncode != 0:
            # strip the intro line of docker-compose errors
            errors += result.stderr.decode().splitlines()[1:]

        if errors:
            msg = '\n'.join([' - {}'.format(msg) for msg in errors])
            raise ValueError(
                'Found errors with docker-compose:\n{}'.format(msg)
            )

    def _command_env(self):
        env = os.environ.copy()
        env.update(self.dotenv)
        return env

    def build(self, image, cache=True, cache_leaf=True):
        env = self._command_env()
        run = super().run

        # build all parents
        for parent in self.nodes[image]:
            if cache:
                run('pull', '--ignore-pull-failures', parent, env=env)
            run('build', parent, env=env)

        # build the image at last
        if cache and cache_leaf:
            run('pull', '--ignore-pull-failures', image, env=env)
            run('build', image, env=env)
        else:
            run('build', '--no-cache', image, env=env)

    def run(self, image, env=None):
        args = []
        if env is not None:
            for k, v in env.items():
                args.extend(['-e', '{}={}'.format(k, v)])
        super().run('run', '--rm', *args, image, env=self._command_env())

    def push(self, image, user, password):
        try:
            Docker().run('login', '-u', user, '-p', password)
        except subprocess.CalledProcessError:
            # hide credentials
            msg = ('Failed to push `{}`, check the passed credentials'
                   .format(image))
            raise RuntimeError(msg) from None
        else:
            super().run('push', image, env=self._command_env())
