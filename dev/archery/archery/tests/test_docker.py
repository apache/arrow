from unittest import mock
from pathlib import Path

import pytest

from archery.docker import DockerCompose


example_missing_service = """
x-hierarchy:
  - foo:
    - sub-foo:
      - sub-sub-foo
      - another-sub-sub-foo
  - bar:
    - sub-bar
  - baz

services:
  foo:
  sub-sub-foo:
  another-sub-sub-foo:
  bar:
  sub-bar:
  baz:
"""

example_missing_node = """
x-hierarchy:
  - foo:
    - sub-foo:
      - sub-sub-foo
      - another-sub-sub-foo
  - bar
  - baz

services:
  foo:
  sub-foo:
  sub-sub-foo:
  another-sub-sub-foo:
  bar:
  sub-bar:
  baz:
"""

example_ok = """
x-hierarchy:
  - foo:
    - sub-foo:
      - sub-sub-foo
      - another-sub-sub-foo
  - bar:
    - sub-bar
  - baz

services:
  foo:
  sub-foo:
  sub-sub-foo:
  another-sub-sub-foo:
  bar:
  sub-bar:
  baz:
"""


def create_config(tmpdir, yml_content):
    config_path = tmpdir / 'docker-compose.yml'
    with config_path.open('w') as fp:
        fp.write(yml_content)
    return DockerCompose(config_path)


def test_config_validation(tmpdir):
    compose = create_config(tmpdir, example_missing_service)
    msg = "`sub-foo` is defined in `x-hierarchy` bot not in `services`"
    with pytest.raises(ValueError, match=msg):
        compose.validate()

    compose = create_config(tmpdir, example_missing_node)
    msg = "`sub-bar` is defined in `services` but not in `x-hierarchy`"
    with pytest.raises(ValueError, match=msg):
        compose.validate()

    compose = create_config(tmpdir, example_ok)
    compose.validate()


def test_executed_docker_commands(tmpdir):
    arrow_config = Path(__file__).parents[4] / 'docker-compose.yml'
    compose = DockerCompose(arrow_config)

    with mock.patch('subprocess.run', autospec=True) as run:
        compose.run('conda-python-pandas')
        cmd = ['docker-compose', 'run', '--rm', 'conda-python-pandas']
        run.assert_called_with(cmd, check=True, env=mock.ANY)

    with mock.patch('subprocess.run', autospec=True) as run:
        compose.build('conda-python-pandas', cache=False)
        commands = [
            ['docker-compose', 'build', '--no-cache', 'conda-cpp'],
            ['docker-compose', 'build', '--no-cache', 'conda-python'],
            ['docker-compose', 'build', '--no-cache', 'conda-python-pandas']
        ]
        run.assert_has_calls(
            [mock.call(cmd, check=True, env=mock.ANY) for cmd in commands]
        )

    with mock.patch('subprocess.run', autospec=True) as run:
        compose.build('conda-python-pandas', cache=True, cache_leaf=False)
        commands = [
            ['docker-compose', 'pull', '--ignore-pull-failures', 'conda-cpp'],
            ['docker-compose', 'build', 'conda-cpp'],
            ['docker-compose', 'pull', '--ignore-pull-failures',
             'conda-python'],
            ['docker-compose', 'build', 'conda-python'],
            ['docker-compose', 'build', '--no-cache', 'conda-python-pandas']
        ]
        run.assert_has_calls(
            [mock.call(cmd, check=True, env=mock.ANY) for cmd in commands]
        )

    with mock.patch('subprocess.run', autospec=True) as run:
        compose.build('conda-python-pandas')
        commands = [
            ['docker-compose', 'pull', '--ignore-pull-failures', 'conda-cpp'],
            ['docker-compose', 'build', 'conda-cpp'],
            ['docker-compose', 'pull', '--ignore-pull-failures',
             'conda-python'],
            ['docker-compose', 'build', 'conda-python'],
            ['docker-compose', 'pull', '--ignore-pull-failures',
             'conda-python-pandas'],
            ['docker-compose', 'build', 'conda-python-pandas']
        ]
        run.assert_has_calls(
            [mock.call(cmd, check=True, env=mock.ANY) for cmd in commands]
        )
