from unittest.mock import patch

import pytest
from click.testing import CliRunner

from archery.cli import archery
from archery.docker import DockerCompose


@pytest.mark.parametrize(('command', 'args', 'kwargs'), [
    (
        ['ubuntu-cpp'],
        ['ubuntu-cpp'],
        dict(
            command=None,
            env={},
            force_pull=True,
            force_build=True,
            use_cache=True,
            use_leaf_cache=True
        )
    ),
    (
        ['ubuntu-cpp', 'bash'],
        ['ubuntu-cpp'],
        dict(
            command='bash',
            env={},
            force_pull=True,
            force_build=True,
            use_cache=True,
            use_leaf_cache=True
        )
    ),
    (
        ['ubuntu-cpp', '--no-pull', '--no-build'],
        ['ubuntu-cpp'],
        dict(
            command=None,
            env={},
            force_pull=False,
            force_build=False,
            use_cache=True,
            use_leaf_cache=True
        )
    ),
    (
        [
            'ubuntu-cpp', '--no-pull', '--force-build', '--no-cache',
            '--no-leaf-cache'
        ],
        ['ubuntu-cpp'],
        dict(
            command=None,
            env={},
            force_pull=False,
            force_build=True,
            use_cache=False,
            use_leaf_cache=False
        )
    ),
    (
        ['-e', 'ARROW_GANDIVA=OFF', '-e',  'ARROW_FLIGHT=ON', 'ubuntu-cpp'],
        ['ubuntu-cpp'],
        dict(
            command=None,
            env={
                'ARROW_GANDIVA': 'OFF',
                'ARROW_FLIGHT': 'ON'
            },
            force_pull=True,
            force_build=True,
            use_cache=True,
            use_leaf_cache=True
        )
    )
])
def test_docker_run(command, args, kwargs):
    runner = CliRunner()

    with patch.object(DockerCompose, 'run') as run:
        result = runner.invoke(archery, ['docker', 'run'] + command)
        assert result.exit_code == 0
        run.assert_called_once_with(*args, **kwargs)
