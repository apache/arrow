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

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from archery.cli import archery
from archery.docker import DockerCompose


@pytest.mark.parametrize(('command', 'args', 'kwargs'), [
    (
        ['ubuntu-cpp', '--build-only'],
        ['ubuntu-cpp'],
        dict(
            command=None,
            env={},
            user=None,
            force_pull=True,
            force_build=True,
            build_only=True,
            use_cache=True,
            use_leaf_cache=True,
            volumes=()
        )
    ),
    (
        ['ubuntu-cpp', 'bash'],
        ['ubuntu-cpp'],
        dict(
            command='bash',
            env={},
            user=None,
            force_pull=True,
            force_build=True,
            build_only=False,
            use_cache=True,
            use_leaf_cache=True,
            volumes=()
        )
    ),
    (
        ['ubuntu-cpp', '--no-pull', '--no-build'],
        ['ubuntu-cpp'],
        dict(
            command=None,
            env={},
            user=None,
            force_pull=False,
            force_build=False,
            build_only=False,
            use_cache=True,
            use_leaf_cache=True,
            volumes=()
        )
    ),
    (
        [
            'ubuntu-cpp', '--no-pull', '--force-build', '--user', 'me',
            '--no-cache', '--no-leaf-cache'
        ],
        ['ubuntu-cpp'],
        dict(
            command=None,
            env={},
            user='me',
            force_pull=False,
            force_build=True,
            build_only=False,
            use_cache=False,
            use_leaf_cache=False,
            volumes=()
        )
    ),
    (
        [
            '-e', 'ARROW_GANDIVA=OFF', '-e', 'ARROW_FLIGHT=ON', '-u', 'root',
            'ubuntu-cpp'
        ],
        ['ubuntu-cpp'],
        dict(
            command=None,
            env={
                'ARROW_GANDIVA': 'OFF',
                'ARROW_FLIGHT': 'ON'
            },
            user='root',
            force_pull=True,
            force_build=True,
            build_only=False,
            use_cache=True,
            use_leaf_cache=True,
            volumes=()
        )
    ),
    (
        [
            '--volume', './build:/build', '-v', './ccache:/ccache:delegated',
            'ubuntu-cpp'
        ],
        ['ubuntu-cpp'],
        dict(
            command=None,
            env={},
            user=None,
            force_pull=True,
            force_build=True,
            build_only=False,
            use_cache=True,
            use_leaf_cache=True,
            volumes=(
                './build:/build',
                './ccache:/ccache:delegated',
            )
        )
    )
])
def test_docker_run(command, args, kwargs):
    runner = CliRunner()

    with patch.object(DockerCompose, 'run') as run:
        result = runner.invoke(archery, ['docker', 'run'] + command)
        assert result.exit_code == 0
        run.assert_called_once_with(*args, **kwargs)
