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

import logging
import os
from subprocess import CompletedProcess, PIPE
from unittest.mock import patch

import pytest

from click.testing import CliRunner

from archery.cli import archery
from archery.docker import DockerCompose
from archery.docker.cli import docker
from archery.utils.logger import ctx as log_ctx, logger


@pytest.mark.parametrize(('options', 'debug_env', 'debug'), [
    ([], None, True),
    ([], '1', True),
    ([], '0', False),
    ([], 'false', False),
    ([], 'OFF', False),
    (['--quiet'], None, False),
    (['--debug'], '0', True),
    (['--debug', '--quiet'], None, True),
    (['--quiet'], '1', True),
])
def test_docker_debug_output(options, debug_env, debug, tmp_path,
                             monkeypatch, caplog):
    monkeypatch.delenv('ARCHERY_DEBUG', raising=False)
    if debug_env is not None:
        monkeypatch.setenv('ARCHERY_DEBUG', debug_env)
    monkeypatch.setattr(log_ctx, 'quiet', False)
    (tmp_path / 'cpp').mkdir()
    (tmp_path / 'cpp' / 'CMakeLists.txt').touch()
    config = '''
x-hierarchy:
  - test
services:
  test:
    image: test
    build:
      context: .
      dockerfile: Dockerfile
'''
    (tmp_path / 'compose.yaml').write_text(config)
    args = options + ['docker', '--src', str(tmp_path),
                      'build', '--no-pull', 'test']

    with caplog.at_level(logging.DEBUG, logger='archery'):
        logger.setLevel(logging.INFO)
        with patch('subprocess.run', return_value=CompletedProcess(
                [], 0, stdout=config.encode(), stderr=b'')) as run:
            result = CliRunner().invoke(archery, args)

    assert result.exit_code == 0, result.output
    calls = run.call_args_list
    assert any(call.args[0] == ['docker', 'version']
               for call in calls) is debug
    build = next(call for call in calls if 'build' in call.args[0])
    assert ('--progress=plain' in build.args[0]) is (debug and os.name != 'nt')
    assert (build.kwargs.get('stdout') == PIPE) is ('--quiet' in options)
    assert any(record.name == 'archery' and record.levelno == logging.DEBUG
               and 'Executing' in record.message
               for record in caplog.records) is debug


def test_non_docker_debug_default(monkeypatch, caplog):
    monkeypatch.delenv('ARCHERY_DEBUG', raising=False)
    monkeypatch.setattr(log_ctx, 'quiet', False)
    with caplog.at_level(logging.DEBUG, logger='archery'):
        logger.setLevel(logging.INFO)
        with patch('archery.linking.check_dynamic_library_dependencies',
                   side_effect=lambda *args, **kwargs:
                   logger.debug('Checking dependencies')) as check:
            result = CliRunner().invoke(
                archery, ['linking', 'check-dependencies', 'somelib.so'])

    assert result.exit_code == 0, result.output
    check.assert_called_once()
    assert not any(record.message == 'Checking dependencies'
                   for record in caplog.records)


@patch.object(DockerCompose, "pull")
@patch.object(DockerCompose, "build")
@patch.object(DockerCompose, "run")
def test_docker_run_with_custom_command(run, build, pull):
    # with custom command
    args = ["run", "ubuntu-cpp", "bash"]
    result = CliRunner().invoke(docker, args)

    assert result.exit_code == 0
    pull.assert_called_once_with(
        "ubuntu-cpp", pull_leaf=True,
    )
    build.assert_called_once_with(
        "ubuntu-cpp",
        use_cache=True,
        use_leaf_cache=True,
    )
    run.assert_called_once_with(
        "ubuntu-cpp",
        command="bash",
        env={},
        resource_limit=None,
        user=None,
        volumes=(),
    )


@patch.object(DockerCompose, "pull")
@patch.object(DockerCompose, "build")
@patch.object(DockerCompose, "run")
def test_docker_run_options(run, build, pull):
    # environment variables and volumes
    args = [
        "run",
        "-e",
        "ARROW_GANDIVA=OFF",
        "-e",
        "ARROW_FLIGHT=ON",
        "--volume",
        "./build:/build",
        "-v",
        "./ccache:/ccache:delegated",
        "-u",
        "root",
        "ubuntu-cpp",
    ]
    result = CliRunner().invoke(docker, args)
    assert result.exit_code == 0
    pull.assert_called_once_with(
        "ubuntu-cpp", pull_leaf=True,
    )
    build.assert_called_once_with(
        "ubuntu-cpp",
        use_cache=True,
        use_leaf_cache=True,
    )
    run.assert_called_once_with(
        "ubuntu-cpp",
        command=None,
        env={"ARROW_GANDIVA": "OFF", "ARROW_FLIGHT": "ON"},
        resource_limit=None,
        user="root",
        volumes=(
            "./build:/build",
            "./ccache:/ccache:delegated",
        ),
    )


@patch.object(DockerCompose, "run")
def test_docker_limit_options(run):
    # environment variables and volumes
    args = [
        "run",
        "-e",
        "ARROW_GANDIVA=OFF",
        "-e",
        "ARROW_FLIGHT=ON",
        "--volume",
        "./build:/build",
        "-v",
        "./ccache:/ccache:delegated",
        "-u",
        "root",
        "--resource-limit=github",
        "--no-build",
        "--no-pull",
        "ubuntu-cpp",
    ]
    result = CliRunner().invoke(docker, args)
    assert result.exit_code == 0
    run.assert_called_once_with(
        "ubuntu-cpp",
        command=None,
        env={"ARROW_GANDIVA": "OFF", "ARROW_FLIGHT": "ON"},
        resource_limit="github",
        user="root",
        volumes=(
            "./build:/build",
            "./ccache:/ccache:delegated",
        ),
    )


@patch.object(DockerCompose, "run")
def test_docker_run_without_pulling_or_building(run):
    args = ["run", "--no-pull", "--no-build", "ubuntu-cpp"]
    result = CliRunner().invoke(docker, args)
    assert result.exit_code == 0
    run.assert_called_once_with(
        "ubuntu-cpp",
        command=None,
        env={},
        resource_limit=None,
        user=None,
        volumes=(),
    )


@patch.object(DockerCompose, "pull")
@patch.object(DockerCompose, "build")
def test_docker_run_only_pulling_and_building(build, pull):
    args = ["run", "ubuntu-cpp", "--build-only"]
    result = CliRunner().invoke(docker, args)
    assert result.exit_code == 0
    pull.assert_called_once_with(
        "ubuntu-cpp", pull_leaf=True,
    )
    build.assert_called_once_with(
        "ubuntu-cpp",
        use_cache=True,
        use_leaf_cache=True,
    )


@patch.object(DockerCompose, "build")
@patch.object(DockerCompose, "run")
def test_docker_run_without_build_cache(run, build):
    args = [
        "run",
        "--no-pull",
        "--force-build",
        "--user",
        "me",
        "--no-cache",
        "--no-leaf-cache",
        "ubuntu-cpp",
    ]
    result = CliRunner().invoke(docker, args)
    assert result.exit_code == 0
    build.assert_called_once_with(
        "ubuntu-cpp",
        use_cache=False,
        use_leaf_cache=False,
    )
    run.assert_called_once_with(
        "ubuntu-cpp",
        command=None,
        env={},
        resource_limit=None,
        user="me",
        volumes=(),
    )
