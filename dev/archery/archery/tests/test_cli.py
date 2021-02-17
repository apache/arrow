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

from click.testing import CliRunner

from archery.cli import archery
from archery.docker import DockerCompose


@patch.object(DockerCompose, "pull")
@patch.object(DockerCompose, "build")
@patch.object(DockerCompose, "run")
def test_docker_run_with_custom_command(run, build, pull):
    # with custom command
    args = ["docker", "run", "ubuntu-cpp", "bash"]
    result = CliRunner().invoke(archery, args)
    assert result.exit_code == 0
    pull.assert_called_once_with(
        "ubuntu-cpp", pull_leaf=True, using_docker=False
    )
    build.assert_called_once_with(
        "ubuntu-cpp",
        use_cache=True,
        use_leaf_cache=True,
        using_docker=False,
        using_buildx=False
    )
    run.assert_called_once_with(
        "ubuntu-cpp",
        command="bash",
        env={},
        user=None,
        using_docker=False,
        volumes=(),
    )


@patch.object(DockerCompose, "pull")
@patch.object(DockerCompose, "build")
@patch.object(DockerCompose, "run")
def test_docker_run_options(run, build, pull):
    # environment variables and volumes
    args = [
        "docker",
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
    result = CliRunner().invoke(archery, args)
    assert result.exit_code == 0
    pull.assert_called_once_with(
        "ubuntu-cpp", pull_leaf=True, using_docker=False
    )
    build.assert_called_once_with(
        "ubuntu-cpp",
        use_cache=True,
        use_leaf_cache=True,
        using_docker=False,
        using_buildx=False
    )
    run.assert_called_once_with(
        "ubuntu-cpp",
        command=None,
        env={"ARROW_GANDIVA": "OFF", "ARROW_FLIGHT": "ON"},
        user="root",
        using_docker=False,
        volumes=(
            "./build:/build",
            "./ccache:/ccache:delegated",
        ),
    )


@patch.object(DockerCompose, "run")
def test_docker_run_without_pulling_or_building(run):
    args = ["docker", "run", "--no-pull", "--no-build", "ubuntu-cpp"]
    result = CliRunner().invoke(archery, args)
    assert result.exit_code == 0
    run.assert_called_once_with(
        "ubuntu-cpp",
        command=None,
        env={},
        user=None,
        using_docker=False,
        volumes=(),
    )


@patch.object(DockerCompose, "pull")
@patch.object(DockerCompose, "build")
def test_docker_run_only_pulling_and_building(build, pull):
    args = ["docker", "run", "ubuntu-cpp", "--build-only"]
    result = CliRunner().invoke(archery, args)
    assert result.exit_code == 0
    pull.assert_called_once_with(
        "ubuntu-cpp", pull_leaf=True, using_docker=False
    )
    build.assert_called_once_with(
        "ubuntu-cpp",
        use_cache=True,
        use_leaf_cache=True,
        using_docker=False,
        using_buildx=False
    )


@patch.object(DockerCompose, "build")
@patch.object(DockerCompose, "run")
def test_docker_run_without_build_cache(run, build):
    args = [
        "docker",
        "run",
        "--no-pull",
        "--force-build",
        "--user",
        "me",
        "--no-cache",
        "--no-leaf-cache",
        "ubuntu-cpp",
    ]
    result = CliRunner().invoke(archery, args)
    assert result.exit_code == 0
    build.assert_called_once_with(
        "ubuntu-cpp",
        use_cache=False,
        use_leaf_cache=False,
        using_docker=False,
        using_buildx=False
    )
    run.assert_called_once_with(
        "ubuntu-cpp",
        command=None,
        env={},
        user="me",
        using_docker=False,
        volumes=(),
    )
