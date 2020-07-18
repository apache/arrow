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

import collections
import os
import re
import subprocess
from unittest import mock

import pytest

from archery.docker import DockerCompose
from archery.testing import assert_subprocess_calls, override_env, PartialEnv


missing_service_compose_yml = """
version: '3.5'

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
    image: dummy
  sub-sub-foo:
    image: dummy
  another-sub-sub-foo:
    image: dummy
  bar:
    image: dummy
  sub-bar:
    image: dummy
  baz:
    image: dummy
"""

missing_node_compose_yml = """
version: '3.5'

x-hierarchy:
  - foo:
    - sub-foo:
      - sub-sub-foo
      - another-sub-sub-foo
  - bar
  - baz

services:
  foo:
    image: dummy
  sub-foo:
    image: dummy
  sub-sub-foo:
    image: dummy
  another-sub-sub-foo:
    image: dummy
  bar:
    image: dummy
  sub-bar:
    image: dummy
  baz:
    image: dummy
"""

ok_compose_yml = """
version: '3.5'

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
    image: dummy
  sub-foo:
    image: dummy
  sub-sub-foo:
    image: dummy
  another-sub-sub-foo:
    image: dummy
  bar:
    image: dummy
  sub-bar:
    image: dummy
  baz:
    image: dummy
"""

arrow_compose_yml = """
version: '3.5'

x-with-gpus:
  - ubuntu-cuda

x-hierarchy:
  - conda-cpp:
    - conda-python:
      - conda-python-pandas
      - conda-python-dask
    - conda-r
  - ubuntu-cpp:
    - ubuntu-cpp-cmake32
    - ubuntu-c-glib:
      - ubuntu-ruby
  - ubuntu-cuda

services:
  conda-cpp:
    image: dummy
  conda-python:
    image: dummy
  conda-python-pandas:
    image: dummy
  conda-python-dask:
    image: dummy
  conda-r:
    image: dummy
  ubuntu-cpp:
    image: dummy
  ubuntu-cpp-cmake32:
    image: dummy
  ubuntu-c-glib:
    image: dummy
  ubuntu-ruby:
    image: dummy
  ubuntu-cuda:
    image: dummy-cuda
    environment:
      CUDA_ENV: 1
      OTHER_ENV: 2
    volumes:
     - /host:/container
    command: /bin/bash -c "echo 1 > /tmp/dummy && cat /tmp/dummy"
"""

arrow_compose_env = {
    'UBUNTU': '20.04',  # overridden below
    'PYTHON': '3.6',
    'PANDAS': 'latest',
    'DASK': 'latest',  # overridden below
}


def create_config(directory, yml_content, env_content=None):
    env_path = directory / '.env'
    config_path = directory / 'docker-compose.yml'

    with config_path.open('w') as fp:
        fp.write(yml_content)

    if env_content is not None:
        with env_path.open('w') as fp:
            for k, v in env_content.items():
                fp.write("{}={}\n".format(k, v))

    return config_path


def format_run(args):
    cmd = ["run", "--rm"]
    if isinstance(args, str):
        return " ".join(cmd + [args])
    else:
        return cmd + args


@pytest.fixture
def arrow_compose_path(tmpdir):
    return create_config(tmpdir, arrow_compose_yml, arrow_compose_env)


def test_config_validation(tmpdir):
    config_path = create_config(tmpdir, missing_service_compose_yml)
    msg = "`sub-foo` is defined in `x-hierarchy` bot not in `services`"
    with pytest.raises(ValueError, match=msg):
        DockerCompose(config_path)

    config_path = create_config(tmpdir, missing_node_compose_yml)
    msg = "`sub-bar` is defined in `services` but not in `x-hierarchy`"
    with pytest.raises(ValueError, match=msg):
        DockerCompose(config_path)

    config_path = create_config(tmpdir, ok_compose_yml)
    DockerCompose(config_path)  # no issue


def assert_docker_calls(compose, expected_args):
    base_command = ['docker']
    expected_commands = []
    for args in expected_args:
        if isinstance(args, str):
            args = re.split(r"\s", args)
        expected_commands.append(base_command + args)
    return assert_subprocess_calls(expected_commands, check=True)


def assert_compose_calls(compose, expected_args, env=mock.ANY):
    base_command = ['docker-compose', '--file', str(compose.config_path)]
    expected_commands = []
    for args in expected_args:
        if isinstance(args, str):
            args = re.split(r"\s", args)
        expected_commands.append(base_command + args)
    return assert_subprocess_calls(expected_commands, check=True, env=env)


def test_arrow_example_validation_passes(arrow_compose_path):
    DockerCompose(arrow_compose_path)


def test_compose_default_params_and_env(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path, params=dict(
        UBUNTU='18.04',
        DASK='master'
    ))
    assert compose.dotenv == arrow_compose_env
    assert compose.params == {
        'UBUNTU': '18.04',
        'DASK': 'master',
    }


def test_forwarding_env_variables(arrow_compose_path):
    expected_calls = [
        "pull --ignore-pull-failures conda-cpp",
        "build conda-cpp",
    ]
    expected_env = PartialEnv(
        MY_CUSTOM_VAR_A='a',
        MY_CUSTOM_VAR_B='b'
    )
    with override_env({'MY_CUSTOM_VAR_A': 'a', 'MY_CUSTOM_VAR_B': 'b'}):
        compose = DockerCompose(arrow_compose_path)
        with assert_compose_calls(compose, expected_calls, env=expected_env):
            assert os.environ['MY_CUSTOM_VAR_A'] == 'a'
            assert os.environ['MY_CUSTOM_VAR_B'] == 'b'
            compose.pull('conda-cpp')
            compose.build('conda-cpp')


def test_compose_pull(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path)

    expected_calls = [
        "pull --ignore-pull-failures conda-cpp",
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.pull('conda-cpp')

    expected_calls = [
        "pull --ignore-pull-failures conda-cpp",
        "pull --ignore-pull-failures conda-python",
        "pull --ignore-pull-failures conda-python-pandas"
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.pull('conda-python-pandas')

    expected_calls = [
        "pull --ignore-pull-failures conda-cpp",
        "pull --ignore-pull-failures conda-python",
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.pull('conda-python-pandas', pull_leaf=False)


def test_compose_pull_params(arrow_compose_path):
    expected_calls = [
        "pull --ignore-pull-failures conda-cpp",
        "pull --ignore-pull-failures conda-python",
    ]
    compose = DockerCompose(arrow_compose_path, params=dict(UBUNTU='18.04'))
    expected_env = PartialEnv(PYTHON='3.6', PANDAS='latest')
    with assert_compose_calls(compose, expected_calls, env=expected_env):
        compose.pull('conda-python-pandas', pull_leaf=False)


def test_compose_build(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path)

    expected_calls = [
        "build conda-cpp",
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.build('conda-cpp')

    expected_calls = [
        "build --no-cache conda-cpp"
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.build('conda-cpp', use_cache=False)

    expected_calls = [
        "build conda-cpp",
        "build conda-python",
        "build conda-python-pandas"
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.build('conda-python-pandas')

    expected_calls = [
        "build --no-cache conda-cpp",
        "build --no-cache conda-python",
        "build --no-cache conda-python-pandas",
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.build('conda-python-pandas', use_cache=False)

    expected_calls = [
        "build conda-cpp",
        "build conda-python",
        "build --no-cache conda-python-pandas",
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.build('conda-python-pandas', use_cache=True,
                      use_leaf_cache=False)


def test_compose_build_params(arrow_compose_path):
    expected_calls = [
        "build ubuntu-cpp",
    ]

    compose = DockerCompose(arrow_compose_path, params=dict(UBUNTU='18.04'))
    expected_env = PartialEnv(UBUNTU="18.04")
    with assert_compose_calls(compose, expected_calls, env=expected_env):
        compose.build('ubuntu-cpp')

    compose = DockerCompose(arrow_compose_path, params=dict(UBUNTU='16.04'))
    expected_env = PartialEnv(UBUNTU="16.04")
    with assert_compose_calls(compose, expected_calls, env=expected_env):
        compose.build('ubuntu-cpp')

    expected_calls = [
        "build --no-cache conda-cpp",
        "build --no-cache conda-python",
        "build --no-cache conda-python-pandas",
    ]
    compose = DockerCompose(arrow_compose_path, params=dict(UBUNTU='18.04'))
    expected_env = PartialEnv(PYTHON='3.6', PANDAS='latest')
    with assert_compose_calls(compose, expected_calls, env=expected_env):
        compose.build('conda-python-pandas', use_cache=False)


def test_compose_run(arrow_compose_path):
    expected_calls = [
        format_run("conda-cpp"),
    ]
    compose = DockerCompose(arrow_compose_path)
    with assert_compose_calls(compose, expected_calls):
        compose.run('conda-cpp')

    expected_calls = [
        format_run("conda-python")
    ]
    expected_env = PartialEnv(PYTHON='3.6')
    with assert_compose_calls(compose, expected_calls, env=expected_env):
        compose.run('conda-python')

    compose = DockerCompose(arrow_compose_path, params=dict(PYTHON='3.8'))
    expected_env = PartialEnv(PYTHON='3.8')
    with assert_compose_calls(compose, expected_calls, env=expected_env):
        compose.run('conda-python')

    compose = DockerCompose(arrow_compose_path, params=dict(PYTHON='3.8'))
    for command in ["bash", "echo 1"]:
        expected_calls = [
            format_run(["conda-python", command]),
        ]
        expected_env = PartialEnv(PYTHON='3.8')
        with assert_compose_calls(compose, expected_calls, env=expected_env):
            compose.run('conda-python', command)

    expected_calls = [
        (
            format_run("-e CONTAINER_ENV_VAR_A=a -e CONTAINER_ENV_VAR_B=b "
                       "conda-python")
        )
    ]
    compose = DockerCompose(arrow_compose_path)
    expected_env = PartialEnv(PYTHON='3.6')
    with assert_compose_calls(compose, expected_calls, env=expected_env):
        env = collections.OrderedDict([
            ("CONTAINER_ENV_VAR_A", "a"),
            ("CONTAINER_ENV_VAR_B", "b")
        ])
        compose.run('conda-python', env=env)

    expected_calls = [
        (
            format_run("--volume /host/build:/build --volume "
                       "/host/ccache:/ccache:delegated conda-python")
        )
    ]
    compose = DockerCompose(arrow_compose_path)
    with assert_compose_calls(compose, expected_calls):
        volumes = ("/host/build:/build", "/host/ccache:/ccache:delegated")
        compose.run('conda-python', volumes=volumes)


def test_compose_run_force_pull_and_build(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path)

    expected_calls = [
        "pull --ignore-pull-failures conda-cpp",
        format_run("conda-cpp")
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.run('conda-cpp', force_pull=True)

    expected_calls = [
        "build conda-cpp",
        format_run("conda-cpp")
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.run('conda-cpp', force_build=True)

    expected_calls = [
        "pull --ignore-pull-failures conda-cpp",
        "build conda-cpp",
        format_run("conda-cpp")
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.run('conda-cpp', force_pull=True, force_build=True)

    expected_calls = [
        "pull --ignore-pull-failures conda-cpp",
        "pull --ignore-pull-failures conda-python",
        "pull --ignore-pull-failures conda-python-pandas",
        "build conda-cpp",
        "build conda-python",
        "build conda-python-pandas",
        format_run("conda-python-pandas bash")
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.run('conda-python-pandas', command='bash', force_build=True,
                    force_pull=True)

    expected_calls = [
        "pull --ignore-pull-failures conda-cpp",
        "pull --ignore-pull-failures conda-python",
        "build conda-cpp",
        "build conda-python",
        "build --no-cache conda-python-pandas",
        format_run("conda-python-pandas bash")
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.run('conda-python-pandas', command='bash', force_build=True,
                    force_pull=True, use_leaf_cache=False)


def test_compose_push(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path, params=dict(PYTHON='3.8'))
    expected_env = PartialEnv(PYTHON="3.8")
    expected_calls = [
        mock.call(["docker", "login", "-u", "user", "-p", "pass"], check=True),
    ]
    for image in ["conda-cpp", "conda-python", "conda-python-pandas"]:
        expected_calls.append(
            mock.call(["docker-compose", "--file", str(compose.config_path),
                       "push", image], check=True, env=expected_env)
        )
    with assert_subprocess_calls(expected_calls):
        compose.push('conda-python-pandas', user='user', password='pass')


def test_compose_error(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path, params=dict(
        PYTHON='3.8',
        PANDAS='master'
    ))

    error = subprocess.CalledProcessError(99, [])
    with mock.patch('subprocess.run', side_effect=error):
        with pytest.raises(RuntimeError) as exc:
            compose.run('conda-cpp')

    exception_message = str(exc.value)
    assert "exited with a non-zero exit code 99" in exception_message
    assert "PANDAS: latest" in exception_message
    assert "export PANDAS=master" in exception_message


def test_image_with_gpu(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path)

    expected_calls = [
        [
            "run", "--rm", "-it", "--gpus", "all",
            "-e", "CUDA_ENV=1",
            "-e", "OTHER_ENV=2",
            "-v", "/host:/container:rw",
            "dummy-cuda",
            "/bin/bash", "-c", "echo 1 > /tmp/dummy && cat /tmp/dummy"
        ]
    ]
    with assert_docker_calls(compose, expected_calls):
        compose.run('ubuntu-cuda', force_pull=False, force_build=False)


def test_listing_images(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path)
    assert sorted(compose.images()) == [
        'conda-cpp',
        'conda-python',
        'conda-python-dask',
        'conda-python-pandas',
        'conda-r',
        'ubuntu-c-glib',
        'ubuntu-cpp',
        'ubuntu-cpp-cmake32',
        'ubuntu-cuda',
        'ubuntu-ruby',
    ]
