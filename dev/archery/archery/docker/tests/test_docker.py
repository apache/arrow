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
    image: org/foo
  sub-sub-foo:
    image: org/sub-sub-foo
  another-sub-sub-foo:
    image: org/another-sub-sub-foo
  bar:
    image: org/bar
  sub-bar:
    image: org/sub-bar
  baz:
    image: org/baz
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
    image: org/foo
  sub-foo:
    image: org/sub-foo
  sub-sub-foo:
    image: org/sub-foo-foo
  another-sub-sub-foo:
    image: org/another-sub-sub-foo
  bar:
    image: org/bar
  sub-bar:
    image: org/sub-bar
  baz:
    image: org/baz
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
    image: org/foo
  sub-foo:
    image: org/sub-foo
  sub-sub-foo:
    image: org/sub-sub-foo
  another-sub-sub-foo:
    image: org/another-sub-sub-foo
  bar:
    image: org/bar
  sub-bar:
    image: org/sub-bar
  baz:
    image: org/baz
"""

arrow_compose_yml = """
version: '3.5'

x-sccache: &sccache
  AWS_ACCESS_KEY_ID:
  AWS_SECRET_ACCESS_KEY:
  SCCACHE_BUCKET:

x-with-gpus:
  - ubuntu-cuda

x-hierarchy:
  - conda-cpp:
    - conda-python:
      - conda-python-pandas
      - conda-python-dask
  - ubuntu-cpp:
    - ubuntu-cpp-cmake32
    - ubuntu-c-glib:
      - ubuntu-ruby
  - ubuntu-cuda

x-limit-presets:
  github:
    cpuset_cpus: [0, 1]
    memory: 7g

services:
  conda-cpp:
    image: org/conda-cpp
    build:
      context: .
      dockerfile: ci/docker/conda-cpp.dockerfile
  conda-python:
    image: org/conda-python
    build:
      context: .
      dockerfile: ci/docker/conda-cpp.dockerfile
      args:
        python: 3.8
  conda-python-pandas:
    image: org/conda-python-pandas
    build:
      context: .
      dockerfile: ci/docker/conda-python-pandas.dockerfile
  conda-python-dask:
    image: org/conda-python-dask
  ubuntu-cpp:
    image: org/ubuntu-cpp
    build:
      context: .
      dockerfile: ci/docker/ubuntu-${UBUNTU}-cpp.dockerfile
  ubuntu-cpp-cmake32:
    image: org/ubuntu-cpp-cmake32
  ubuntu-c-glib:
    image: org/ubuntu-c-glib
    environment:
      <<: [*sccache]
  ubuntu-ruby:
    image: org/ubuntu-ruby
  ubuntu-cuda:
    image: org/ubuntu-cuda
    environment:
      CUDA_ENV: 1
      OTHER_ENV: 2
    volumes:
     - /host:/container
    command: /bin/bash -c "echo 1 > /tmp/dummy && cat /tmp/dummy"
"""

arrow_compose_env = {
    'UBUNTU': '20.04',  # overridden below
    'PYTHON': '3.8',
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
    base_command = ['docker-compose', '--file', str(compose.config.path)]
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
        DASK='upstream_devel'
    ))
    assert compose.config.dotenv == arrow_compose_env
    assert compose.config.params == {
        'UBUNTU': '18.04',
        'DASK': 'upstream_devel',
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
        compose.clear_pull_memory()
        compose.pull('conda-cpp')

    expected_calls = [
        "pull --ignore-pull-failures conda-cpp",
        "pull --ignore-pull-failures conda-python",
        "pull --ignore-pull-failures conda-python-pandas"
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.clear_pull_memory()
        compose.pull('conda-python-pandas')

    expected_calls = [
        "pull --ignore-pull-failures conda-cpp",
        "pull --ignore-pull-failures conda-python",
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.clear_pull_memory()
        compose.pull('conda-python-pandas', pull_leaf=False)


def test_compose_pull_params(arrow_compose_path):
    expected_calls = [
        "pull --ignore-pull-failures conda-cpp",
        "pull --ignore-pull-failures conda-python",
    ]
    compose = DockerCompose(arrow_compose_path, params=dict(UBUNTU='18.04'))
    expected_env = PartialEnv(PYTHON='3.8', PANDAS='latest')
    with assert_compose_calls(compose, expected_calls, env=expected_env):
        compose.clear_pull_memory()
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


@mock.patch.dict(os.environ, {"BUILDKIT_INLINE_CACHE": "1"})
def test_compose_buildkit_inline_cache(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path)

    expected_calls = [
        "build --build-arg BUILDKIT_INLINE_CACHE=1 conda-cpp",
    ]
    with assert_compose_calls(compose, expected_calls):
        compose.build('conda-cpp')


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
    expected_env = PartialEnv(PYTHON='3.8', PANDAS='latest')
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
    expected_env = PartialEnv(PYTHON='3.8')
    with assert_compose_calls(compose, expected_calls, env=expected_env):
        compose.run('conda-python')

    compose = DockerCompose(arrow_compose_path, params=dict(PYTHON='3.9'))
    expected_env = PartialEnv(PYTHON='3.9')
    with assert_compose_calls(compose, expected_calls, env=expected_env):
        compose.run('conda-python')

    compose = DockerCompose(arrow_compose_path, params=dict(PYTHON='3.9'))
    for command in ["bash", "echo 1"]:
        expected_calls = [
            format_run(["conda-python", command]),
        ]
        expected_env = PartialEnv(PYTHON='3.9')
        with assert_compose_calls(compose, expected_calls, env=expected_env):
            compose.run('conda-python', command)

    expected_calls = [
        (
            format_run("-e CONTAINER_ENV_VAR_A=a -e CONTAINER_ENV_VAR_B=b "
                       "conda-python")
        )
    ]
    compose = DockerCompose(arrow_compose_path)
    expected_env = PartialEnv(PYTHON='3.8')
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


def test_compose_run_with_resource_limits(arrow_compose_path):
    expected_calls = [
        format_run([
            "--cpuset-cpus=0,1",
            "--memory=7g",
            "--memory-swap=7g",
            "org/conda-cpp"
        ]),
    ]
    compose = DockerCompose(arrow_compose_path)
    with assert_docker_calls(compose, expected_calls):
        compose.run('conda-cpp', resource_limit="github")


def test_compose_push(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path, params=dict(PYTHON='3.9'))
    expected_env = PartialEnv(PYTHON="3.9")
    expected_calls = [
        mock.call(["docker", "login", "-u", "user", "-p", "pass"], check=True),
    ]
    for image in ["conda-cpp", "conda-python", "conda-python-pandas"]:
        expected_calls.append(
            mock.call(["docker-compose", "--file", str(compose.config.path),
                       "push", image], check=True, env=expected_env)
        )
    with assert_subprocess_calls(expected_calls):
        compose.push('conda-python-pandas', user='user', password='pass')


def test_compose_error(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path, params=dict(
        PYTHON='3.8',
        PANDAS='upstream_devel'
    ))

    error = subprocess.CalledProcessError(99, [])
    with mock.patch('subprocess.run', side_effect=error):
        with pytest.raises(RuntimeError) as exc:
            compose.run('conda-cpp')

    exception_message = str(exc.value)
    assert "exited with a non-zero exit code 99" in exception_message
    assert "PANDAS: latest" in exception_message
    assert "export PANDAS=upstream_devel" in exception_message


def test_image_with_gpu(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path)

    expected_calls = [
        [
            "run", "--rm", "--gpus", "all",
            "-e", "CUDA_ENV=1",
            "-e", "OTHER_ENV=2",
            "-v", "/host:/container:rw",
            "org/ubuntu-cuda",
            "/bin/bash", "-c", "echo 1 > /tmp/dummy && cat /tmp/dummy",
        ]
    ]
    with assert_docker_calls(compose, expected_calls):
        compose.run('ubuntu-cuda')


def test_listing_images(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path)
    assert sorted(compose.images()) == [
        'conda-cpp',
        'conda-python',
        'conda-python-dask',
        'conda-python-pandas',
        'ubuntu-c-glib',
        'ubuntu-cpp',
        'ubuntu-cpp-cmake32',
        'ubuntu-cuda',
        'ubuntu-ruby',
    ]


def test_service_info(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path)
    service = compose.config.raw_config["services"]["conda-cpp"]
    assert compose.info(service) == [
        "  image: org/conda-cpp",
        "  build",
        "    context: .",
        "    dockerfile: ci/docker/conda-cpp.dockerfile"
    ]


def test_service_info_filters(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path)
    service = compose.config.raw_config["services"]["conda-cpp"]
    assert compose.info(service, filters="dockerfile") == [
        "    dockerfile: ci/docker/conda-cpp.dockerfile"
    ]


def test_service_info_non_existing_filters(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path)
    service = compose.config.raw_config["services"]["conda-cpp"]
    assert compose.info(service, filters="non-existing") == []


def test_service_info_inherited_env(arrow_compose_path):
    compose = DockerCompose(arrow_compose_path)
    service = compose.config.raw_config["services"]["ubuntu-c-glib"]
    assert compose.info(service, filters="environment") == [
        "  environment",
        "    AWS_ACCESS_KEY_ID: <inherited>",
        "    AWS_SECRET_ACCESS_KEY: <inherited>",
        "    SCCACHE_BUCKET: <inherited>"
    ]
