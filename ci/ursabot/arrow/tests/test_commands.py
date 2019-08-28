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

import pytest
from ursabot.commands import CommandError

from ..commands import ursabot


@pytest.mark.parametrize(('command', 'expected_props'), [
    ('build', {'command': 'build'}),
    ('benchmark', {'command': 'benchmark'})
])
def test_ursabot_commands(command, expected_props):
    props = ursabot(command)
    assert props == expected_props


@pytest.mark.parametrize(('command', 'expected_args'), [
    ('crossbow test -g docker', ['-c', 'tests.yml', '-g', 'docker']),
    ('crossbow test -g integration -g docker',
     ['-c', 'tests.yml', '-g', 'integration', '-g', 'docker']),
    ('crossbow test -g docker -g cpp-python',
     ['-c', 'tests.yml', '-g', 'docker', '-g', 'cpp-python']),
    ('crossbow package wheel-osx-cp27m ubuntu-xenial',
     ['-c', 'tasks.yml', 'wheel-osx-cp27m', 'ubuntu-xenial']),
    ('crossbow package -g wheel -g conda',
     ['-c', 'tasks.yml', '-g', 'wheel', '-g', 'conda']),
    ('crossbow package -g wheel -g conda wheel-win-cp37m wheel-osx-cp27m',
     ['-c', 'tasks.yml', '-g', 'wheel', '-g', 'conda', 'wheel-win-cp37m',
      'wheel-osx-cp27m']),
    ('crossbow test docker-python-3.6-nopandas docker-python-3.7-nopandas',
     ['-c', 'tests.yml', 'docker-python-3.6-nopandas',
      'docker-python-3.7-nopandas']),
    ('crossbow test -g cpp-python docker-python-3.6-nopandas',
     ['-c', 'tests.yml', '-g', 'cpp-python', 'docker-python-3.6-nopandas'])
])
def test_crossbow_commands(command, expected_args):
    props = ursabot(command)
    expected = {
        'command': 'crossbow',
        'crossbow_repo': 'https://github.com/ursa-labs/crossbow',
        'crossbow_args': expected_args
    }
    assert props == expected


@pytest.mark.parametrize(('command', 'expected_repo'), [
    ('crossbow test -g docker', 'https://github.com/ursa-labs/crossbow'),
    ('crossbow -r ursa-labs/crossbow test -g docker',
     'https://github.com/ursa-labs/crossbow'),
    ('crossbow -r kszucs/crossbow test -g docker',
     'https://github.com/kszucs/crossbow'),
])
def test_crossbow_repo(command, expected_repo):
    props = ursabot(command)
    expected = {
        'command': 'crossbow',
        'crossbow_repo': expected_repo,
        'crossbow_args': ['-c', 'tests.yml', '-g', 'docker']
    }
    assert props == expected


@pytest.mark.parametrize(('command', 'expected_msg'), [
    ('buil', 'No such command "buil".'),
    ('bench', 'No such command "bench".'),
    ('crossbow something', 'No such command "something".'),
    ('crossbow test -g pkgs', 'Invalid value for "--group" / "-g": '
                              'invalid choice: pkgs. '
                              '(choose from docker, integration, cpp-python)')
])
def test_wrong_commands(command, expected_msg):
    with pytest.raises(CommandError) as excinfo:
        ursabot(command)
    assert excinfo.value.message == expected_msg


@pytest.mark.parametrize('command', [
    '',
    '--help',
])
def test_ursabot_help(command):
    with pytest.raises(CommandError) as excinfo:
        ursabot(command)
    prefix = 'Usage: @ursabot [OPTIONS] COMMAND [ARGS]...'
    assert excinfo.value.message.startswith(prefix)


@pytest.mark.parametrize('command', [
    'crossbow',
    'crossbow --help',
])
def test_ursabot_crossbow_help(command):
    with pytest.raises(CommandError) as excinfo:
        ursabot(command)
    prefix = 'Usage: @ursabot crossbow [OPTIONS] COMMAND [ARGS]...'
    assert excinfo.value.message.startswith(prefix)
