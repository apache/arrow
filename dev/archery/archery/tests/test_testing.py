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

import subprocess

import pytest

from archery.testing import PartialEnv, assert_subprocess_calls


def test_partial_env():
    assert PartialEnv(a=1, b=2) == {'a': 1, 'b': 2, 'c': 3}
    assert PartialEnv(a=1) == {'a': 1, 'b': 2, 'c': 3}
    assert PartialEnv(a=1, b=2) == {'a': 1, 'b': 2}
    assert PartialEnv(a=1, b=2) != {'b': 2, 'c': 3}
    assert PartialEnv(a=1, b=2) != {'a': 1, 'c': 3}


def test_assert_subprocess_calls():
    expected_calls = [
        "echo Hello",
        ["echo", "World"]
    ]
    with assert_subprocess_calls(expected_calls):
        subprocess.run(['echo', 'Hello'])
        subprocess.run(['echo', 'World'])

    expected_env = PartialEnv(
        CUSTOM_ENV_A='a',
        CUSTOM_ENV_C='c'
    )
    with assert_subprocess_calls(expected_calls, env=expected_env):
        env = {
            'CUSTOM_ENV_A': 'a',
            'CUSTOM_ENV_B': 'b',
            'CUSTOM_ENV_C': 'c'
        }
        subprocess.run(['echo', 'Hello'], env=env)
        subprocess.run(['echo', 'World'], env=env)

    with pytest.raises(AssertionError):
        with assert_subprocess_calls(expected_calls, env=expected_env):
            env = {
                'CUSTOM_ENV_B': 'b',
                'CUSTOM_ENV_C': 'c'
            }
            subprocess.run(['echo', 'Hello'], env=env)
            subprocess.run(['echo', 'World'], env=env)
