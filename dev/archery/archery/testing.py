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

from contextlib import contextmanager
import os
from unittest import mock
import re


class DotDict(dict):

    def __getattr__(self, key):
        try:
            item = self[key]
        except KeyError:
            raise AttributeError(key)
        if isinstance(item, dict):
            return DotDict(item)
        else:
            return item


class PartialEnv(dict):

    def __eq__(self, other):
        return self.items() <= other.items()


_mock_call_type = type(mock.call())


def _ensure_mock_call_object(obj, **kwargs):
    if isinstance(obj, _mock_call_type):
        return obj
    elif isinstance(obj, str):
        cmd = re.split(r"\s+", obj)
        return mock.call(cmd, **kwargs)
    elif isinstance(obj, list):
        return mock.call(obj, **kwargs)
    else:
        raise TypeError(obj)


class SuccessfulSubprocessResult:

    def check_returncode(self):
        return


@contextmanager
def assert_subprocess_calls(expected_commands_or_calls, **kwargs):
    calls = [
        _ensure_mock_call_object(obj, **kwargs)
        for obj in expected_commands_or_calls
    ]
    with mock.patch('subprocess.run', autospec=True) as run:
        run.return_value = SuccessfulSubprocessResult()
        yield run
        run.assert_has_calls(calls)


@contextmanager
def override_env(mapping):
    original = os.environ
    try:
        os.environ = dict(os.environ, **mapping)
        yield os.environ
    finally:
        os.environ = original
