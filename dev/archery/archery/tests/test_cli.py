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

from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from archery.cli import archery


@patch("archery.linking.check_dynamic_library_dependencies")
def test_linking_check_dependencies(fn):
    args = [
        "linking",
        "check-dependencies",
        "-a", "libarrow",
        "-d", "libcurl",
        "somelib.so"
    ]
    result = CliRunner().invoke(archery, args)
    assert result.exit_code == 0
    fn.assert_called_once_with(
        Path('somelib.so'), allowed={'libarrow'}, disallowed={'libcurl'}
    )


@patch("archery.integration.runner.run_all_tests")
@patch("archery.integration.runner.select_testers")
def test_integration_with_all_enables_every_implementation(select_testers,
                                                           run_all_tests):
    select_testers.return_value = (["tester"], [])
    result = CliRunner().invoke(
        archery, ["integration", "--with-all", "--run-ipc"])
    assert result.exit_code == 0
    enabled = {name: value
               for name, value in select_testers.call_args.kwargs.items()
               if name.startswith("with_")}
    assert enabled
    assert all(enabled.values()), enabled


@patch("archery.integration.runner.run_all_tests")
@patch("archery.integration.runner.select_testers")
def test_integration_with_all_yields_to_an_explicit_option(select_testers,
                                                           run_all_tests):
    select_testers.return_value = (["tester"], [])
    result = CliRunner().invoke(
        archery, ["integration", "--with-all", "--with-java=0", "--run-ipc"])
    assert result.exit_code == 0
    kwargs = select_testers.call_args.kwargs
    assert kwargs["with_java"] is False
    assert kwargs["with_cpp"] is True


@patch("archery.integration.runner.run_all_tests")
@patch("archery.integration.runner.select_testers")
def test_integration_without_with_all_enables_nothing(select_testers,
                                                      run_all_tests):
    select_testers.return_value = (["tester"], [])
    result = CliRunner().invoke(archery, ["integration", "--run-ipc"])
    assert result.exit_code == 0
    enabled = {name: value
               for name, value in select_testers.call_args.kwargs.items()
               if name.startswith("with_")}
    assert enabled
    assert not any(enabled.values()), enabled
