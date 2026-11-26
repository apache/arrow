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

import pytest

from archery.integration.datagen import File
from archery.integration.runner import IntegrationRunner
from archery.integration.scenario import Scenario


@pytest.mark.parametrize("match, expected_files, expected_scenarios", [
    (None, ["primitive", "flight_sql_data"],
     ["auth:basic_proto", "flight_sql", "flight_sql:extension"]),
    ("", ["primitive", "flight_sql_data"],
     ["auth:basic_proto", "flight_sql", "flight_sql:extension"]),
    ("flight_sql", ["flight_sql_data"],
     ["flight_sql", "flight_sql:extension"]),
    ("flight_sql:extension", [], ["flight_sql:extension"]),
    ("primitive", ["primitive"], []),
    ("not-a-scenario", [], []),
])
def test_match_filters_files_and_flight_scenarios(
        tmp_path, match, expected_files, expected_scenarios):
    files = [File(name, schema=None, batches=None, path=name + ".json")
             for name in ["primitive", "flight_sql_data"]]
    scenarios = [Scenario(name, description=name) for name in
                 ["auth:basic_proto", "flight_sql", "flight_sql:extension"]]
    runner = IntegrationRunner(files, scenarios, [], [],
                               tempdir=str(tmp_path), match=match)

    assert [case.name for case in runner.json_files] == expected_files
    assert [case.name for case in runner.flight_scenarios] == expected_scenarios
    # Filtering one runner must not change the reusable source catalog.
    assert len(files) == 2
    assert len(scenarios) == 3


class RecordingTester:
    FLIGHT_SERVER = True
    FLIGHT_CLIENT = True
    CONSUMER = True

    def __init__(self, name, calls):
        self.name = name
        self.calls = calls

    @contextmanager
    def flight_server(self, scenario_name=None):
        self.calls.append(("serve", self.name, scenario_name))
        yield 12345
        self.calls.append(("stop", self.name, scenario_name))

    def flight_request(self, port, **kwargs):
        assert port == 12345
        self.calls.append(("request", self.name, kwargs["scenario_name"]))


@pytest.mark.parametrize("serial", [True, False])
def test_flight_match_applies_to_every_implementation_pair(tmp_path, serial):
    calls = []
    # Test doubles verify dispatch, not language implementation conformance.
    testers = [RecordingTester("first", calls), RecordingTester("second", calls)]
    scenarios = [Scenario("middleware", "Not selected"),
                 Scenario("flight_sql", "Selected"),
                 Scenario("flight_sql:extension", "Known unsupported",
                          skip_testers={"first", "second"})]
    runner = IntegrationRunner([], scenarios, testers[:1], testers[1:],
                               tempdir=str(tmp_path), match="flight_sql",
                               serial=serial)
    runner.run_flight()

    assert not runner.failures
    assert len(runner.skips) == 3
    assert len([c for c in calls if c[0] == "request"]) == 3
    assert len([c for c in calls if c[0] == "stop"]) == 3
    assert all(c[2] == "flight_sql" for c in calls)
