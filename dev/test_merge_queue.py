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

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
from ruamel.yaml import YAML


ROOT = Path(__file__).resolve().parents[1]


def read_yaml(path):
    return YAML(typ="safe").load((ROOT / path).read_text())


@pytest.mark.parametrize("event,result,passes", [
    ("pull_request", "skipped", True),
    ("merge_group", "success", True),
    ("merge_group", "failure", False),
    ("merge_group", "cancelled", False),
    ("merge_group", "skipped", False),
    ("workflow_dispatch", "success", True),
    ("workflow_dispatch", "failure", False),
    ("push", "success", False),
    ("merge_group", None, False),
])
def test_required_result(event, result, passes):
    gate = read_yaml(".github/workflows/merge_queue.yml")["jobs"]["required"]
    expected = "skipped" if event == "pull_request" else "success"
    needs = {name: {"result": expected} for name in gate["needs"]}
    if result is None:
        needs = {}
    else:
        needs[gate["needs"][0]]["result"] = result
    process = subprocess.run(
        [sys.executable, "-c", gate["steps"][0]["run"]],
        env={**os.environ, "EVENT_NAME": event, "NEEDS": json.dumps(needs)},
        capture_output=True, text=True,
    )
    assert (process.returncode == 0) == passes, process.stdout + process.stderr


def test_required_workflow_wiring():
    workflow = read_yaml(".github/workflows/merge_queue.yml")
    jobs = workflow["jobs"]
    gate = jobs["required"]
    assert set(gate["needs"]) == set(jobs) - {"required"}
    assert gate["if"] == "always()"
    assert "push" not in workflow["on"]
    protection = read_yaml(".asf.yaml")["github"]["protected_branches"]["main"]
    assert protection["required_status_checks"]["contexts"] == [gate["name"]]
    groups = [workflow["concurrency"]["group"]]
    for name in gate["needs"]:
        child = read_yaml(jobs[name]["uses"])
        assert "workflow_call" in child["on"]
        if "concurrency" in child:
            groups.append(child["concurrency"]["group"])
    # Called workflows share github.workflow with the caller.
    assert len(groups) == len(set(groups))
    for path in (ROOT / ".github/workflows").glob("*.yml"):
        triggers = read_yaml(path)["on"]
        if isinstance(triggers, dict):
            push = triggers.get("push") or {}
            if "**" in push.get("branches", []):
                assert "!gh-readonly-queue/**" in push["branches"], path
