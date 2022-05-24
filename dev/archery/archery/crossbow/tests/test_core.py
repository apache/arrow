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

from archery.utils.source import ArrowSources
from archery.crossbow import Config, Queue

import pathlib
from unittest import mock


def test_config():
    src = ArrowSources.find()
    conf = Config.load_yaml(src.dev / "tasks" / "tasks.yml")
    conf.validate()


def test_task_select(request):
    conf = Config.load_yaml(pathlib.Path(
        request.node.fspath).parent / "fixtures" / "tasks.yaml")
    conf.validate()

    test_out = conf.select(tasks=["test-a-test-two"])
    assert test_out.keys() >= {"test-a-test-two"}


def test_group_select(request):
    conf = Config.load_yaml(pathlib.Path(
        request.node.fspath).parent / "fixtures" / "tasks.yaml")
    conf.validate()

    test_out = conf.select(groups=["test"])
    assert test_out.keys() >= {"test-a-test-two", "test-a-test"}


def test_group_select_blocklist(request):
    conf = Config.load_yaml(pathlib.Path(
        request.node.fspath).parent / "fixtures" / "tasks.yaml")
    conf.validate()

    # we respect the nightly blocklist
    nightly_out = conf.select(groups=["nightly"])
    assert nightly_out.keys() >= {"test-a-test", "nightly-fine"}

    # but if a task is not blocked in both groups, it shows up at least once
    test_nightly_out = conf.select(groups=["nightly", "test"])
    assert test_nightly_out.keys() >= {
        "test-a-test-two", "test-a-test", "nightly-fine"}

    # but can then over-ride by requesting the task
    test_nightly_out = conf.select(
        tasks=["nightly-not-fine", "nightly-fine"], groups=["nightly", "test"])
    assert test_nightly_out.keys() >= {
        "test-a-test-two", "test-a-test", "nightly-fine", "nightly-not-fine"}

    # and we can glob with the blocklist too!
    test_nightly_no_test_out = conf.select(groups=["nightly-no-test"])
    assert test_nightly_no_test_out.keys(
    ) >= {"nightly-fine", "nightly-not-fine"}


def test_latest_for_prefix(request):
    queue = Queue(pathlib.Path(request.node.fspath).parent)
    with mock.patch("archery.crossbow.core.Repo.repo") as mocked_repo:
        mocked_repo.branches = [
            "origin/nightly-packaging-2022-04-10-0",
            "origin/nightly-packaging-2022-04-11-0",
        ]
        with mock.patch("archery.crossbow.core.Queue.get") as mocked_get:
            queue.latest_for_prefix("nightly-packaging-2022-04-10")
            mocked_get.assert_called_once_with(
                "nightly-packaging-2022-04-10-0")

    with mock.patch("archery.crossbow.core.Repo.repo") as mocked_repo:
        mocked_repo.branches = [
            "origin/nightly-packaging-2022-04-10-0",
            "origin/nightly-packaging-2022-04-11-0",
        ]
        with mock.patch("archery.crossbow.core.Queue.get") as mocked_get:
            queue.latest_for_prefix("nightly-packaging")
            mocked_get.assert_called_once_with(
                "nightly-packaging-2022-04-11-0")
