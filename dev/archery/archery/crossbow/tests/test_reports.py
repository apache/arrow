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

import textwrap

from archery.crossbow.core import yaml
from archery.crossbow.reports import ChatReport, CommentReport, Report


def test_crossbow_comment_formatter(load_fixture):
    msg = load_fixture('crossbow-success-message.md')
    job = load_fixture('crossbow-job.yaml', decoder=yaml.load)

    report = CommentReport(job, crossbow_repo='ursa-labs/crossbow')
    expected = msg.format(
        repo='ursa-labs/crossbow',
        branch='ursabot-1',
        revision='f766a1d615dd1b7ee706d05102e579195951a61c',
        status='has been succeeded.'
    )
    assert report.show() == textwrap.dedent(expected).strip()


def test_crossbow_report(load_fixture):
    expected_msg = load_fixture('chat-report.txt')
    job = load_fixture('crossbow-job.yaml', decoder=yaml.load)
    report = Report(job)
    assert report.tasks_by_state is not None
    report_chat = ChatReport(report=report)

    assert report_chat.render("text") == textwrap.dedent(expected_msg)
