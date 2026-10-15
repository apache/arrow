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

import re
import textwrap
from unittest import mock

import pytest

from archery.crossbow.cli import (NightlyEmailReport, TokenExpirationEmailReport)
from archery.crossbow.core import yaml
from archery.crossbow.reports import (ChatReport, CommentReport, Report)


@pytest.mark.parametrize(('ci', 'expected_url'), [
    ('github', 'https://github.com/apache/crossbow/actions?query=branch:{branch}'),
    ('azure', 'https://dev.azure.com/apache/crossbow/_build/latest'
     '?definitionId=1&branchName={branch}'),
    ('travis', 'https://github.com/apache/crossbow/tree/{branch}'),
    ('circle', 'https://github.com/apache/crossbow/tree/{branch}'),
    ('appveyor', 'https://github.com/apache/crossbow/tree/{branch}'),
    ('drone', 'https://github.com/apache/crossbow/tree/{branch}'),
])
def test_crossbow_task_url_without_build_link(load_fixture, ci, expected_url):
    job = load_fixture('crossbow-job.yaml', decoder=yaml.load)
    task = job.tasks['docker-cpp-cmake32']
    task.ci = ci
    task.status().build_links = []

    report = Report(job)
    assert report.task_url(task) == expected_url.format(branch=task.branch)


@pytest.mark.parametrize(('ci', 'expected_url'), [
    ('github', 'https://github.com/apache/crossbow/actions?query=branch:'
     'feature%2Ffoo%2Bbar%26baz'),
    ('azure', 'https://dev.azure.com/apache/crossbow/_build/latest'
     '?definitionId=1&branchName=feature%2Ffoo%2Bbar%26baz'),
])
def test_crossbow_task_url_escaped_branch(load_fixture, ci, expected_url):
    job = load_fixture('crossbow-job.yaml', decoder=yaml.load)
    job.queue._remote_url += '.git'
    task = job.tasks['docker-cpp-cmake32']
    task.ci = ci
    task.branch = 'feature/foo+bar&baz'
    task.status().build_links = []

    assert Report(job).task_url(task) == expected_url


def test_crossbow_task_url_build_link(load_fixture):
    job = load_fixture('crossbow-job.yaml', decoder=yaml.load)
    task = job.tasks['docker-cpp-cmake32']
    task.ci = 'github'
    task.status().build_links = [
        'https://github.com/apache/crossbow/actions/runs/1',
        'https://github.com/apache/crossbow/actions/runs/2',
    ]
    report = Report(job, wait_for_task=10)

    with mock.patch('archery.crossbow.reports.time.sleep') as sleep:
        assert report.task_url(task) == task.status().build_links[0]
        sleep.assert_not_called()


@pytest.mark.parametrize('build_links', [
    [],
    ['https://github.com/apache/crossbow/actions/runs/1'],
])
def test_crossbow_task_url_after_wait(load_fixture, build_links):
    job = load_fixture('crossbow-job.yaml', decoder=yaml.load)
    task = job.tasks['docker-cpp-cmake32']
    task.ci = 'github'
    report = Report(job, wait_for_task=10)
    expected_url = (build_links[0] if build_links else
                    f'https://github.com/apache/crossbow/actions'
                    f'?query=branch:{task.branch}')

    with mock.patch.object(task, 'status', side_effect=[
        mock.Mock(build_links=[]), mock.Mock(build_links=build_links)
    ]) as status, mock.patch('archery.crossbow.reports.time.sleep') as sleep:
        assert report.task_url(task) == expected_url
        sleep.assert_called_once_with(10)
        assert status.call_args_list == [mock.call(), mock.call(force_query=True)]


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


def test_crossbow_chat_report(load_fixture):
    expected_msg = load_fixture('chat-report.txt')
    job = load_fixture('crossbow-job.yaml', decoder=yaml.load)
    report = Report(job)
    assert report.tasks_by_state is not None
    report_chat = ChatReport(report=report, extra_message_success=None,
                             extra_message_failure=None)

    assert report_chat.render("text") == textwrap.dedent(expected_msg)


def test_crossbow_chat_report_extra_message_failure(load_fixture):
    expected_msg = load_fixture('chat-report-extra-message-failure.txt')
    job = load_fixture('crossbow-job.yaml', decoder=yaml.load)
    report = Report(job)
    assert report.tasks_by_state is not None
    report_chat = ChatReport(report=report,
                             extra_message_success="Should not be present",
                             extra_message_failure="Failure present")

    assert report_chat.render("text") == textwrap.dedent(expected_msg)


def test_crossbow_chat_report_extra_message_success(load_fixture):
    expected_msg = load_fixture('chat-report-extra-message-success.txt')
    job = load_fixture('crossbow-job-no-failure.yaml', decoder=yaml.load)
    report = Report(job)
    assert report.tasks_by_state is not None
    report_chat = ChatReport(report=report,
                             extra_message_success="Success present",
                             extra_message_failure="Should not be present")

    assert report_chat.render("text") == textwrap.dedent(expected_msg)


def test_crossbow_nightly_email_report(load_fixture):
    expected_msg = load_fixture('nightly-email-report.txt')
    job = load_fixture('crossbow-job.yaml', decoder=yaml.load)
    report = Report(job)
    assert report.tasks_by_state is not None
    email_report = NightlyEmailReport(
        report=report,
        sender_name='Sender Reporter',
        sender_email='sender@arrow.com',
        recipient_email='recipient@arrow.com'
    )

    actual = str(email_report.render())
    # Normalize dynamic headers
    actual = re.sub(r'(?m)^Message-Id: <.+?>',
                    'Message-Id: <message-id>',
                    actual)
    actual = re.sub(r'(?m)^Date: [^\n]+ -0000$',
                    'Date: date',
                    actual)
    assert actual == textwrap.dedent(expected_msg)


def test_crossbow_token_expiration_email_report(load_fixture):
    expected_msg = load_fixture('token-expiration-email-report.txt')

    class TokenExpirationReport:
        def __init__(self, token_expiration_date, days_left):
            self.token_expiration_date = token_expiration_date
            self.days_left = days_left

    report = TokenExpirationReport('2026-01-17', 7)
    email_report = TokenExpirationEmailReport(
        report=report,
        sender_name='Sender Reporter',
        sender_email='sender@arrow.com',
        recipient_email='recipient@arrow.com'
    )

    actual = str(email_report.render())
    # Normalize dynamic headers
    actual = re.sub(r'(?m)^Message-Id: <.+?>',
                    'Message-Id: <message-id>',
                    actual)
    actual = re.sub(r'(?m)^Date: [^\n]+ -0000$',
                    'Date: date',
                    actual)
    assert actual == textwrap.dedent(expected_msg)


def test_crossbow_export_report(load_fixture):
    job = load_fixture('crossbow-job.yaml', decoder=yaml.load)
    report = Report(job)
    assert len(list(report.rows)) == 4
    expected_first_row = [
        'docker-cpp-cmake32',
        'success',
        ['https://github.com/apache/crossbow/runs/1'],
        'https://github.com/apache/crossbow/tree/'
        'ursabot-1-circle-docker-cpp-cmake32',
        'circle',
        {'commands': ['docker compose build cpp-cmake32',
                      'docker compose run cpp-cmake32']},
        'docker-tests/circle.linux.yml',
        'f766a1d615dd1b7ee706d05102e579195951a61c'
    ]
    assert next(report.rows) == expected_first_row
