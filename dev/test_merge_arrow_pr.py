#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from collections import namedtuple

import pytest

import merge_arrow_pr


FakeIssue = namedtuple('issue', ['fields'])
FakeFields = namedtuple('fields', ['status', 'summary', 'assignee'])
FakeAssignee = namedtuple('assignee', ['displayName'])
FakeStatus = namedtuple('status', ['name'])
FakeProjectVersion = namedtuple('version', ['name', 'raw'])

SOURCE_VERSIONS = [FakeProjectVersion('JS-0.4.0', {'released': False}),
                   FakeProjectVersion('0.11.0', {'released': False}),
                   FakeProjectVersion('0.12.0', {'released': False}),
                   FakeProjectVersion('0.10.0', {'released': True}),
                   FakeProjectVersion('0.9.0', {'released': True})]

TRANSITIONS = [{'name': 'Resolve Issue', 'id': 1}]

jira_id = 'ARROW-1234'
status = FakeStatus('In Progress')
fields = FakeFields(status, 'issue summary', FakeAssignee('groundhog'))
FAKE_ISSUE_1 = FakeIssue(fields)


class FakeJIRA:

    def __init__(self, issue=None, project_versions=None, transitions=None):
        self._issue = issue
        self._project_versions = project_versions
        self._transitions = transitions

    def issue(self, jira_id):
        return self._issue

    def transitions(self, jira_id):
        return self._transitions

    def transition_issue(self, jira_id, transition_id, comment=None,
                         fixVersions=None):
        self.captured_transition = {
            'jira_id': jira_id,
            'transition_id': transition_id,
            'comment': comment,
            'fixVersions': fixVersions
        }

    def project_versions(self, project):
        return self._project_versions


class FakeCLI:

    def __init__(self, responses=()):
        self.responses = responses
        self.position = 0

    def prompt(self, prompt):
        response = self.responses[self.position]
        self.position += 1
        return response

    def fail(self, msg):
        raise Exception(msg)


def test_jira_fix_versions():
    jira = FakeJIRA(project_versions=SOURCE_VERSIONS,
                    transitions=TRANSITIONS)

    issue = merge_arrow_pr.JiraIssue(jira, 'ARROW-1234', 'ARROW', FakeCLI())
    all_versions, default_versions = issue.get_candidate_fix_versions()

    expected = sorted([x for x in SOURCE_VERSIONS
                       if not x.raw['released']],
                      key=lambda x: x.name, reverse=True)
    assert all_versions == expected
    assert default_versions == ['0.11.0']


def test_jira_invalid_issue():
    class Mock:

        def issue(self, jira_id):
            raise Exception("not found")

    with pytest.raises(Exception):
        merge_arrow_pr.JiraIssue(Mock(), 'ARROW-1234', 'ARROW', FakeCLI())


def test_jira_resolve():
    jira = FakeJIRA(issue=FAKE_ISSUE_1,
                    project_versions=SOURCE_VERSIONS,
                    transitions=TRANSITIONS)

    my_comment = 'my comment'
    fix_versions = [SOURCE_VERSIONS[1].raw]

    issue = merge_arrow_pr.JiraIssue(jira, 'ARROW-1234', 'ARROW', FakeCLI())
    issue.resolve(fix_versions, my_comment)

    assert jira.captured_transition == {
        'jira_id': 'ARROW-1234',
        'transition_id': 1,
        'comment': my_comment,
        'fixVersions': fix_versions
    }


def test_jira_resolve_non_mainline():
    jira = FakeJIRA(issue=FAKE_ISSUE_1,
                    project_versions=SOURCE_VERSIONS,
                    transitions=TRANSITIONS)

    my_comment = 'my comment'
    fix_versions = [SOURCE_VERSIONS[0].raw]

    issue = merge_arrow_pr.JiraIssue(jira, 'ARROW-1234', 'ARROW', FakeCLI())
    issue.resolve(fix_versions, my_comment)

    assert jira.captured_transition == {
        'jira_id': 'ARROW-1234',
        'transition_id': 1,
        'comment': my_comment,
        'fixVersions': fix_versions
    }


def test_jira_already_resolved():
    status = FakeStatus('Resolved')
    fields = FakeFields(status, 'issue summary', FakeAssignee('groundhog'))
    issue = FakeIssue(fields)

    jira = FakeJIRA(issue=issue,
                    project_versions=SOURCE_VERSIONS,
                    transitions=TRANSITIONS)

    fix_versions = [SOURCE_VERSIONS[0].raw]
    issue = merge_arrow_pr.JiraIssue(jira, 'ARROW-1234', 'ARROW', FakeCLI())

    with pytest.raises(Exception,
                       match="ARROW-1234 already has status 'Resolved'"):
        issue.resolve(fix_versions, "")
