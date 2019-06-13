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
FakeFields = namedtuple('fields', ['status', 'summary', 'assignee',
                                   'components'])
FakeAssignee = namedtuple('assignee', ['displayName'])
FakeStatus = namedtuple('status', ['name'])
FakeComponent = namedtuple('component', ['name'])
FakeProjectVersion = namedtuple('version', ['name', 'raw'])

RAW_VERSION_JSON = [
    {'version': 'JS-0.4.0', 'released': False},
    {'version': '0.11.0', 'released': False},
    {'version': '0.12.0', 'released': False},
    {'version': '0.10.0', 'released': True},
    {'version': '0.9.0', 'released': True}
]


SOURCE_VERSIONS = [FakeProjectVersion(raw['version'], raw)
                   for raw in RAW_VERSION_JSON]

TRANSITIONS = [{'name': 'Resolve Issue', 'id': 1}]

jira_id = 'ARROW-1234'
status = FakeStatus('In Progress')
fields = FakeFields(status, 'issue summary', FakeAssignee('groundhog'),
                    [FakeComponent('C++'), FakeComponent('Format')])
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

    def get_candidate_fix_versions(self):
        return SOURCE_VERSIONS, ['0.12.0']

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
    assert all_versions == SOURCE_VERSIONS
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


def test_jira_resolve_released_fix_version():
    # ARROW-5083
    jira = FakeJIRA(issue=FAKE_ISSUE_1,
                    project_versions=SOURCE_VERSIONS,
                    transitions=TRANSITIONS)

    cmd = FakeCLI(responses=['0.9.0'])
    fix_versions_json = merge_arrow_pr.prompt_for_fix_version(cmd, jira)
    assert fix_versions_json == [RAW_VERSION_JSON[-1]]


def test_jira_already_resolved():
    status = FakeStatus('Resolved')
    fields = FakeFields(status, 'issue summary', FakeAssignee('groundhog'),
                        [FakeComponent('Java')])
    issue = FakeIssue(fields)

    jira = FakeJIRA(issue=issue,
                    project_versions=SOURCE_VERSIONS,
                    transitions=TRANSITIONS)

    fix_versions = [SOURCE_VERSIONS[0].raw]
    issue = merge_arrow_pr.JiraIssue(jira, 'ARROW-1234', 'ARROW', FakeCLI())

    with pytest.raises(Exception,
                       match="ARROW-1234 already has status 'Resolved'"):
        issue.resolve(fix_versions, "")


def test_jira_output_no_components():
    # ARROW-5472
    status = 'Interesting work'
    components = []
    output = merge_arrow_pr.format_resolved_issue_status(
        'ARROW-1234', 'Resolved', status, FakeAssignee('Foo Bar'),
        components)

    assert output == """=== JIRA ARROW-1234 ===
Summary\t\tInteresting work
Assignee\tFoo Bar
Components\tNO COMPONENTS!!!
Status\t\tResolved
URL\t\thttps://issues.apache.org/jira/browse/ARROW-1234"""

    output = merge_arrow_pr.format_resolved_issue_status(
        'ARROW-1234', 'Resolved', status, FakeAssignee('Foo Bar'),
        [FakeComponent('C++'), FakeComponent('Python')])

    assert output == """=== JIRA ARROW-1234 ===
Summary\t\tInteresting work
Assignee\tFoo Bar
Components\tC++, Python
Status\t\tResolved
URL\t\thttps://issues.apache.org/jira/browse/ARROW-1234"""
