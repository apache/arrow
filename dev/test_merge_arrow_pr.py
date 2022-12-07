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
                                   'components', 'fixVersions'])
FakeAssignee = namedtuple('assignee', ['displayName'])
FakeStatus = namedtuple('status', ['name'])
FakeComponent = namedtuple('component', ['name'])
FakeVersion = namedtuple('version', ['name', 'raw'])

RAW_VERSION_JSON = [
    {'name': 'JS-0.4.0', 'released': False},
    {'name': '0.9.0', 'released': False},
    {'name': '0.10.0', 'released': False},
    {'name': '0.8.0', 'released': True},
    {'name': '0.7.0', 'released': True}
]


SOURCE_VERSIONS = [FakeVersion(raw['name'], raw)
                   for raw in RAW_VERSION_JSON]

TRANSITIONS = [{'name': 'Resolve Issue', 'id': 1}]

jira_id = 'ARROW-1234'
status = FakeStatus('In Progress')
fields = FakeFields(status, 'issue summary', FakeAssignee('groundhog'),
                    [FakeComponent('C++'), FakeComponent('Format')],
                    [])
FAKE_ISSUE_1 = FakeIssue(fields)


class FakeJIRA:

    def __init__(self, issue=None, project_versions=None, transitions=None,
                 current_fix_versions=None):
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

    @property
    def current_versions(self):
        all_versions = self._project_versions or SOURCE_VERSIONS
        return [
            v for v in all_versions if not v.raw.get("released")
        ] + ['0.11.0']

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
    fix_version = merge_arrow_pr.get_candidate_fix_version(
        issue.current_versions
    )
    assert fix_version == '0.9.0'


def test_jira_fix_versions_filters_maintenance():
    maintenance_branches = ["maint-0.9.0"]
    jira = FakeJIRA(project_versions=SOURCE_VERSIONS,
                    transitions=TRANSITIONS)

    issue = merge_arrow_pr.JiraIssue(jira, 'ARROW-1234', 'ARROW', FakeCLI())
    fix_version = merge_arrow_pr.get_candidate_fix_version(
        issue.current_versions,
        maintenance_branches=maintenance_branches
    )
    assert fix_version == '0.10.0'


def test_jira_no_suggest_patch_release():
    versions_json = [
        {'name': '0.9.1', 'released': False},
        {'name': '0.10.0', 'released': False},
    ]

    versions = [FakeVersion(raw['name'], raw) for raw in versions_json]

    jira = FakeJIRA(project_versions=versions, transitions=TRANSITIONS)
    issue = merge_arrow_pr.JiraIssue(jira, 'ARROW-1234', 'ARROW', FakeCLI())
    fix_version = merge_arrow_pr.get_candidate_fix_version(
        issue.current_versions
    )
    assert fix_version == '0.10.0'


def test_jira_parquet_no_suggest_non_cpp():
    # ARROW-7351
    versions_json = [
        {'name': 'cpp-1.5.0', 'released': True},
        {'name': 'cpp-1.6.0', 'released': False},
        {'name': 'cpp-1.7.0', 'released': False},
        {'name': '1.11.0', 'released': False},
        {'name': '1.12.0', 'released': False}
    ]

    versions = [FakeVersion(raw['name'], raw)
                for raw in versions_json]

    jira = FakeJIRA(project_versions=versions, transitions=TRANSITIONS)
    issue = merge_arrow_pr.JiraIssue(jira, 'PARQUET-1713', 'PARQUET',
                                     FakeCLI())
    fix_version = merge_arrow_pr.get_candidate_fix_version(
        issue.current_versions
    )
    assert fix_version == 'cpp-1.6.0'


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
    fix_version = "0.10.0"

    issue = merge_arrow_pr.JiraIssue(jira, 'ARROW-1234', 'ARROW', FakeCLI())
    issue.resolve(fix_version, my_comment)

    assert jira.captured_transition == {
        'jira_id': 'ARROW-1234',
        'transition_id': 1,
        'comment': my_comment,
        'fixVersions': [{'name': '0.10.0', 'released': False}]
    }


def test_jira_resolve_non_mainline():
    jira = FakeJIRA(issue=FAKE_ISSUE_1,
                    project_versions=SOURCE_VERSIONS,
                    transitions=TRANSITIONS)

    my_comment = 'my comment'
    fix_version = "JS-0.4.0"

    issue = merge_arrow_pr.JiraIssue(jira, 'ARROW-1234', 'ARROW', FakeCLI())
    issue.resolve(fix_version, my_comment)

    assert jira.captured_transition == {
        'jira_id': 'ARROW-1234',
        'transition_id': 1,
        'comment': my_comment,
        'fixVersions': [{'name': 'JS-0.4.0', 'released': False}]
    }


def test_jira_resolve_released_fix_version():
    # ARROW-5083
    jira = FakeJIRA(issue=FAKE_ISSUE_1,
                    project_versions=SOURCE_VERSIONS,
                    transitions=TRANSITIONS)

    cmd = FakeCLI(responses=['0.7.0'])
    fix_versions_json = merge_arrow_pr.prompt_for_fix_version(cmd, jira)
    assert fix_versions_json == "0.7.0"


def test_multiple_authors_bad_input():
    a0 = 'Jimbob Crawfish <jimbob.crawfish@gmail.com>'
    a1 = 'Jarvis McCratchett <jarvis.mccratchett@hotmail.com>'
    a2 = 'Hank Miller <hank.miller@protonmail.com>'
    distinct_authors = [a0, a1]

    cmd = FakeCLI(responses=[''])
    primary_author, distinct_other_authors = \
        merge_arrow_pr.get_primary_author(cmd, distinct_authors)
    assert primary_author == a0
    assert distinct_other_authors == [a1]

    cmd = FakeCLI(responses=['oops', a1])
    primary_author, distinct_other_authors = \
        merge_arrow_pr.get_primary_author(cmd, distinct_authors)
    assert primary_author == a1
    assert distinct_other_authors == [a0]

    cmd = FakeCLI(responses=[a2])
    primary_author, distinct_other_authors = \
        merge_arrow_pr.get_primary_author(cmd, distinct_authors)
    assert primary_author == a2
    assert distinct_other_authors == [a0, a1]


def test_jira_already_resolved():
    status = FakeStatus('Resolved')
    fields = FakeFields(status, 'issue summary', FakeAssignee('groundhog'),
                        [FakeComponent('Java')], [])
    issue = FakeIssue(fields)

    jira = FakeJIRA(issue=issue,
                    project_versions=SOURCE_VERSIONS,
                    transitions=TRANSITIONS)

    fix_versions = [SOURCE_VERSIONS[0].raw]
    issue = merge_arrow_pr.JiraIssue(jira, 'ARROW-1234', 'ARROW', FakeCLI())

    with pytest.raises(Exception,
                       match="ARROW-1234 already has status 'Resolved'"):
        issue.resolve(fix_versions, "")


def test_no_unset_point_release_fix_version():
    # ARROW-6915: We have had the problem of issues marked with a point release
    # having their fix versions overwritten by the merge tool. This verifies
    # that existing patch release versions are carried over
    status = FakeStatus('In Progress')

    versions_json = {
        '0.14.2': {'name': '0.14.2', 'id': 1},
        '0.15.1': {'name': '0.15.1', 'id': 2},
        '0.16.0': {'name': '0.16.0', 'id': 3},
        '0.17.0': {'name': '0.17.0', 'id': 4}
    }

    fields = FakeFields(status, 'summary', FakeAssignee('someone'),
                        [FakeComponent('Java')],
                        [FakeVersion(v, versions_json[v])
                         for v in ['0.17.0', '0.15.1', '0.14.2']])
    issue = FakeIssue(fields)

    jira = FakeJIRA(
        issue=issue,
        project_versions=[
            FakeVersion(v, vdata) for v, vdata in versions_json.items()
        ],
        transitions=TRANSITIONS
    )

    issue = merge_arrow_pr.JiraIssue(jira, 'ARROW-1234', 'ARROW', FakeCLI())
    issue.resolve('0.16.0', "a comment")

    assert jira.captured_transition == {
        'jira_id': 'ARROW-1234',
        'transition_id': 1,
        'comment': 'a comment',
        'fixVersions': [versions_json[v]
                        for v in ['0.16.0', '0.15.1', '0.14.2']]
    }

    issue.resolve([versions_json['0.15.1']], "a comment")

    assert jira.captured_transition == {
        'jira_id': 'ARROW-1234',
        'transition_id': 1,
        'comment': 'a comment',
        'fixVersions': [versions_json[v] for v in ['0.15.1', '0.14.2']]
    }


def test_jira_output_no_components():
    # ARROW-5472
    status = 'Interesting work'
    components = []
    output = merge_arrow_pr.format_issue_output(
        "jira", 'ARROW-1234', 'Resolved', status,
        FakeAssignee('Foo Bar'), components
    )

    assert output == """=== JIRA ARROW-1234 ===
Summary\t\tInteresting work
Assignee\tFoo Bar
Components\tNO COMPONENTS!!!
Status\t\tResolved
URL\t\thttps://issues.apache.org/jira/browse/ARROW-1234"""

    output = merge_arrow_pr.format_issue_output(
        "jira", 'ARROW-1234', 'Resolved', status, FakeAssignee('Foo Bar'),
        [FakeComponent('C++'), FakeComponent('Python')]
    )

    assert output == """=== JIRA ARROW-1234 ===
Summary\t\tInteresting work
Assignee\tFoo Bar
Components\tC++, Python
Status\t\tResolved
URL\t\thttps://issues.apache.org/jira/browse/ARROW-1234"""


def test_sorting_versions():
    versions_json = [
        {'name': '11.0.0', 'released': False},
        {'name': '9.0.0', 'released': False},
        {'name': '10.0.0', 'released': False},
    ]
    versions = [FakeVersion(raw['name'], raw) for raw in versions_json]
    fix_version = merge_arrow_pr.get_candidate_fix_version(versions)
    assert fix_version == "9.0.0"
