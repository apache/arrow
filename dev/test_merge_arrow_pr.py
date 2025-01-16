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
FakeFields = namedtuple(
    'fields', ['assignees', 'labels', 'milestone', 'state', 'title'])
FakeAssignee = namedtuple('assignees', ['login'])
FakeLabel = namedtuple('label', ['name'])
FakeMilestone = namedtuple('milestone', ['title', 'state'])

RAW_VERSION_JSON = [
    {'title': 'JS-0.4.0', 'state': 'open'},
    {'title': '1.0.0', 'state': 'open'},
    {'title': '2.0.0', 'state': 'open'},
    {'title': '0.9.0', 'state': 'open'},
    {'title': '0.10.0', 'state': 'open'},
    {'title': '0.8.0', 'state': 'closed'},
    {'title': '0.7.0', 'state': 'closed'}
]

SOURCE_VERSIONS = [FakeMilestone(raw['title'], raw['state'])
                   for raw in RAW_VERSION_JSON]
fake_issue_id = 'GH-1234'
fields = FakeFields([FakeAssignee('groundhog')._asdict()],
                    [FakeLabel('Component: C++')._asdict(),
                     FakeLabel('Component: Format')._asdict()],
                    FakeMilestone('', 'open')._asdict(),
                    'open', '[C++][Format] The issue Title')
FAKE_ISSUE_1 = FakeIssue(fields)


class FakeGitHub:

    def __init__(self, issues=None, project_versions=None, state='open'):
        self._issues = issues
        self._project_versions = project_versions
        self._state = state
        self._transitions = []

    @property
    def issue(self):
        return self._issues[fake_issue_id].fields._asdict()

    @property
    def current_versions(self):
        return [
            v.title for v in self._project_versions if not v.state == 'closed'
        ]

    @property
    def current_fix_versions(self):
        return 'JS-0.4.0'

    @property
    def state(self):
        return self._state

    def get_issue_data(self, issue_id):
        return self._issues.get(issue_id).fields._asdict()

    def get_milestones(self):
        return [v._asdict() for v in self._project_versions]

    def assign_milestone(self, issue_id, milestone):
        self._transitions.append(
            {'action': 'assign_milestone', 'issue_id': issue_id,
             'milestone': milestone})

    def close_issue(self, issue_id, comment):
        self._transitions.append(
            {'action': 'close_issue', 'issue_id': issue_id, 'comment': comment})

    @property
    def captured_transitions(self):
        return self._transitions


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


def test_gh_fix_versions():
    gh = FakeGitHub(issues={fake_issue_id: FAKE_ISSUE_1},
                    project_versions=SOURCE_VERSIONS)

    issue = merge_arrow_pr.GitHubIssue(gh, fake_issue_id, FakeCLI())
    fix_version = merge_arrow_pr.get_candidate_fix_version(
        issue.current_versions
    )
    assert fix_version == '1.0.0'


def test_gh_fix_versions_filters_maintenance():
    maintenance_branches = ["maint-1.0.0"]
    gh = FakeGitHub(issues={fake_issue_id: FAKE_ISSUE_1},
                    project_versions=SOURCE_VERSIONS)

    issue = merge_arrow_pr.GitHubIssue(gh, fake_issue_id, FakeCLI())
    fix_version = merge_arrow_pr.get_candidate_fix_version(
        issue.current_versions,
        maintenance_branches=maintenance_branches
    )
    assert fix_version == '2.0.0'


def test_gh_only_suggest_major_release():
    versions_json = [
        {'name': '0.9.1', 'state': "open"},
        {'name': '0.10.0', 'state': "open"},
        {'name': '1.0.0', 'state': "open"},
    ]

    versions = [FakeMilestone(raw['name'], raw['state']) for raw in versions_json]

    gh = FakeGitHub(issues={fake_issue_id: FAKE_ISSUE_1}, project_versions=versions)
    issue = merge_arrow_pr.GitHubIssue(gh, fake_issue_id, FakeCLI())
    fix_version = merge_arrow_pr.get_candidate_fix_version(
        issue.current_versions
    )
    assert fix_version == '1.0.0'


def test_gh_invalid_issue():
    class Mock:

        def issue(self, gh_id):
            raise Exception("not found")

    with pytest.raises(Exception):
        merge_arrow_pr.GitHubIssue(Mock(), fake_issue_id, FakeCLI())


def test_gh_resolve():
    gh = FakeGitHub(issues={fake_issue_id: FAKE_ISSUE_1},
                    project_versions=SOURCE_VERSIONS)

    my_comment = 'my comment'
    fix_version = "0.10.0"

    issue = merge_arrow_pr.GitHubIssue(gh, fake_issue_id, FakeCLI())
    issue.resolve(fix_version, my_comment, pr_body="")

    assert len(gh.captured_transitions) == 2
    assert gh.captured_transitions[0]['action'] == 'assign_milestone'
    assert gh.captured_transitions[1]['action'] == 'close_issue'
    assert gh.captured_transitions[1]['comment'] == my_comment
    assert gh.captured_transitions[0]['milestone'] == fix_version


def test_gh_resolve_non_mainline():
    gh = FakeGitHub(issues={fake_issue_id: FAKE_ISSUE_1},
                    project_versions=SOURCE_VERSIONS)

    my_comment = 'my comment'
    fix_version = "JS-0.4.0"

    issue = merge_arrow_pr.GitHubIssue(gh, fake_issue_id, FakeCLI())
    issue.resolve(fix_version, my_comment, "")

    assert len(gh.captured_transitions) == 2
    assert gh.captured_transitions[1]['comment'] == my_comment
    assert gh.captured_transitions[0]['milestone'] == fix_version


def test_gh_resolve_released_fix_version():
    # ARROW-5083
    gh = FakeGitHub(issues={fake_issue_id: FAKE_ISSUE_1},
                    project_versions=SOURCE_VERSIONS)

    cmd = FakeCLI(responses=['1.0.0'])
    fix_versions_json = merge_arrow_pr.prompt_for_fix_version(cmd, gh)
    assert fix_versions_json == "1.0.0"


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


def test_gh_already_resolved():
    fields = FakeFields([FakeAssignee('groundhog')._asdict()],
                        [FakeLabel('Component: Java')._asdict()],
                        FakeMilestone('', 'open')._asdict(),
                        'closed', '[Java] The issue Title')
    issue = FakeIssue(fields)

    gh = FakeGitHub(issues={fake_issue_id: issue},
                    project_versions=SOURCE_VERSIONS)

    fix_versions = [SOURCE_VERSIONS[0]._asdict()]
    issue = merge_arrow_pr.GitHubIssue(gh, fake_issue_id, FakeCLI())

    with pytest.raises(Exception,
                       match="GitHub issue GH-1234 already has status 'closed'"):
        issue.resolve(fix_versions, "", "")


def test_gh_output_no_components():
    # ARROW-5472
    status = 'Interesting work'
    output = merge_arrow_pr.format_issue_output(
        'github', 'GH-1234', 'Resolved', status,
        'username', []
    )

    assert output == """=== GITHUB GH-1234 ===
Summary\t\tInteresting work
Assignee\tusername
Components\tNO COMPONENTS!!!
Status\t\tResolved
URL\t\thttps://github.com/apache/arrow/issues/1234"""

    output = merge_arrow_pr.format_issue_output(
        'github', 'GH-1234', 'Resolved', status, 'username',
        [FakeLabel('C++'), FakeLabel('Python')]
    )

    assert output == """=== GITHUB GH-1234 ===
Summary\t\tInteresting work
Assignee\tusername
Components\tC++, Python
Status\t\tResolved
URL\t\thttps://github.com/apache/arrow/issues/1234"""


def test_sorting_versions():
    versions_json = [
        {'title': '11.0.0', 'state': 'open'},
        {'title': '9.0.0', 'state': 'open'},
        {'title': '10.0.0', 'state': 'open'},
    ]
    versions = [FakeMilestone(raw['title'], raw['state']) for raw in versions_json]
    fix_version = merge_arrow_pr.get_candidate_fix_version([x.title for x in versions])
    assert fix_version == "9.0.0"
