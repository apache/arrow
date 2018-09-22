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
FakeStatus = namedtuple('status', ['name'])
FakeProjectVersion = namedtuple('version', ['name', 'raw'])


class FakeJIRA:

    def __init__(self, fake_issue=None, fake_project_versions=None):
        self.fake_issue = fake_issue
        self.fake_project_versions = fake_project_versions

    def issue(self, jira_id):
        return self.fake_issue

    def project_versions(self, project):
        return self.fake_project_versions


class FakeCLI:

    def __init__(self, responses):
        self.responses = responses
        self.position = 0

    def prompt(self, prompt):
        response = self.responses[self.position]
        self.position += 1
        return response


def test_jira_fix_versions():
    source_versions = [FakeProjectVersion('JS-0.4.0', {'released': False}),
                       FakeProjectVersion('0.11.0', {'released': False}),
                       FakeProjectVersion('0.12.0', {'released': False}),
                       FakeProjectVersion('0.10.0', {'released': True}),
                       FakeProjectVersion('0.9.0', {'released': True})]

    jira = FakeJIRA(fake_project_versions=source_versions)


    issue = merge_arrow_pr.JiraIssue(jira, 'ARROW-1234', 'ARROW')
    fix_versions = issue.get_candidate_fix_versions()

    expected = {source_versions[1], source_versions[2]}
    assert set(fix_versions) == expected


def test_jira_invalid_issue():
    pass


def test_jira_resolve():
    jira_id = 'ARROW-1234'
    status = FakeStatus(jira_id)
    fields = FakeFields(status, 'issue summary', 'groundhog')
    fake_issue = FakeIssue(fields)
