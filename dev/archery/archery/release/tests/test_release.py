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

import pytest

from archery.release.core import (
    Release, MajorRelease, MinorRelease, PatchRelease,
    IssueTracker, Version, Issue, CommitTitle, Commit
)
from archery.testing import DotDict


# subset of issues per revision
_issues = {
    "3.0.0": [
        Issue("GH-9784", type="Bug", summary="[C++] Title"),
        Issue("GH-9767", type="New Feature", summary="[Crossbow] Title"),
        Issue("GH-1231", type="Bug", summary="[Java] Title"),
        Issue("GH-1244", type="Bug", summary="[C++] Title"),
        Issue("GH-1301", type="Bug", summary="[Python][Archery] Title")
    ],
    "2.0.0": [
        Issue("ARROW-9784", type="Bug", summary="[Java] Title"),
        Issue("ARROW-9767", type="New Feature", summary="[Crossbow] Title"),
        Issue("GH-1230", type="Bug", summary="[Dev] Title"),
        Issue("ARROW-9694", type="Bug", summary="[Release] Title"),
        Issue("ARROW-5643", type="Bug", summary="[Go] Title"),
        Issue("GH-1243", type="Bug", summary="[Python] Title"),
        Issue("GH-1300", type="Bug", summary="[CI][Archery] Title")
    ],
    "1.0.1": [
        Issue("ARROW-9684", type="Bug", summary="[C++] Title"),
        Issue("ARROW-9667", type="New Feature", summary="[Crossbow] Title"),
        Issue("ARROW-9659", type="Bug", summary="[C++] Title"),
        Issue("ARROW-9644", type="Bug", summary="[C++][Dataset] Title"),
        Issue("ARROW-9643", type="Bug", summary="[C++] Title"),
        Issue("ARROW-9609", type="Bug", summary="[C++] Title"),
        Issue("ARROW-9606", type="Bug", summary="[C++][Dataset] Title")
    ],
    "1.0.0": [
        Issue("ARROW-300", type="New Feature", summary="[Format] Title"),
        Issue("ARROW-4427", type="Task", summary="[Doc] Title"),
        Issue("ARROW-5035", type="Improvement", summary="[C#] Title"),
        Issue("ARROW-8473", type="Bug", summary="[Rust] Title"),
        Issue("ARROW-8472", type="Bug", summary="[Go][Integration] Title"),
        Issue("ARROW-8471", type="Bug", summary="[C++][Integration] Title"),
        Issue("ARROW-8974", type="Improvement", summary="[C++] Title"),
        Issue("ARROW-8973", type="New Feature", summary="[Java] Title")
    ],
    "0.17.1": [
        Issue("ARROW-8684", type="Bug", summary="[Python] Title"),
        Issue("ARROW-8657", type="Bug", summary="[C++][Parquet] Title"),
        Issue("ARROW-8641", type="Bug", summary="[Python] Title"),
        Issue("ARROW-8609", type="Bug", summary="[C++] Title"),
    ],
    "0.17.0": [
        Issue("ARROW-2882", type="New Feature", summary="[C++][Python] Title"),
        Issue("ARROW-2587", type="Bug", summary="[Python] Title"),
        Issue("ARROW-2447", type="Improvement", summary="[C++] Title"),
        Issue("ARROW-2255", type="Bug", summary="[Integration] Title"),
        Issue("ARROW-1907", type="Bug", summary="[C++/Python] Title"),
        Issue("ARROW-1636", type="New Feature", summary="[Format] Title")
    ]
}


class FakeIssueTracker(IssueTracker):

    def __init__(self):
        pass

    def project_versions(self):
        return [
            Version.parse("4.0.0", released=False),
            Version.parse("3.0.0", released=False),
            Version.parse("2.0.0", released=False),
            Version.parse("1.1.0", released=False),
            Version.parse("1.0.1", released=False),
            Version.parse("1.0.0", released=True),
            Version.parse("0.17.1", released=True),
            Version.parse("0.17.0", released=True),
            Version.parse("0.16.0", released=True),
            Version.parse("0.15.2", released=True),
            Version.parse("0.15.1", released=True),
            Version.parse("0.15.0", released=True),
        ]

    def project_issues(self, version):
        return _issues[str(version)]


@pytest.fixture
def fake_issue_tracker():
    return FakeIssueTracker()


def test_version(fake_issue_tracker):
    v = Version.parse("1.2.5")
    assert str(v) == "1.2.5"
    assert v.major == 1
    assert v.minor == 2
    assert v.patch == 5
    assert v.released is False
    assert v.release_date is None

    v = Version.parse("1.0.0", released=True, release_date="2020-01-01")
    assert str(v) == "1.0.0"
    assert v.major == 1
    assert v.minor == 0
    assert v.patch == 0
    assert v.released is True
    assert v.release_date == "2020-01-01"


def test_issue(fake_issue_tracker):
    i = Issue("ARROW-1234", type='Bug', summary="title")
    assert i.key == "ARROW-1234"
    assert i.type == "Bug"
    assert i.summary == "title"
    assert i.project == "ARROW"
    assert i.number == 1234

    i = Issue("PARQUET-1111", type='Improvement', summary="another title")
    assert i.key == "PARQUET-1111"
    assert i.type == "Improvement"
    assert i.summary == "another title"
    assert i.project == "PARQUET"
    assert i.number == 1111

    fake_jira_issue = DotDict({
        'key': 'ARROW-2222',
        'fields': {
            'issuetype': {
                'name': 'Feature'
            },
            'summary': 'Issue title'
        }
    })
    i = Issue.from_jira(fake_jira_issue)
    assert i.key == "ARROW-2222"
    assert i.type == "Feature"
    assert i.summary == "Issue title"
    assert i.project == "ARROW"
    assert i.number == 2222


def test_commit_title():
    t = CommitTitle.parse(
        "ARROW-9598: [C++][Parquet] Fix writing nullable structs"
    )
    assert t.project == "ARROW"
    assert t.issue == "ARROW-9598"
    assert t.components == ["C++", "Parquet"]
    assert t.summary == "Fix writing nullable structs"
    assert t.minor is False

    t = CommitTitle.parse(
        "ARROW-8002: [C++][Dataset][R] Support partitioned dataset writing"
    )
    assert t.project == "ARROW"
    assert t.issue == "ARROW-8002"
    assert t.components == ["C++", "Dataset", "R"]
    assert t.summary == "Support partitioned dataset writing"
    assert t.minor is False

    t = CommitTitle.parse(
        "ARROW-9600: [Rust][Arrow] pin older version of proc-macro2 during "
        "build"
    )
    assert t.project == "ARROW"
    assert t.issue == "ARROW-9600"
    assert t.components == ["Rust", "Arrow"]
    assert t.summary == "pin older version of proc-macro2 during build"
    assert t.minor is False

    t = CommitTitle.parse("[Release] Update versions for 1.0.0")
    assert t.project is None
    assert t.issue is None
    assert t.components == ["Release"]
    assert t.summary == "Update versions for 1.0.0"
    assert t.minor is False

    t = CommitTitle.parse("MINOR: [Release] Update versions for 1.0.0")
    assert t.project is None
    assert t.issue is None
    assert t.components == ["Release"]
    assert t.summary == "Update versions for 1.0.0"
    assert t.minor is True

    t = CommitTitle.parse("[Python][Doc] Fix rst role dataset.rst (#7725)")
    assert t.project is None
    assert t.issue is None
    assert t.components == ["Python", "Doc"]
    assert t.summary == "Fix rst role dataset.rst (#7725)"
    assert t.minor is False

    t = CommitTitle.parse(
        "PARQUET-1882: [C++] Buffered Reads should allow for 0 length"
    )
    assert t.project == 'PARQUET'
    assert t.issue == 'PARQUET-1882'
    assert t.components == ["C++"]
    assert t.summary == "Buffered Reads should allow for 0 length"
    assert t.minor is False

    t = CommitTitle.parse(
        "ARROW-9340 [R] Use CRAN version of decor package "
        "\nsomething else\n"
        "\nwhich should be truncated"
    )
    assert t.project == 'ARROW'
    assert t.issue == 'ARROW-9340'
    assert t.components == ["R"]
    assert t.summary == "Use CRAN version of decor package "
    assert t.minor is False


def test_release_basics(fake_issue_tracker):
    r = Release("1.0.0", repo=None, issue_tracker=fake_issue_tracker)
    assert isinstance(r, MajorRelease)
    assert r.is_released is True
    assert r.branch == 'maint-1.0.0'
    assert r.tag == 'apache-arrow-1.0.0'

    r = Release("1.1.0", repo=None, issue_tracker=fake_issue_tracker)
    assert isinstance(r, MinorRelease)
    assert r.is_released is False
    assert r.branch == 'maint-1.x.x'
    assert r.tag == 'apache-arrow-1.1.0'

    # minor releases before 1.0 are treated as major releases
    r = Release("0.17.0", repo=None, issue_tracker=fake_issue_tracker)
    assert isinstance(r, MajorRelease)
    assert r.is_released is True
    assert r.branch == 'maint-0.17.0'
    assert r.tag == 'apache-arrow-0.17.0'

    r = Release("0.17.1", repo=None, issue_tracker=fake_issue_tracker)
    assert isinstance(r, PatchRelease)
    assert r.is_released is True
    assert r.branch == 'maint-0.17.x'
    assert r.tag == 'apache-arrow-0.17.1'


def test_previous_and_next_release(fake_issue_tracker):
    r = Release("4.0.0", repo=None, issue_tracker=fake_issue_tracker)
    assert isinstance(r.previous, MajorRelease)
    assert r.previous.version == Version.parse("3.0.0")
    with pytest.raises(ValueError, match="There is no upcoming release set"):
        assert r.next

    r = Release("3.0.0", repo=None, issue_tracker=fake_issue_tracker)
    assert isinstance(r.previous, MajorRelease)
    assert isinstance(r.next, MajorRelease)
    assert r.previous.version == Version.parse("2.0.0")
    assert r.next.version == Version.parse("4.0.0")

    r = Release("1.1.0", repo=None, issue_tracker=fake_issue_tracker)
    assert isinstance(r.previous, MajorRelease)
    assert isinstance(r.next, MajorRelease)
    assert r.previous.version == Version.parse("1.0.0")
    assert r.next.version == Version.parse("2.0.0")

    r = Release("1.0.0", repo=None, issue_tracker=fake_issue_tracker)
    assert isinstance(r.next, MajorRelease)
    assert isinstance(r.previous, MajorRelease)
    assert r.previous.version == Version.parse("0.17.0")
    assert r.next.version == Version.parse("2.0.0")

    r = Release("0.17.0", repo=None, issue_tracker=fake_issue_tracker)
    assert isinstance(r.previous, MajorRelease)
    assert r.previous.version == Version.parse("0.16.0")

    r = Release("0.15.2", repo=None, issue_tracker=fake_issue_tracker)
    assert isinstance(r.previous, PatchRelease)
    assert isinstance(r.next, MajorRelease)
    assert r.previous.version == Version.parse("0.15.1")
    assert r.next.version == Version.parse("0.16.0")

    r = Release("0.15.1", repo=None, issue_tracker=fake_issue_tracker)
    assert isinstance(r.previous, MajorRelease)
    assert isinstance(r.next, PatchRelease)
    assert r.previous.version == Version.parse("0.15.0")
    assert r.next.version == Version.parse("0.15.2")


def test_release_issues(fake_issue_tracker):
    # major release issues
    r = Release("1.0.0", repo=None, issue_tracker=fake_issue_tracker)
    assert r.issues.keys() == set([
        "ARROW-300",
        "ARROW-4427",
        "ARROW-5035",
        "ARROW-8473",
        "ARROW-8472",
        "ARROW-8471",
        "ARROW-8974",
        "ARROW-8973"
    ])
    # minor release issues
    r = Release("0.17.0", repo=None, issue_tracker=fake_issue_tracker)
    assert r.issues.keys() == set([
        "ARROW-2882",
        "ARROW-2587",
        "ARROW-2447",
        "ARROW-2255",
        "ARROW-1907",
        "ARROW-1636",
    ])
    # patch release issues
    r = Release("1.0.1", repo=None, issue_tracker=fake_issue_tracker)
    assert r.issues.keys() == set([
        "ARROW-9684",
        "ARROW-9667",
        "ARROW-9659",
        "ARROW-9644",
        "ARROW-9643",
        "ARROW-9609",
        "ARROW-9606"
    ])
    r = Release("2.0.0", repo=None, issue_tracker=fake_issue_tracker)
    assert r.issues.keys() == set([
        "ARROW-9784",
        "ARROW-9767",
        "GH-1230",
        "ARROW-9694",
        "ARROW-5643",
        "GH-1243",
        "GH-1300"
    ])


@pytest.mark.parametrize(('version', 'ncommits'), [
    ("1.0.0", 771),
    ("0.17.1", 27),
    ("0.17.0", 569),
    ("0.15.1", 41)
])
def test_release_commits(fake_issue_tracker, version, ncommits):
    r = Release(version, repo=None, issue_tracker=fake_issue_tracker)
    assert len(r.commits) == ncommits
    for c in r.commits:
        assert isinstance(c, Commit)
        assert isinstance(c.title, CommitTitle)
        assert c.url.endswith(c.hexsha)


def test_maintenance_patch_selection(fake_issue_tracker):
    r = Release("0.17.1", repo=None, issue_tracker=fake_issue_tracker)

    shas_to_pick = [
        c.hexsha for c in r.commits_to_pick(exclude_already_applied=False)
    ]
    expected = [
        '8939b4bd446ee406d5225c79d563a27d30fd7d6d',
        'bcef6c95a324417e85e0140f9745d342cd8784b3',
        '6002ec388840de5622e39af85abdc57a2cccc9b2',
        '9123dadfd123bca7af4eaa9455f5b0d1ca8b929d',
    ]
    assert shas_to_pick == expected
