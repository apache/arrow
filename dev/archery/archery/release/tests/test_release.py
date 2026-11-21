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

from types import SimpleNamespace

from github import GithubException
import pytest

from archery.release.core import (
    Release, MajorRelease, MinorRelease, PatchRelease,
    IssueTracker, Version, Issue, CommitTitle, Commit,
    count_committers, update_milestones,
)


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


def _make_issue_tracker(nodes, milestones=(), not_found=()):
    queries = []
    edited = []

    def graphql_query(query, variables):
        queries.append((query, variables))
        data, errors = {}, []
        for number, node in nodes.items():
            alias = f"n{number}"
            if f"{alias}:" in query:
                data[alias] = node
                if number in not_found:
                    errors.append({
                        "type": "NOT_FOUND",
                        "path": ["repository", alias],
                        "message": f"Not found: {number}",
                    })
        payload = {"data": {"repository": data}}
        if errors:
            payload["errors"] = errors
            raise GithubException(404, payload, {})
        return {}, payload

    def get_issue(number):
        return SimpleNamespace(
            edit=lambda **kw: edited.append((number, kw["milestone"].title))
        )

    tracker = object.__new__(IssueTracker)
    tracker.github_repo = SimpleNamespace(
        full_name="apache/arrow",
        get_milestones=lambda state="open": [
            m for m in milestones if state == "all" or m.state == state
        ],
        get_issue=get_issue,
        requester=SimpleNamespace(graphql_query=graphql_query),
    )
    return tracker, queries, edited


def test_count_committers():
    commits = [SimpleNamespace(summary=s) for s in [
        "GH-1: [C++] Title (#11)",
        "GH-2: [Python] Title (#12)",
        "GH-2: [Python] Reland (#12)",
        "MINOR: [Release] Update versions for 2.0.0",
        "GH-3: [R] Title (#13)",
        "GH-4: [Docs] Title (#14)",
        "GH-5: [CI] Mention (#10) in the middle",
        "GH-6: [Ruby] Title (#15)",
        "GH-7: [Go] Title (#16)",
        "GH-8: [C++] Issue number (#17)",
    ]]
    nodes = {
        11: {"mergedBy": {"login": "zoe", "name": "Zoe Z"}},
        12: {"mergedBy": {"login": "bob", "name": "Bob B"}},
        13: {"mergedBy": {"login": "zoe", "name": "Zoe Z"}},
        # a user without a name: the login is used
        14: {"mergedBy": {"login": "alice", "name": None}},
        # no merger: not counted
        15: {"mergedBy": None},
        # not a user, for example a bot: the login is used
        16: {"mergedBy": {"login": "carol"}},
        # an issue rather than a pull request: not counted
        17: {},
    }
    tracker, queries, _ = _make_issue_tracker(nodes)

    with pytest.warns(UserWarning) as record:
        committers = count_committers(commits, tracker, batch_size=4)

    assert [str(w.message) for w in record] == [
        "#15 doesn't have a merger. It isn't counted.",
        "#17 doesn't have a merger. It isn't counted.",
    ]
    assert committers == [
        ("Bob B", 2),
        ("Zoe Z", 2),
        ("alice", 1),
        ("carol", 1),
    ]
    sel = "... on PullRequest {mergedBy {login ... on User {name}}}"
    assert queries == [
        (
            'query($owner: String!, $name: String!) '
            '{repository(owner: $owner, name: $name) {' +
            " ".join(f"n{n}: issueOrPullRequest(number: {n}) {{{sel}}}"
                     for n in batch) +
            '}}',
            {"owner": "apache", "name": "arrow"},
        )
        for batch in ([11, 12, 13, 14], [15, 16, 17])
    ]


def _make_fake_repo(commits_by_range, refs=("main",),
                    tags=("apache-arrow-26.0.0", "apache-arrow-26.0.1")):
    return SimpleNamespace(
        tags=[SimpleNamespace(name=t) for t in tags],
        refs={name: SimpleNamespace(name=name) for name in refs},
        iter_commits=lambda r: iter(
            SimpleNamespace(summary=s) for s in commits_by_range[r]
        ),
    )


def _ms(title, number, state="open"):
    return SimpleNamespace(
        title=title, number=number, state=state, due_on=None
    )


def _issue(state="CLOSED", reason="COMPLETED", milestone=None):
    return {"state": state, "stateReason": reason, "milestone": milestone}


def test_update_milestones_with_maintenance_branch():
    repo = _make_fake_repo(
        refs=("main", "origin/maint-27.0.0"),
        commits_by_range={
            "apache-arrow-26.0.0..origin/maint-27.0.0": [
                "GH-110: [CI] Direct maint commit (#11)",
                "GH-103: [Python] Cherry-picked fix (#3)",
                "GH-102: [C++] Partial fix (#2)",
                "GH-101: [C++] Before branch cut (#1)",
            ],
            "origin/maint-27.0.0..main": [
                "MINOR: [Release] Bump version",
                "GH-109: [Dev] Typo issue number (#10)",
                "GH-108: [Dev] Actually a PR (#9)",
                "GH-107: [Dev] Closed not planned (#8)",
                "GH-106: [Dev] Already milestoned (#7)",
                "GH-105: [Docs] Still open (#6)",
                "GH-104: [R] After branch cut (#5)",
                "GH-102: [C++] Complete fix (#4)",
                "GH-103: [Python] Cherry-picked fix (#3)",
            ],
        },
    )
    milestones = [
        _ms("JS-0.4.0", 1, "closed"),
        _ms("26.0.0", 75, "closed"),
        _ms("26.0.1", 76),
        _ms("27.0.0", 77),
        _ms("28.0.0", 78),
    ]
    nodes = {
        101: _issue(),
        102: _issue(),
        103: _issue(),
        104: _issue(),
        105: _issue(state="OPEN", reason=None),
        106: _issue(milestone={"number": 76}),
        107: _issue(reason="NOT_PLANNED"),
        108: {},
        109: None,
        110: _issue(),
    }
    tracker, _, edited = _make_issue_tracker(
        nodes, milestones=milestones, not_found=(109,)
    )
    expected = [
        (101, "27.0.0"),
        (102, "28.0.0"),
        (103, "27.0.0"),
        (104, "28.0.0"),
        (110, "27.0.0"),
    ]

    with pytest.warns(UserWarning, match="#109 not found"):
        assert update_milestones(
            repo, tracker, dry_run=True, batch_size=4
        ) == expected
    assert edited == []

    with pytest.warns(UserWarning, match="#109 not found"):
        assert update_milestones(
            repo, tracker, dry_run=False, batch_size=4
        ) == expected
    assert edited == expected


def test_update_milestones_missing_next_major():
    repo = _make_fake_repo(
        refs=("main", "origin/maint-27.0.0"),
        commits_by_range={
            "apache-arrow-26.0.0..origin/maint-27.0.0": [],
            "origin/maint-27.0.0..main": ["GH-301: [Dev] Fix"],
        },
    )
    milestones = [_ms("26.0.0", 75, "closed"), _ms("27.0.0", 77)]
    tracker, _, _ = _make_issue_tracker({}, milestones=milestones)

    with pytest.raises(ValueError, match="No open major release milestone"):
        update_milestones(repo, tracker, dry_run=True)
