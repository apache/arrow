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

from collections import defaultdict
import functools
import os
import re
import shelve
import warnings

from git import Repo
from jira import JIRA
from semver import VersionInfo as SemVer

from .utils.source import ArrowSources
from .utils.report import JinjaReport


def cached_property(fn):
    return property(functools.lru_cache(maxsize=1)(fn))


class JiraVersion(SemVer):

    __slots__ = SemVer.__slots__ + ('released', 'release_date')

    def __init__(self, original_jira_version):
        super().__init__(**SemVer.parse(original_jira_version.name).to_dict())
        self.released = original_jira_version.released
        self.release_date = getattr(original_jira_version, 'releaseDate', None)


class JiraIssue:

    def __init__(self, original_jira_issue):
        self.key = original_jira_issue.key
        self.type = original_jira_issue.fields.issuetype.name
        self.summary = original_jira_issue.fields.summary

    @property
    def project(self):
        return self.key.split('-')[0]

    @property
    def number(self):
        return int(self.key.split('-')[1])


class Jira(JIRA):

    def __init__(self, user=None, password=None):
        user = user or os.environ.get('APACHE_JIRA_USER')
        password = password or os.environ.get('APACHE_JIRA_PASSWORD')
        super().__init__(
            {'server': 'https://issues.apache.org/jira'},
            basic_auth=(user, password)
        )

    def arrow_version(self, version_string):
        # query version from jira to populated with additional metadata
        versions = self.arrow_versions()
        # JiraVersion instances are comparable with strings
        return versions[versions.index(version_string)]

    def arrow_versions(self):
        versions = []
        for v in self.project_versions('ARROW'):
            try:
                versions.append(JiraVersion(v))
            except ValueError:
                # ignore invalid semantic versions like JS-0.4.0
                continue
        return sorted(versions, reverse=True)

    def issue(self, key):
        return JiraIssue(super().issue(key))

    def arrow_issues(self, version):
        query = "project=ARROW AND fixVersion={}".format(version)
        issues = self.search_issues(query, maxResults=False)
        return list(map(JiraIssue, issues))


class CachedJira:

    def __init__(self, cache_path, jira=None):
        self.jira = jira or Jira()
        self.cache_path = cache_path

    def __getattr__(self, name):
        attr = getattr(self.jira, name)
        return self._cached(name, attr) if callable(attr) else attr

    def _cached(self, name, method):
        def wrapper(*args, **kwargs):
            key = str((name, args, kwargs))
            with shelve.open(self.cache_path) as cache:
                try:
                    result = cache[key]
                except KeyError:
                    cache[key] = result = method(*args, **kwargs)
            return result
        return wrapper


_TITLE_REGEX = re.compile(
    r"(?P<issue>(?P<project>(ARROW|PARQUET))\-\d+)?\s*:?\s*"
    r"(?P<components>\[.*\])?\s*(?P<summary>.*)"
)
_COMPONENT_REGEX = re.compile(r"\[([^\[\]]+)\]")


class CommitTitle:

    def __init__(self, summary, project=None, issue=None, components=None):
        self.project = project
        self.issue = issue
        self.components = components or []
        self.summary = summary

    def __str__(self):
        out = ""
        if self.issue:
            out += "{}: ".format(self.issue)
        if self.components:
            for component in self.components:
                out += "[{}]".format(component)
            out += " "
        out += self.summary
        return out

    @classmethod
    def parse(cls, headline):
        matches = _TITLE_REGEX.match(headline)
        if matches is None:
            warnings.warn(
                "Unable to parse commit message `{}`".format(headline)
            )
            return CommitTitle(headline)

        values = matches.groupdict()
        components = values.get('components') or ''
        components = _COMPONENT_REGEX.findall(components)

        return CommitTitle(
            values['summary'],
            project=values.get('project'),
            issue=values.get('issue'),
            components=components
        )


class Commit:

    def __init__(self, wrapped):
        self._title = CommitTitle.parse(wrapped.summary)
        self._wrapped = wrapped

    def __getattr__(self, attr):
        if hasattr(self._title, attr):
            return getattr(self._title, attr)
        else:
            return getattr(self._wrapped, attr)

    def __repr__(self):
        template = '<Commit sha={!r} issue={!r} components={!r} summary={!r}>'
        return template.format(self.hexsha, self.issue, self.components,
                               self.summary)

    @property
    def url(self):
        return 'https://github.com/apache/arrow/commit/{}'.format(self.hexsha)

    @property
    def title(self):
        return self._title


class ReleaseCuration(JinjaReport):
    templates = {
        'console': 'release_curation.txt.j2'
    }
    fields = [
        'release',
        'within',
        'outside',
        'nojira',
        'parquet',
        'nopatch'
    ]


class JiraChangelog(JinjaReport):
    templates = {
        'markdown': 'release_changelog.md.j2',
        'html': 'release_changelog.html.j2'
    }
    fields = [
        'release',
        'categories'
    ]


class Release:

    def __init__(self):
        raise TypeError("Do not initialize Release class directly, use "
                        "Release.from_jira(version) instead.")

    def __repr__(self):
        if self.version.released:
            status = "released_at={!r}".format(self.version.release_date)
        else:
            status = "pending"
        return "<{} {!r} {}>".format(self.__class__.__name__,
                                     str(self.version), status)

    @classmethod
    def from_jira(cls, version, jira=None, repo=None):
        jira = jira or Jira()

        if repo is None:
            arrow = ArrowSources.find()
            repo = Repo(arrow.path)
        else:
            repo = Repo(repo)

        if isinstance(version, str):
            version = jira.arrow_version(version)
        elif not isinstance(version, JiraVersion):
            raise TypeError(version)

        # decide the type of the release based on the version number
        klass = Release if version.patch == 0 else PatchRelease

        # prevent instantiating release object directly
        obj = klass.__new__(klass)
        obj.version = version
        obj.jira = jira
        obj.repo = repo

        return obj

    @property
    def is_released(self):
        return self.version.released

    @property
    def tag(self):
        return "apache-arrow-{}".format(str(self.version))

    @property
    def branch(self):
        # TODO(kszucs): add apache remote
        return "master"

    @cached_property
    def previous(self):
        # select all non-patch releases
        versions = [v for v in self.jira.arrow_versions() if v.patch == 0]
        position = versions.index(self.version) + 1
        if position == len(versions):
            # first release doesn't have a previous one
            return None
        previous = versions[position]
        return Release.from_jira(previous)

    @cached_property
    def issues(self):
        return {i.key: i for i in self.jira.arrow_issues(self.version)}

    @cached_property
    def commits(self):
        """
        All commits applied between two versions on the master branch.
        """
        if self.previous is None:
            # first release
            lower = ''
        else:
            lower = self.repo.tags[self.previous.tag]

        if self.version.released:
            upper = self.repo.tags[self.tag]
        else:
            try:
                upper = self.repo.branches[self.branch]
            except IndexError:
                warnings.warn("Release branch `{}` doesn't exist."
                              .format(self.branch))
                return []

        commit_range = "{}..{}".format(lower, upper)
        return list(map(Commit, self.repo.iter_commits(commit_range)))

    def curate(self):
        # handle commits with parquet issue key specially and query them from
        # jira and add it to the issues
        release_issues = self.issues

        within, outside, nojira, parquet = [], [], [], []
        for c in self.commits:
            if c.issue is None:
                nojira.append(c)
            elif c.issue in release_issues:
                within.append((release_issues[c.issue], c))
            elif c.project == 'PARQUET':
                parquet.append((self.jira.issue(c.issue), c))
            else:
                outside.append((self.jira.issue(c.issue), c))

        # remaining jira tickets
        within_keys = {i.key for i, c in within}
        nopatch = [issue for key, issue in release_issues.items()
                   if key not in within_keys]

        return ReleaseCuration(release=self, within=within, outside=outside,
                               nojira=nojira, parquet=parquet, nopatch=nopatch)

    def changelog(self):
        release_issues = []

        # get organized report for the release
        curation = self.curate()

        # jira tickets having patches in the release
        for issue, _ in curation.within:
            release_issues.append(issue)

        # jira tickets without patches
        for issue in curation.nopatch:
            release_issues.append(issue)

        # parquet patches in the release
        for issue, _ in curation.parquet:
            release_issues.append(issue)

        # organize issues into categories
        issue_types = {
            'Bug': 'Bug Fixes',
            'Improvement': 'New Features and Improvements',
            'New Feature': 'New Features and Improvements',
            'Sub-task': 'New Features and Improvements',
            'Task': 'New Features and Improvements',
            'Test': 'Bug Fixes',
            'Wish': 'New Features and Improvements',
        }
        categories = defaultdict(list)
        for issue in release_issues:
            categories[issue_types[issue.type]].append(issue)

        # sort issues by the issue key in ascending order
        for name, issues in categories.items():
            issues.sort(key=lambda issue: (issue.project, issue.number))

        return JiraChangelog(release=self, categories=categories)


class PatchRelease(Release):

    @property
    def branch(self):
        # TODO(kszucs): add apache remote
        return "maint-{}.{}.x".format(self.version.major, self.version.minor)

    @cached_property
    def previous(self):
        # select all releases under this minor
        versions = [v for v in self.jira.arrow_versions()
                    if v.minor == self.version.minor]
        previous = versions[versions.index(self.version) + 1]
        return Release.from_jira(previous)

    def generate_update_branch_commands(self):
        # cherry pick not yet cherry picked commits on top of the maintenance
        # branch
        try:
            target = self.repo.branches[self.branch]
        except IndexError:
            # maintenance branch doesn't exist yet, so create one based off of
            # the previous git tag
            target = self.repo.create_head(self.branch, self.previous.tag)

        # collect commits applied on master since the root of the maintenance
        # branch (the minor release of this patch release)
        commit_range = "apache-arrow-{}.{}.0..master".format(
            self.version.major, self.version.minor
        )
        commits = list(map(Commit, self.repo.iter_commits(commit_range)))

        # iterate over commits applied on master and keep the original order of
        # the commits to minimize the merge conflicts during cherry-picks
        patch_commits = [c for c in commits if c.issue in self.issues]

        commands = [
            'git checkout -b {} {}'.format(target, self.previous.tag)
        ]
        for c in reversed(patch_commits):
            commands.append(
                'git cherry-pick {}  # {}'.format(c.hexsha, c.title)
            )

        return commands

    # TODO(kszucs): update_branch method which tries to cherry pick to a
    # temporary branch and if the patches apply cleanly then update the maint
    # reference
