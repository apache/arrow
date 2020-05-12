from collections import OrderedDict
import semver
import warnings
from io import StringIO
from git import Repo
import os
from jira import JIRA
import re
import functools
from semver import VersionInfo as SemVer


def cached_property(fn):
    return property(functools.lru_cache(maxsize=1)(fn))


class JiraVersion(SemVer):

    __slots__ = SemVer.__slots__ + ('released', 'release_date')

    def __init__(self, jira_version):
        super().__init__(**SemVer.parse(jira_version.name).to_dict())
        self.released = jira_version.released
        self.release_date = getattr(jira_version, 'releaseDate', None)


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

    @functools.lru_cache(maxsize=1)
    def arrow_versions(self):
        versions = []
        for v in self.project_versions('ARROW'):
            try:
                versions.append(JiraVersion(v)) # Version.from_jira(v))
            except ValueError:
                # ignore invalid semantic versions like JS-0.4.0
                continue
        return sorted(versions, reverse=True)

    def arrow_issues(self, version):
        query = "project=ARROW AND fixVersion={}".format(version)
        return self.search_issues(query, maxResults=False)


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
    # TODO: support hashing

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


class Curation:

    def __init__(self, release, within, outside, nojira, parquet, nopatch):
        self.release = release
        self.within = within
        self.outside = outside
        self.nojira = nojira
        self.parquet = parquet
        self.nopatch = nopatch

    def render_report(self):
        out = StringIO()

        out.write('Total number of JIRA tickets assigned to version {}: {}\n'
                  .format(self.release.version, len(self.release.issues)))
        out.write('\n')
        out.write('Total number of applied patches since {}: {}\n'
                  .format(self.release.previous.version,
                          len(self.release.commits)))

        out.write('\n\n')

        out.write('Patches with assigned issue in {}:\n'
                  .format(self.release.version))
        for commit in self.within:
            out.write("- {}: {}\n".format(commit.url, commit.title))

        out.write('\n\n')

        out.write('Patches with assigned issue outside of {}:\n'
                  .format(self.release.version))
        for commit in self.outside:
            out.write("- {}: {}\n".format(commit.url, commit.title))

        out.write('\n\n')

        out.write('Patches without assigned issue:\n')
        for commit in self.nojira:
            out.write("- {}: {}\n".format(commit.url, commit.title))

        out.write('\n\n')

        out.write('JIRAs in {} without assigned patch:\n'
                  .format(self.release.version))
        for issue_key in self.nopatch:
            out.write("- {}\n".format(issue_key))

        return out.getvalue()

    def render_changelog(self):
        pass


def _default_repo():
    # TODO: use arrow source
    return Repo("~/Workspace/arrow")


class Release:

    def __init__(self):
        raise TypeError("dont")

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
        repo = repo or _default_repo()

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
    def tag(self):
        return "apache-arrow-{}".format(str(self.version))

    @property
    def branch(self):
        # TODO: add remote
        return "master"

    @cached_property
    def previous(self):
        # select all non-patch releases
        versions = [v for v in self.jira.arrow_versions() if v.patch == 0]
        previous = versions[versions.index(self.version) + 1]
        return Release.from_jira(previous)

    @cached_property
    def issues(self):
        return {i.key: i for i in self.jira.arrow_issues(self.version)}

    @cached_property
    def commits(self):
        # return all commits applied between two versions on the master branch
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
        release_issues = set(self.issues.keys())

        within, outside, nojira, parquet = [], [], [], []
        for c in self.commits:
            if c.issue is None:
                nojira.append(c)
            elif c.issue in release_issues:
                within.append(c)
                release_issues.remove(c.issue)
            elif c.project == 'PARQUET':
                parquet.append(c)
            else:
                outside.append(c)

        # remaining jira tickets
        nopatch = list(release_issues)

        return Curation(self, within=within, outside=outside, nojira=nojira,
                        parquet=parquet, nopatch=nopatch)


class PatchRelease(Release):

    @property
    def branch(self):
        # TODO: add remote
        return "maint-{}.{}.x".format(self.version.major, self.version.minor)

    @cached_property
    def previous(self):
        # select all releases under this minor
        versions = [v for v in self.jira.arrow_versions()
                    if v.minor == self.version.minor]
        previous = versions[versions.index(self.version) + 1]
        return Release.from_jira(previous)

    def update_branch(self):
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
        patch_issues = self.issues
        patch_commits = [c for c in commits if c.issue in patch_issues]

        # only if they are not applied already in the maint branch
        already_picked = {c.hexsha for c in self.commits}

        self.repo.checkout(self.branch)
        for c in patch_commits:
            if c.hexsha not in already_picked:
                self.repo.cherry_pick(c.hexsha)
