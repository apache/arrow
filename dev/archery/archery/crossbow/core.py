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

import os
import re
import fnmatch
import glob
import time
import logging
import mimetypes
import subprocess
import textwrap
import uuid
from io import StringIO
from pathlib import Path
from datetime import date
import warnings

import jinja2
from ruamel.yaml import YAML

try:
    import github3
    _have_github3 = True
except ImportError:
    github3 = object
    _have_github3 = False

try:
    import pygit2
except ImportError:
    PygitRemoteCallbacks = object
    GitError = Exception
else:
    PygitRemoteCallbacks = pygit2.RemoteCallbacks
    GitError = pygit2.GitError

from ..utils.source import ArrowSources


for pkg in ["requests", "urllib3", "github3"]:
    logging.getLogger(pkg).setLevel(logging.WARNING)

logger = logging.getLogger("crossbow")


class CrossbowError(Exception):
    pass


def _flatten(mapping):
    """Converts a hierarchical mapping to a flat dictionary"""
    result = {}
    for k, v in mapping.items():
        if isinstance(v, dict):
            for ik, iv in _flatten(v).items():
                ik = ik if isinstance(ik, tuple) else (ik,)
                result[(k,) + ik] = iv
        elif isinstance(v, list):
            for ik, iv in enumerate(_flatten(v)):
                ik = ik if isinstance(ik, tuple) else (ik,)
                result[(k,) + ik] = iv
        else:
            result[(k,)] = v
    return result


def _unflatten(mapping):
    """Converts a flat tuple => object mapping to hierarchical one"""
    result = {}
    for path, value in mapping.items():
        parents, leaf = path[:-1], path[-1]
        # create the hierarchy until we reach the leaf value
        temp = result
        for parent in parents:
            temp.setdefault(parent, {})
            temp = temp[parent]
        # set the leaf value
        temp[leaf] = value

    return result


def _unflatten_tree(files):
    """Converts a flat path => object mapping to a hierarchical directories

    Input:
        {
            'path/to/file.a': a_content,
            'path/to/file.b': b_content,
            'path/file.c': c_content
        }
    Output:
        {
            'path': {
                'to': {
                    'file.a': a_content,
                    'file.b': b_content
                },
                'file.c': c_content
            }
        }
    """
    files = {tuple(k.split('/')): v for k, v in files.items()}
    return _unflatten(files)


def _render_jinja_template(searchpath, template, params):
    def format_all(items, pattern):
        return [pattern.format(item) for item in items]

    loader = jinja2.FileSystemLoader(searchpath)
    env = jinja2.Environment(loader=loader, trim_blocks=True,
                             lstrip_blocks=True,
                             undefined=jinja2.StrictUndefined)
    env.filters['format_all'] = format_all
    template = env.get_template(template)
    return template.render(**params)


# configurations for setting up branch skipping
# - appveyor has a feature to skip builds without an appveyor.yml
# - travis reads from the default branch and applies the rules
# - circle requires the configuration to be present on all branch, even ones
#   that are configured to be skipped
# - azure skips branches without azure-pipelines.yml by default
# - github skips branches without .github/workflows/ by default

_default_travis_yml = """
branches:
  only:
    - master
    - /.*-travis-.*/

os: linux
dist: trusty
language: generic
"""

_default_circle_yml = """
version: 2

jobs:
  build:
    machine: true

workflows:
  version: 2
  build:
    jobs:
      - build:
          filters:
            branches:
              only:
                - /.*-circle-.*/
"""

_default_tree = {
    '.travis.yml': _default_travis_yml,
    '.circleci/config.yml': _default_circle_yml
}


class GitRemoteCallbacks(PygitRemoteCallbacks):

    def __init__(self, token):
        self.token = token
        self.attempts = 0
        super().__init__()

    def push_update_reference(self, refname, message):
        pass

    def update_tips(self, refname, old, new):
        pass

    def credentials(self, url, username_from_url, allowed_types):
        # its a libgit2 bug, that it infinitely retries the authentication
        self.attempts += 1

        if self.attempts >= 5:
            # pygit2 doesn't propagate the exception properly
            msg = 'Wrong oauth personal access token'
            print(msg)
            raise CrossbowError(msg)

        if (allowed_types &
                pygit2.credentials.GIT_CREDENTIAL_USERPASS_PLAINTEXT):
            return pygit2.UserPass('x-oauth-basic', self.token)
        else:
            return None


def _git_ssh_to_https(url):
    return url.replace('git@github.com:', 'https://github.com/')


def _parse_github_user_repo(remote_url):
    # TODO: use a proper URL parser instead?
    m = re.match(r'.*\/([^\/]+)\/([^\/\.]+)(\.git|/)?$', remote_url)
    if m is None:
        # Perhaps it's simply "username/reponame"?
        m = re.match(r'^(\w+)/(\w+)$', remote_url)
        if m is None:
            raise CrossbowError(
                f"Unable to parse the github owner and repository from the "
                f"repository's remote url {remote_url!r}"
            )
    user, repo = m.group(1), m.group(2)
    return user, repo


class Repo:
    """
    Base class for interaction with local git repositories

    A high level wrapper used for both reading revision information from
    arrow's repository and pushing continuous integration tasks to the queue
    repository.

    Parameters
    ----------
    require_https : boolean, default False
        Raise exception for SSH origin URLs
    """

    def __init__(self, path, github_token=None, remote_url=None,
                 require_https=False):
        self.path = Path(path)
        self.github_token = github_token
        self.require_https = require_https
        self._remote_url = remote_url
        self._pygit_repo = None
        self._github_repo = None  # set by as_github_repo()
        self._updated_refs = []

    def __str__(self):
        tpl = textwrap.dedent('''
            Repo: {remote}@{branch}
            Commit: {head}
        ''')
        return tpl.format(
            remote=self.remote_url,
            branch=self.branch.branch_name,
            head=self.head
        )

    @property
    def repo(self):
        if self._pygit_repo is None:
            self._pygit_repo = pygit2.Repository(str(self.path))
        return self._pygit_repo

    @property
    def origin(self):
        remote = self.repo.remotes['origin']
        if self.require_https and remote.url.startswith('git@github.com'):
            raise CrossbowError("Change SSH origin URL to HTTPS to use "
                                "Crossbow: {}".format(remote.url))
        return remote

    def fetch(self, retry=3):
        refspec = '+refs/heads/*:refs/remotes/origin/*'
        attempt = 1
        while True:
            try:
                self.origin.fetch([refspec])
                break
            except GitError as e:
                if retry and attempt < retry:
                    attempt += 1
                else:
                    raise e

    def push(self, refs=None, github_token=None):
        github_token = github_token or self.github_token
        if github_token is None:
            raise RuntimeError(
                'Could not determine GitHub token. Please set the '
                'CROSSBOW_GITHUB_TOKEN environment variable to a '
                'valid GitHub access token or pass one to --github-token.'
            )
        callbacks = GitRemoteCallbacks(github_token)
        refs = refs or []
        try:
            self.origin.push(refs + self._updated_refs, callbacks=callbacks)
        except pygit2.GitError:
            raise RuntimeError('Failed to push updated references, '
                               'potentially because of credential issues: {}'
                               .format(self._updated_refs))
        else:
            self.updated_refs = []

    @property
    def head(self):
        """Currently checked out commit's sha"""
        return self.repo.head

    @property
    def branch(self):
        """Currently checked out branch"""
        try:
            return self.repo.branches[self.repo.head.shorthand]
        except KeyError:
            raise CrossbowError(
                'Cannot determine the current branch of the Arrow repository '
                'to clone or push to, perhaps it is in detached HEAD state. '
                'Please checkout a branch.'
            )

    @property
    def remote(self):
        """Currently checked out branch's remote counterpart"""
        try:
            return self.repo.remotes[self.branch.upstream.remote_name]
        except (AttributeError, KeyError):
            raise CrossbowError(
                'Cannot determine git remote for the Arrow repository to '
                'clone or push to, try to push the `{}` branch first to have '
                'a remote tracking counterpart.'.format(self.branch.name)
            )

    @property
    def remote_url(self):
        """Currently checked out branch's remote counterpart URL

        If an SSH github url is set, it will be replaced by the https
        equivalent usable with GitHub OAuth token.
        """
        return self._remote_url or _git_ssh_to_https(self.remote.url)

    @property
    def user_name(self):
        try:
            return next(self.repo.config.get_multivar('user.name'))
        except StopIteration:
            return os.environ.get('GIT_COMMITTER_NAME', 'unknown')

    @property
    def user_email(self):
        try:
            return next(self.repo.config.get_multivar('user.email'))
        except StopIteration:
            return os.environ.get('GIT_COMMITTER_EMAIL', 'unknown')

    @property
    def signature(self):
        return pygit2.Signature(self.user_name, self.user_email,
                                int(time.time()))

    @property
    def default_branch_name(self):
        default_branch_name = os.getenv("ARCHERY_DEFAULT_BRANCH")

        if default_branch_name is None:
            try:
                ref_obj = self.repo.references["refs/remotes/origin/HEAD"]
                target_name = ref_obj.target
                target_name_tokenized = target_name.split("/")
                default_branch_name = target_name_tokenized[-1]
            except KeyError:
                default_branch_name = "main"
                warnings.warn('Unable to determine default branch name: '
                              'ARCHERY_DEFAULT_BRANCH environment variable is '
                              'not set. Git repository does not contain a '
                              '\'refs/remotes/origin/HEAD\'reference. Setting '
                              'the default branch name to ' +
                              default_branch_name, RuntimeWarning)

        return default_branch_name

    def create_tree(self, files):
        builder = self.repo.TreeBuilder()

        for filename, content in files.items():
            if isinstance(content, dict):
                # create a subtree
                tree_id = self.create_tree(content)
                builder.insert(filename, tree_id, pygit2.GIT_FILEMODE_TREE)
            else:
                # create a file
                blob_id = self.repo.create_blob(content)
                builder.insert(filename, blob_id, pygit2.GIT_FILEMODE_BLOB)

        tree_id = builder.write()
        return tree_id

    def create_commit(self, files, parents=None, message='',
                      reference_name=None):
        if parents is None:
            # by default use the main branch as the base of the new branch
            # required to reuse github actions cache across crossbow tasks
            commit, _ = self.repo.resolve_refish(self.default_branch_name)
            parents = [commit.id]
        tree_id = self.create_tree(files)

        author = committer = self.signature
        commit_id = self.repo.create_commit(reference_name, author, committer,
                                            message, tree_id, parents)
        return self.repo[commit_id]

    def create_branch(self, branch_name, files, parents=None, message='',
                      signature=None):
        # create commit with the passed tree
        commit = self.create_commit(files, parents=parents, message=message)

        # create branch pointing to the previously created commit
        branch = self.repo.create_branch(branch_name, commit)

        # append to the pushable references
        self._updated_refs.append('refs/heads/{}'.format(branch_name))

        return branch

    def create_tag(self, tag_name, commit_id, message=''):
        tag_id = self.repo.create_tag(tag_name, commit_id,
                                      pygit2.GIT_OBJ_COMMIT, self.signature,
                                      message)

        # append to the pushable references
        self._updated_refs.append('refs/tags/{}'.format(tag_name))

        return self.repo[tag_id]

    def file_contents(self, commit_id, file):
        commit = self.repo[commit_id]
        entry = commit.tree[file]
        blob = self.repo[entry.id]
        return blob.data

    def _github_login(self, github_token):
        """Returns a logged in github3.GitHub instance"""
        if not _have_github3:
            raise ImportError('Must install github3.py')
        github_token = github_token or self.github_token
        session = github3.session.GitHubSession(
            default_connect_timeout=10,
            default_read_timeout=30
        )
        github = github3.GitHub(session=session)
        github.login(token=github_token)
        return github

    def as_github_repo(self, github_token=None):
        """Converts it to a repository object which wraps the GitHub API"""
        if self._github_repo is None:
            github = self._github_login(github_token)
            username, reponame = _parse_github_user_repo(self.remote_url)
            self._github_repo = github.repository(username, reponame)
        return self._github_repo

    def token_expiration_date(self, github_token=None):
        """Returns the expiration date for the github_token provided"""
        github = self._github_login(github_token)
        # github3 hides the headers from us. Use the _get method
        # to access the response headers.
        resp = github._get(github.session.base_url)
        # Response in the form '2023-01-23 10:40:28 UTC'
        date_string = resp.headers.get(
            'github-authentication-token-expiration')
        if date_string:
            return date.fromisoformat(date_string.split()[0])

    def github_commit(self, sha):
        repo = self.as_github_repo()
        return repo.commit(sha)

    def github_release(self, tag):
        repo = self.as_github_repo()
        try:
            return repo.release_from_tag(tag)
        except github3.exceptions.NotFoundError:
            return None

    def github_upload_asset_requests(self, release, path, name, mime,
                                     max_retries=None, retry_backoff=None):
        if max_retries is None:
            max_retries = int(os.environ.get('CROSSBOW_MAX_RETRIES', 8))
        if retry_backoff is None:
            retry_backoff = int(os.environ.get('CROSSBOW_RETRY_BACKOFF', 5))

        for i in range(max_retries):
            try:
                with open(path, 'rb') as fp:
                    result = release.upload_asset(name=name, asset=fp,
                                                  content_type=mime)
            except github3.exceptions.ResponseError as e:
                logger.error('Attempt {} has failed with message: {}.'
                             .format(i + 1, str(e)))
                logger.error('Error message {}'.format(e.msg))
                logger.error('List of errors provided by Github:')
                for err in e.errors:
                    logger.error(' - {}'.format(err))

                if e.code == 422:
                    # 422 Validation Failed, probably raised because
                    # ReleaseAsset already exists, so try to remove it before
                    # reattempting the asset upload
                    for asset in release.assets():
                        if asset.name == name:
                            logger.info('Release asset {} already exists, '
                                        'removing it...'.format(name))
                            asset.delete()
                            logger.info('Asset {} removed.'.format(name))
                            break
            except github3.exceptions.ConnectionError as e:
                logger.error('Attempt {} has failed with message: {}.'
                             .format(i + 1, str(e)))
            else:
                logger.info('Attempt {} has finished.'.format(i + 1))
                return result

            time.sleep(retry_backoff)

        raise RuntimeError('Github asset uploading has failed!')

    def github_upload_asset_curl(self, release, path, name, mime):
        upload_url, _ = release.upload_url.split('{?')
        upload_url += '?name={}'.format(name)

        command = [
            'curl',
            '--fail',
            '-H', "Authorization: token {}".format(self.github_token),
            '-H', "Content-Type: {}".format(mime),
            '--data-binary', '@{}'.format(path),
            upload_url
        ]
        return subprocess.run(command, shell=False, check=True)

    def github_overwrite_release_assets(self, tag_name, target_commitish,
                                        patterns, method='requests'):
        # Since github has changed something the asset uploading via requests
        # got instable, so prefer the cURL alternative.
        # Potential cause:
        #    sigmavirus24/github3.py/issues/779#issuecomment-379470626
        repo = self.as_github_repo()
        if not tag_name:
            raise CrossbowError('Empty tag name')
        if not target_commitish:
            raise CrossbowError('Empty target commit for the release tag')

        # remove the whole release if it already exists
        try:
            release = repo.release_from_tag(tag_name)
        except github3.exceptions.NotFoundError:
            pass
        else:
            release.delete()

        release = repo.create_release(tag_name, target_commitish)
        for pattern in patterns:
            for path in glob.glob(pattern, recursive=True):
                name = os.path.basename(path)
                size = os.path.getsize(path)
                mime = mimetypes.guess_type(name)[0] or 'application/zip'

                logger.info(
                    'Uploading asset `{}` with mimetype {} and size {}...'
                    .format(name, mime, size)
                )

                if method == 'requests':
                    self.github_upload_asset_requests(release, path, name=name,
                                                      mime=mime)
                elif method == 'curl':
                    self.github_upload_asset_curl(release, path, name=name,
                                                  mime=mime)
                else:
                    raise CrossbowError(
                        'Unsupported upload method {}'.format(method)
                    )

    def github_pr(self, title, head=None, base=None, body=None,
                  github_token=None, create=False):
        if create:
            # Default value for base is the default_branch_name
            base = self.default_branch_name if base is None else base
        github_token = github_token or self.github_token
        repo = self.as_github_repo(github_token=github_token)
        if create:
            return repo.create_pull(title=title, base=base, head=head,
                                    body=body)
        else:
            # Retrieve open PR for base and head.
            # There should be a single open one with that title.
            for pull in repo.pull_requests(state="open", head=head,
                                           base=base):
                if title in pull.title:
                    return pull
            raise CrossbowError(
                f"Pull request with Title: {title!r} not found "
                f"in repository {repo.full_name!r}"
            )


class Queue(Repo):

    def _latest_prefix_id(self, prefix):
        pattern = re.compile(r'[\w\/-]*{}-(\d+)'.format(prefix))
        matches = list(filter(None, map(pattern.match, self.repo.branches)))
        if matches:
            latest = max(int(m.group(1)) for m in matches)
        else:
            latest = -1
        return latest

    def _prefix_contains_date(self, prefix):
        prefix_date_pattern = re.compile(r'[\w\/-]*-(\d+)-(\d+)-(\d+)')
        match_prefix = prefix_date_pattern.match(prefix)
        if match_prefix:
            return match_prefix.group(0)[-10:]

    def _latest_prefix_date(self, prefix):
        pattern = re.compile(r'[\w\/-]*{}-(\d+)-(\d+)-(\d+)'.format(prefix))
        matches = list(filter(None, map(pattern.match, self.repo.branches)))
        if matches:
            latest = sorted([m.group(0) for m in matches])[-1]
            # slice the trailing date part (YYYY-MM-DD)
            latest = latest[-10:]
        else:
            latest = -1
        return latest

    def _next_job_id(self, prefix):
        """Auto increments the branch's identifier based on the prefix"""
        latest_id = self._latest_prefix_id(prefix)
        return '{}-{}'.format(prefix, latest_id + 1)

    def _new_hex_id(self, prefix):
        """Append a new id to branch's identifier based on the prefix"""
        hex_id = uuid.uuid4().hex[:10]
        return '{}-{}'.format(prefix, hex_id)

    def latest_for_prefix(self, prefix):
        prefix_date = self._prefix_contains_date(prefix)
        if prefix.startswith("nightly") and not prefix_date:
            latest_id = self._latest_prefix_date(prefix)
            if not latest_id:
                raise RuntimeError(
                    f"No job has been submitted with prefix '{prefix}'' yet"
                )
            latest_id += "-0"
        else:
            latest_id = self._latest_prefix_id(prefix)
            if latest_id < 0:
                raise RuntimeError(
                    f"No job has been submitted with prefix '{prefix}' yet"
                )
        job_name = '{}-{}'.format(prefix, latest_id)
        return self.get(job_name)

    def date_of(self, job):
        # it'd be better to bound to the queue repository on deserialization
        # and reorganize these methods to Job
        branch_name = 'origin/{}'.format(job.branch)
        branch = self.repo.branches[branch_name]
        commit = self.repo[branch.target]
        return date.fromtimestamp(commit.commit_time)

    def jobs(self, pattern):
        """Return jobs sorted by its identifier in reverse order"""
        job_names = []
        for name in self.repo.branches.remote:
            origin, name = name.split('/', 1)
            result = re.match(pattern, name)
            if result:
                job_names.append(name)

        for name in sorted(job_names, reverse=True):
            yield self.get(name)

    def get(self, job_name):
        branch_name = 'origin/{}'.format(job_name)
        branch = self.repo.branches[branch_name]
        try:
            content = self.file_contents(branch.target, 'job.yml')
        except KeyError:
            raise CrossbowError(
                'No job is found with name: {}'.format(job_name)
            )

        buffer = StringIO(content.decode('utf-8'))
        job = yaml.load(buffer)
        job.queue = self
        return job

    def put(self, job, prefix='build', increment_job_id=True):
        if not isinstance(job, Job):
            raise CrossbowError('`job` must be an instance of Job')
        if job.branch is not None:
            raise CrossbowError('`job.branch` is automatically generated, '
                                'thus it must be blank')

        job.queue = self
        if increment_job_id:
            # auto increment and set next job id, e.g. build-85
            job.branch = self._next_job_id(prefix)
        else:
            # set new branch to something unique, e.g. build-41d017af40
            job.branch = self._new_hex_id(prefix)

        # create tasks' branches
        for task_name, task in job.tasks.items():
            # adding CI's name to the end of the branch in order to use skip
            # patterns on travis and circleci
            task.branch = '{}-{}-{}'.format(job.branch, task.ci, task_name)
            params = {
                **job.params,
                "arrow": job.target,
                "job": job,
                "queue_remote_url": self.remote_url
            }
            files = task.render_files(job.template_searchpath, params=params)
            branch = self.create_branch(task.branch, files=files)
            self.create_tag(task.tag, branch.target)
            task.commit = str(branch.target)

        # create job's branch with its description
        return self.create_branch(job.branch, files=job.render_files())


def get_version(root, **kwargs):
    """
    Parse function for setuptools_scm that ignores tags for non-C++
    subprojects, e.g. apache-arrow-js-XXX tags.
    """
    from setuptools_scm.git import parse as parse_git_version

    # query the calculated version based on the git tags
    kwargs['describe_command'] = (
        'git describe --dirty --tags --long --match "apache-arrow-[0-9]*.*"'
    )
    version = parse_git_version(root, **kwargs)
    tag = str(version.tag)

    # We may get a development tag for the next version, such as "5.0.0.dev0",
    # or the tag of an already released version, such as "4.0.0".
    # In the latter case, we need to increment the version so that the computed
    # version comes after any patch release (the next feature version after
    # 4.0.0 is 5.0.0).
    pattern = r"^(\d+)\.(\d+)\.(\d+)"
    match = re.match(pattern, tag)
    major, minor, patch = map(int, match.groups())
    if 'dev' not in tag:
        major += 1

    return "{}.{}.{}.dev{}".format(major, minor, patch, version.distance or 0)


class Serializable:

    @classmethod
    def to_yaml(cls, representer, data):
        tag = '!{}'.format(cls.__name__)
        dct = {k: v for k, v in data.__dict__.items() if not k.startswith('_')}
        return representer.represent_mapping(tag, dct)


class Target(Serializable):
    """
    Describes target repository and revision the builds run against

    This serializable data container holding information about arrow's
    git remote, branch, sha and version number as well as some metadata
    (currently only an email address where the notification should be sent).
    """

    def __init__(self, head, branch, remote, version, r_version, email=None):
        self.head = head
        self.email = email
        self.branch = branch
        self.remote = remote
        self.github_repo = "/".join(_parse_github_user_repo(remote))
        self.version = version
        self.r_version = r_version
        self.no_rc_version = re.sub(r'-rc\d+\Z', '', version)
        self.no_rc_r_version = re.sub(r'-rc\d+\Z', '', r_version)
        # Semantic Versioning 1.0.0: https://semver.org/spec/v1.0.0.html
        #
        # > A pre-release version number MAY be denoted by appending an
        # > arbitrary string immediately following the patch version and a
        # > dash. The string MUST be comprised of only alphanumerics plus
        # > dash [0-9A-Za-z-].
        #
        # Example:
        #
        #   '0.16.1.dev10' ->
        #   '0.16.1-dev10'
        self.no_rc_semver_version = \
            re.sub(r'\.(dev\d+)\Z', r'-\1', self.no_rc_version)
        # Substitute dev version for SNAPSHOT
        #
        # Example:
        #
        # '10.0.0.dev235' ->
        # '10.0.0-SNAPSHOT'
        self.no_rc_snapshot_version = re.sub(
            r'\.(dev\d+)$', '-SNAPSHOT', self.no_rc_version)

    @classmethod
    def from_repo(cls, repo, head=None, branch=None, remote=None, version=None,
                  email=None):
        """Initialize from a repository

        Optionally override detected remote, branch, head, and/or version.
        """
        assert isinstance(repo, Repo)

        if head is None:
            head = str(repo.head.target)
        if branch is None:
            branch = repo.branch.branch_name
        if remote is None:
            remote = repo.remote_url
        if version is None:
            version = get_version(repo.path)
        if email is None:
            email = repo.user_email

        version_dev_match = re.match(r".*\.dev(\d+)$", version)
        if version_dev_match:
            with open(f"{repo.path}/r/DESCRIPTION") as description_file:
                description = description_file.read()
                r_version_pattern = re.compile(r"^Version:\s*(.*)$",
                                               re.MULTILINE)
                r_version = re.findall(r_version_pattern, description)[0]
            if r_version:
                version_dev = int(version_dev_match[1])
                # "1_0000_00_00 +" is for generating a greater version
                # than YYYYMMDD. For example, 1_0000_00_01
                # (version_dev == 1 case) is greater than 2022_10_16.
                #
                # Why do we need a greater version than YYYYMMDD? It's
                # for keeping backward compatibility. We used
                # MAJOR.MINOR.PATCH.YYYYMMDD as our nightly package
                # version. (See also ARROW-16403). If we use "9000 +
                # version_dev" here, a developer that used
                # 9.0.0.20221016 can't upgrade to the later nightly
                # package unless we release 10.0.0. Because 9.0.0.9234
                # or something is less than 9.0.0.20221016.
                r_version_dev = 1_0000_00_00 + version_dev
                # version: 10.0.0.dev234
                # r_version: 9.0.0.9000
                # -> 9.0.0.100000234
                r_version = re.sub(r"\.9000\Z", f".{r_version_dev}", r_version)
            else:
                r_version = version
        else:
            r_version = version

        return cls(head=head, email=email, branch=branch, remote=remote,
                   version=version, r_version=r_version)

    def is_default_branch(self):
        return self.branch == 'main'


class Task(Serializable):
    """
    Describes a build task and metadata required to render CI templates

    A task is represented as a single git commit and branch containing jinja2
    rendered files (currently appveyor.yml or .travis.yml configurations).

    A task can't be directly submitted to a queue, must belong to a job.
    Each task's unique identifier is its branch name, which is generated after
    submitting the job to a queue.
    """

    def __init__(self, name, ci, template, artifacts=None, params=None):
        assert ci in {
            'circle',
            'travis',
            'appveyor',
            'azure',
            'github',
            'drone',
        }
        self.name = name
        self.ci = ci
        self.template = template
        self.artifacts = artifacts or []
        self.params = params or {}
        self.branch = None  # filled after adding to a queue
        self.commit = None  # filled after adding to a queue
        self._queue = None  # set by the queue object after put or get
        self._status = None  # status cache
        self._assets = None  # assets cache

    def render_files(self, searchpath, params=None):
        params = {**self.params, **(params or {}), "task": self}
        try:
            rendered = _render_jinja_template(searchpath, self.template,
                                              params=params)
        except jinja2.TemplateError as e:
            raise RuntimeError(
                'Failed to render template `{}` with {}: {}'.format(
                    self.template, e.__class__.__name__, str(e)
                )
            )

        tree = {**_default_tree, self.filename: rendered}
        return _unflatten_tree(tree)

    @property
    def tag(self):
        return self.branch

    @property
    def filename(self):
        config_files = {
            'circle': '.circleci/config.yml',
            'travis': '.travis.yml',
            'appveyor': 'appveyor.yml',
            'azure': 'azure-pipelines.yml',
            'github': '.github/workflows/crossbow.yml',
            'drone': '.drone.yml',
        }
        return config_files[self.ci]

    def status(self, force_query=False):
        _status = getattr(self, '_status', None)
        if force_query or _status is None:
            github_commit = self._queue.github_commit(self.commit)
            self._status = TaskStatus(github_commit)
        return self._status

    def assets(self, force_query=False, validate_patterns=True):
        _assets = getattr(self, '_assets', None)
        if force_query or _assets is None:
            github_release = self._queue.github_release(self.tag)
            self._assets = TaskAssets(github_release,
                                      artifact_patterns=self.artifacts,
                                      validate_patterns=validate_patterns)
        return self._assets


class TaskStatus:
    """
    Combine the results from status and checks API to a single state.

    Azure pipelines uses checks API which doesn't provide a combined
    interface like status API does, so we need to manually combine
    both the commit statuses and the commit checks coming from
    different API endpoint

    Status.state: error, failure, pending or success, default pending
    CheckRun.status: queued, in_progress or completed, default: queued
    CheckRun.conclusion: success, failure, neutral, cancelled, timed_out
                            or action_required, only set if
                            CheckRun.status == 'completed'

    1. Convert CheckRun's status and conclusion to one of Status.state
    2. Merge the states based on the following rules:
        - failure if any of the contexts report as error or failure
        - pending if there are no statuses or a context is pending
        - success if the latest status for all contexts is success
        error otherwise.

    Parameters
    ----------
    commit : github3.Commit
        Commit to query the combined status for.

    Returns
    -------
    TaskStatus(
        combined_state='error|failure|pending|success',
        github_status='original github status object',
        github_check_runs='github checks associated with the commit',
        total_count='number of statuses and checks'
    )
    """

    def __init__(self, commit):
        status = commit.status()
        check_runs = list(commit.check_runs())
        states = [s.state for s in status.statuses]

        for check in check_runs:
            if check.status == 'completed':
                if check.conclusion in {'success', 'failure'}:
                    states.append(check.conclusion)
                elif check.conclusion in {'cancelled', 'timed_out',
                                          'action_required'}:
                    states.append('error')
                # omit `neutral` conclusion
            else:
                states.append('pending')

        # it could be more effective, but the following is more descriptive
        combined_state = 'error'
        if len(states):
            if any(state in {'error', 'failure'} for state in states):
                combined_state = 'failure'
            elif any(state == 'pending' for state in states):
                combined_state = 'pending'
            elif all(state == 'success' for state in states):
                combined_state = 'success'

        # show link to the actual build, some of the CI providers implement
        # the statuses API others implement the checks API, so display both
        build_links = [s.target_url for s in status.statuses]
        build_links += [c.html_url for c in check_runs]

        self.combined_state = combined_state
        self.github_status = status
        self.github_check_runs = check_runs
        self.total_count = len(states)
        self.build_links = build_links


class TaskAssets(dict):

    def __init__(self, github_release, artifact_patterns,
                 validate_patterns=True):
        # HACK(kszucs): don't expect uploaded assets of no atifacts were
        # defiened for the tasks in order to spare a bit of github rate limit
        if not artifact_patterns:
            return

        if github_release is None:
            github_assets = {}  # no assets have been uploaded for the task
        else:
            github_assets = {a.name: a for a in github_release.assets()}

        if not validate_patterns:
            # shortcut to avoid pattern validation and just set all artifacts
            return self.update(github_assets)

        for pattern in artifact_patterns:
            # artifact can be a regex pattern
            compiled = re.compile(f"^{pattern}$")
            matches = list(
                filter(None, map(compiled.match, github_assets.keys()))
            )
            num_matches = len(matches)

            # validate artifact pattern matches single asset
            if num_matches == 0:
                self[pattern] = None
            elif num_matches == 1:
                self[pattern] = github_assets[matches[0].group(0)]
            else:
                raise CrossbowError(
                    'Only a single asset should match pattern `{}`, there are '
                    'multiple ones: {}'.format(pattern, ', '.join(matches))
                )

    def missing_patterns(self):
        return [pattern for pattern, asset in self.items() if asset is None]

    def uploaded_assets(self):
        return [asset for asset in self.values() if asset is not None]


class Job(Serializable):
    """Describes multiple tasks against a single target repository"""

    def __init__(self, target, tasks, params=None, template_searchpath=None):
        if not tasks:
            raise ValueError('no tasks were provided for the job')
        if not all(isinstance(task, Task) for task in tasks.values()):
            raise ValueError('each `tasks` mus be an instance of Task')
        if not isinstance(target, Target):
            raise ValueError('`target` must be an instance of Target')
        if not isinstance(params, dict):
            raise ValueError('`params` must be an instance of dict')

        self.target = target
        self.tasks = tasks
        self.params = params or {}  # additional parameters for the tasks
        self.branch = None  # filled after adding to a queue
        self._queue = None  # set by the queue object after put or get
        if template_searchpath is None:
            self._template_searchpath = ArrowSources.find().path
        else:
            self._template_searchpath = template_searchpath

    def render_files(self):
        with StringIO() as buf:
            yaml.dump(self, buf)
            content = buf.getvalue()
        tree = {**_default_tree, "job.yml": content}
        return _unflatten_tree(tree)

    def render_tasks(self, params=None):
        result = {}
        params = {
            **self.params,
            "arrow": self.target,
            "job": self,
            **(params or {})
        }
        for task_name, task in self.tasks.items():
            files = task.render_files(self._template_searchpath, params)
            result[task_name] = files
        return result

    @property
    def template_searchpath(self):
        return self._template_searchpath

    @property
    def queue(self):
        assert isinstance(self._queue, Queue)
        return self._queue

    @queue.setter
    def queue(self, queue):
        assert isinstance(queue, Queue)
        self._queue = queue
        for task in self.tasks.values():
            task._queue = queue

    @property
    def email(self):
        return os.environ.get('CROSSBOW_EMAIL', self.target.email)

    @property
    def date(self):
        return self.queue.date_of(self)

    def show(self, stream=None):
        return yaml.dump(self, stream=stream)

    @classmethod
    def from_config(cls, config, target, tasks=None, groups=None, params=None):
        """
        Intantiate a job from based on a config.

        Parameters
        ----------
        config : dict
            Deserialized content of tasks.yml
        target : Target
            Describes target repository and revision the builds run against.
        tasks : Optional[List[str]], default None
            List of glob patterns for matching task names.
        groups : Optional[List[str]], default None
            List of exact group names matching predefined task sets in the
            config.
        params : Optional[Dict[str, str]], default None
            Additional rendering parameters for the task templates.

        Returns
        -------
        Job

        Raises
        ------
        Exception:
            If invalid groups or tasks has been passed.
        """
        task_definitions = config.select(tasks, groups=groups)

        # instantiate the tasks
        tasks = {}
        versions = {
            'version': target.version,
            'no_rc_version': target.no_rc_version,
            'no_rc_semver_version': target.no_rc_semver_version,
            'no_rc_snapshot_version': target.no_rc_snapshot_version,
            'r_version': target.r_version,
            'no_rc_r_version': target.no_rc_r_version,
        }
        for task_name, task in task_definitions.items():
            task = task.copy()
            artifacts = task.pop('artifacts', None) or []  # because of yaml
            artifacts = [fn.format(**versions) for fn in artifacts]
            tasks[task_name] = Task(task_name, artifacts=artifacts, **task)
        return cls(target=target, tasks=tasks, params=params,
                   template_searchpath=config.template_searchpath)

    def is_finished(self):
        for task in self.tasks.values():
            status = task.status(force_query=True)
            if status.combined_state == 'pending':
                return False
        return True

    def wait_until_finished(self, poll_max_minutes=120,
                            poll_interval_minutes=10):
        started_at = time.time()
        while True:
            if self.is_finished():
                break

            waited_for_minutes = (time.time() - started_at) / 60
            if waited_for_minutes > poll_max_minutes:
                msg = ('Exceeded the maximum amount of time waiting for job '
                       'to finish, waited for {} minutes.')
                raise RuntimeError(msg.format(waited_for_minutes))

            logger.info('Waiting {} minutes and then checking again'
                        .format(poll_interval_minutes))
            time.sleep(poll_interval_minutes * 60)


class Config(dict):

    def __init__(self, tasks, template_searchpath):
        super().__init__(tasks)
        self.template_searchpath = template_searchpath

    @classmethod
    def load_yaml(cls, path):
        path = Path(path)
        searchpath = path.parent
        rendered = _render_jinja_template(searchpath, template=path.name,
                                          params={})
        config = yaml.load(rendered)
        return cls(config, template_searchpath=searchpath)

    def show(self, stream=None):
        return yaml.dump(dict(self), stream=stream)

    def select(self, tasks=None, groups=None):
        config_groups = dict(self['groups'])
        config_tasks = dict(self['tasks'])
        valid_groups = set(config_groups.keys())
        valid_tasks = set(config_tasks.keys())
        group_allowlist = list(groups or [])
        task_allowlist = list(tasks or [])

        # validate that the passed groups are defined in the config
        requested_groups = set(group_allowlist)
        invalid_groups = requested_groups - valid_groups
        if invalid_groups:
            msg = 'Invalid group(s) {!r}. Must be one of {!r}'.format(
                invalid_groups, valid_groups
            )
            raise CrossbowError(msg)

        # treat the task names as glob patterns to select tasks more easily
        requested_tasks = set()
        for pattern in task_allowlist:
            matches = fnmatch.filter(valid_tasks, pattern)
            if len(matches):
                requested_tasks.update(matches)
            else:
                raise CrossbowError(
                    "Unable to match any tasks for `{}`".format(pattern)
                )

        requested_group_tasks = set()
        for group in group_allowlist:
            # separate the patterns from the blocklist patterns
            task_patterns = list(config_groups[group])
            task_blocklist_patterns = [
                x.strip("~") for x in task_patterns if x.startswith("~")]
            task_patterns = [x for x in task_patterns if not x.startswith("~")]

            # treat the task names as glob patterns to select tasks more easily
            for pattern in task_patterns:
                matches = fnmatch.filter(valid_tasks, pattern)
                if len(matches):
                    requested_group_tasks.update(matches)
                else:
                    raise CrossbowError(
                        "Unable to match any tasks for `{}`".format(pattern)
                    )

            # remove any tasks that are negated with ~task-name
            for block_pattern in task_blocklist_patterns:
                matches = fnmatch.filter(valid_tasks, block_pattern)
                if len(matches):
                    requested_group_tasks = requested_group_tasks.difference(
                        matches)
                else:
                    raise CrossbowError(
                        "Unable to match any tasks for `{}`".format(pattern)
                    )

        requested_tasks = requested_tasks.union(requested_group_tasks)

        # validate that the passed and matched tasks are defined in the config
        invalid_tasks = requested_tasks - valid_tasks
        if invalid_tasks:
            msg = 'Invalid task(s) {!r}. Must be one of {!r}'.format(
                invalid_tasks, valid_tasks
            )
            raise CrossbowError(msg)

        return {
            task_name: config_tasks[task_name] for task_name in requested_tasks
        }

    def validate(self):
        # validate that the task groups are properly refering to the tasks
        for group_name, group in self['groups'].items():
            for pattern in group:
                # remove the negation character for blocklisted tasks
                pattern = pattern.strip("~")
                tasks = self.select(tasks=[pattern])
                if not tasks:
                    raise CrossbowError(
                        "The pattern `{}` defined for task group `{}` is not "
                        "matching any of the tasks defined in the "
                        "configuration file.".format(pattern, group_name)
                    )

        # validate that the tasks are constructible
        for task_name, task in self['tasks'].items():
            try:
                Task(task_name, **task)
            except Exception as e:
                raise CrossbowError(
                    'Unable to construct a task object from the '
                    'definition  of task `{}`. The original error message '
                    'is: `{}`'.format(task_name, str(e))
                )

        # Get the default branch name from the repository
        arrow_source_dir = ArrowSources.find()
        repo = Repo(arrow_source_dir.path)

        # validate that the defined tasks are renderable, in order to to that
        # define the required object with dummy data
        target = Target(
            head='e279a7e06e61c14868ca7d71dea795420aea6539',
            branch=repo.default_branch_name,
            remote='https://github.com/apache/arrow',
            version='1.0.0dev123',
            r_version='0.13.0.100000123',
            email='dummy@example.ltd'
        )
        job = Job.from_config(config=self,
                              target=target,
                              tasks=self['tasks'],
                              groups=self['groups'],
                              params={})

        for task_name, task in self['tasks'].items():
            task = Task(task_name, **task)
            files = task.render_files(
                self.template_searchpath,
                params=dict(
                    arrow=target,
                    job=job,
                    queue_remote_url='https://github.com/org/crossbow'
                )
            )
            if not files:
                raise CrossbowError('No files have been rendered for task `{}`'
                                    .format(task_name))


# configure yaml serializer
yaml = YAML()
yaml.register_class(Job)
yaml.register_class(Task)
yaml.register_class(Target)
yaml.register_class(Queue)
yaml.register_class(TaskStatus)
