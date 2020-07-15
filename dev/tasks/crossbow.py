#!/usr/bin/env python

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
from io import StringIO
from pathlib import Path
from textwrap import dedent
from datetime import date
from functools import partial

import click
import toolz

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
else:
    PygitRemoteCallbacks = pygit2.RemoteCallbacks


# initialize logging
logging.basicConfig()
logging.getLogger().setLevel(logging.ERROR)

# enable verbose logging for requests
# http_client.HTTPConnection.debuglevel = 1
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.ERROR)
requests_log.propagate = True


CWD = Path(__file__).parent.absolute()


def unflatten(mapping):
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


def unflatten_tree(files):
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
    files = toolz.keymap(lambda path: tuple(path.split('/')), files)
    return unflatten(files)


# configurations for setting up branch skipping
# - appveyor has a feature to skip builds without an appveyor.yml
# - travis reads from the master branch and applies the rules
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
            raise ValueError(msg)

        if allowed_types & pygit2.credentials.GIT_CREDTYPE_USERPASS_PLAINTEXT:
            return pygit2.UserPass(self.token, 'x-oauth-basic')
        else:
            return None


def _git_ssh_to_https(url):
    return url.replace('git@github.com:', 'https://github.com/')


class Repo:
    """Base class for interaction with local git repositories

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
        tpl = dedent('''
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
            raise ValueError("Change SSH origin URL to HTTPS to use "
                             "Crossbow: {}".format(remote.url))
        return remote

    def fetch(self):
        refspec = '+refs/heads/*:refs/remotes/origin/*'
        self.origin.fetch([refspec])

    def push(self, refs=None, github_token=None):
        github_token = github_token or self.github_token
        if github_token is None:
            raise click.ClickException(
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
            return None  # detached

    @property
    def remote(self):
        """Currently checked out branch's remote counterpart"""
        try:
            return self.repo.remotes[self.branch.upstream.remote_name]
        except (AttributeError, KeyError):
            return None  # cannot detect

    @property
    def remote_url(self):
        """Currently checked out branch's remote counterpart URL

        If an SSH github url is set, it will be replaced by the https
        equivalent usable with GitHub OAuth token.
        """
        try:
            return self._remote_url or _git_ssh_to_https(self.remote.url)
        except AttributeError:
            return None

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
        parents = parents or []
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

    def _parse_github_user_repo(self):
        m = re.match(r'.*\/([^\/]+)\/([^\/\.]+)(\.git)?$', self.remote_url)
        if m is None:
            raise ValueError("Unable to parse the github owner and repository "
                             "from the repository's remote url '{}'"
                             .format(self.remote_url))
        user, repo = m.group(1), m.group(2)
        return user, repo

    def as_github_repo(self, github_token=None):
        """Converts it to a repository object which wraps the GitHub API"""
        if self._github_repo is None:
            if not _have_github3:
                raise ImportError('Must install github3.py')
            github_token = github_token or self.github_token
            username, reponame = self._parse_github_user_repo()
            session = github3.session.GitHubSession(
                default_connect_timeout=10,
                default_read_timeout=30
            )
            github = github3.GitHub(session=session)
            github.login(token=github_token)
            self._github_repo = github.repository(username, reponame)
        return self._github_repo

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
                click.echo('Attempt {} has failed with message: {}.'
                           .format(i + 1, str(e)))
                click.echo('Error message {}'.format(e.msg))
                click.echo('List of errors provided by Github:')
                for err in e.errors:
                    click.echo(' - {}'.format(err))

                if e.code == 422:
                    # 422 Validation Failed, probably raised because
                    # ReleaseAsset already exists, so try to remove it before
                    # reattempting the asset upload
                    for asset in release.assets():
                        if asset.name == name:
                            click.echo('Release asset {} already exists, '
                                       'removing it...'.format(name))
                            asset.delete()
                            click.echo('Asset {} removed.'.format(name))
                            break
            except github3.exceptions.ConnectionError as e:
                click.echo('Attempt {} has failed with message: {}.'
                           .format(i + 1, str(e)))
            else:
                click.echo('Attempt {} has finished.'.format(i + 1))
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
            raise ValueError('Empty tag name')
        if not target_commitish:
            raise ValueError('Empty target commit for the release tag')

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

                click.echo(
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
                    raise ValueError(
                        'Unsupported upload method {}'.format(method)
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

    def _next_job_id(self, prefix):
        """Auto increments the branch's identifier based on the prefix"""
        latest_id = self._latest_prefix_id(prefix)
        return '{}-{}'.format(prefix, latest_id + 1)

    def latest_for_prefix(self, prefix):
        latest_id = self._latest_prefix_id(prefix)
        if latest_id < 0:
            raise RuntimeError(
                'No job has been submitted with prefix {} yet'.format(prefix)
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
            raise ValueError('No job is found with name: {}'.format(job_name))

        buffer = StringIO(content.decode('utf-8'))
        job = yaml.load(buffer)
        job.queue = self
        return job

    def put(self, job, prefix='build'):
        if not isinstance(job, Job):
            raise ValueError('`job` must be an instance of Job')
        if job.branch is not None:
            raise ValueError('`job.branch` is automatically generated, thus '
                             'it must be blank')

        if job.target.remote is None:
            raise RuntimeError(
                'Cannot determine git remote for the Arrow repository to '
                'clone or push to, try to push the branch first to have a '
                'remote tracking counterpart.'
            )
        if job.target.branch is None:
            raise RuntimeError(
                'Cannot determine the current branch of the Arrow repository '
                'to clone or push to, perhaps it is in detached HEAD state. '
                'Please checkout a branch.'
            )

        # auto increment and set next job id, e.g. build-85
        job._queue = self
        job.branch = self._next_job_id(prefix)

        # create tasks' branches
        for task_name, task in job.tasks.items():
            # adding CI's name to the end of the branch in order to use skip
            # patterns on travis and circleci
            task.branch = '{}-{}-{}'.format(job.branch, task.ci, task_name)
            files = task.render_files(arrow=job.target,
                                      queue_remote_url=self.remote_url)
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
        'git describe --dirty --tags --long --match "apache-arrow-[0-9].*"'
    )
    version = parse_git_version(root, **kwargs)

    # increment the minor version, because there can be patch releases created
    # from maintenance branches where the tags are unreachable from the
    # master's HEAD, so the git command above generates 0.17.0.dev300 even if
    # arrow has a never 0.17.1 patch release
    pattern = r"^(\d+)\.(\d+)\.(\d+)$"
    match = re.match(pattern, str(version.tag))
    major, minor, patch = map(int, match.groups())

    # the bumped version number after 0.17.x will be 0.18.0.dev300
    return "{}.{}.{}.dev{}".format(major, minor + 1, patch, version.distance)


class Serializable:

    @classmethod
    def to_yaml(cls, representer, data):
        tag = '!{}'.format(cls.__name__)
        dct = {k: v for k, v in data.__dict__.items() if not k.startswith('_')}
        return representer.represent_mapping(tag, dct)


class Target(Serializable):
    """Describes target repository and revision the builds run against

    This serializable data container holding information about arrow's
    git remote, branch, sha and version number as well as some metadata
    (currently only an email address where the notification should be sent).
    """

    def __init__(self, head, branch, remote, version, email=None):
        self.head = head
        self.email = email
        self.branch = branch
        self.remote = remote
        self.version = version
        self.no_rc_version = re.sub(r'-rc\d+\Z', '', version)
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

        return cls(head=head, email=email, branch=branch, remote=remote,
                   version=version)


class Task(Serializable):
    """Describes a build task and metadata required to render CI templates

    A task is represented as a single git commit and branch containing jinja2
    rendered files (currently appveyor.yml or .travis.yml configurations).

    A task can't be directly submitted to a queue, must belong to a job.
    Each task's unique identifier is its branch name, which is generated after
    submitting the job to a queue.
    """

    def __init__(self, ci, template, artifacts=None, params=None):
        assert ci in {'circle', 'travis', 'appveyor', 'azure', 'github'}
        self.ci = ci
        self.template = template
        self.artifacts = artifacts or []
        self.params = params or {}
        self.branch = None  # filled after adding to a queue
        self.commit = None  # filled after adding to a queue
        self._queue = None  # set by the queue object after put or get
        self._status = None  # status cache
        self._assets = None  # assets cache

    def render_files(self, **extra_params):
        from jinja2 import Template, StrictUndefined
        from jinja2.exceptions import TemplateError

        path = CWD / self.template
        params = toolz.merge(self.params, extra_params)
        template = Template(path.read_text(), undefined=StrictUndefined)
        try:
            rendered = template.render(task=self, **params)
        except TemplateError as e:
            raise RuntimeError(
                'Failed to render template `{}` with {}: {}'.format(
                    path, e.__class__.__name__, str(e)
                )
            )

        tree = toolz.merge(_default_tree, {self.filename: rendered})
        return unflatten_tree(tree)

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
        }
        return config_files[self.ci]

    def status(self, force_query=False):
        _status = getattr(self, '_status', None)
        if force_query or _status is None:
            github_commit = self._queue.github_commit(self.commit)
            self._status = TaskStatus(github_commit)
        return self._status

    def assets(self, force_query=False):
        _assets = getattr(self, '_assets', None)
        if force_query or _assets is None:
            github_release = self._queue.github_release(self.tag)
            self._assets = TaskAssets(github_release,
                                      artifact_patterns=self.artifacts)
        return self._assets


class TaskStatus:
    """Combine the results from status and checks API to a single state.

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
        check_runs = commit.check_runs()

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
        if any(state in {'error', 'failure'} for state in states):
            combined_state = 'failure'
        elif any(state == 'pending' for state in states):
            combined_state = 'pending'
        elif all(state == 'success' for state in states):
            combined_state = 'success'
        else:
            combined_state = 'error'

        self.combined_state = combined_state
        self.github_status = status
        self.github_check_runs = check_runs
        self.total_count = len(states)


class TaskAssets(dict):

    def __init__(self, github_release, artifact_patterns):
        # HACK(kszucs): don't expect uploaded assets of no atifacts were
        # defiened for the tasks in order to spare a bit of github rate limit
        if not artifact_patterns:
            return

        if github_release is None:
            github_assets = {}  # no assets have been uploaded for the task
        else:
            github_assets = {a.name: a for a in github_release.assets()}

        for pattern in artifact_patterns:
            # artifact can be a regex pattern
            compiled = re.compile(pattern)
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
                raise ValueError(
                    'Only a single asset should match pattern `{}`, there are '
                    'multiple ones: {}'.format(pattern, ', '.join(matches))
                )

    def missing_patterns(self):
        return [pattern for pattern, asset in self.items() if asset is None]

    def uploaded_assets(self):
        return [asset for asset in self.values() if asset is not None]


class Job(Serializable):
    """Describes multiple tasks against a single target repository"""

    def __init__(self, target, tasks):
        if not tasks:
            raise ValueError('no tasks were provided for the job')
        if not all(isinstance(task, Task) for task in tasks.values()):
            raise ValueError('each `tasks` mus be an instance of Task')
        if not isinstance(target, Target):
            raise ValueError('`target` must be an instance of Target')
        self.target = target
        self.tasks = tasks
        self.branch = None  # filled after adding to a queue
        self._queue = None  # set by the queue object after put or get

    def render_files(self):
        with StringIO() as buf:
            yaml.dump(self, buf)
            content = buf.getvalue()
        tree = toolz.merge(_default_tree, {'job.yml': content})
        return unflatten_tree(tree)

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

    @classmethod
    def from_config(cls, config, target, tasks=None, groups=None):
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
        groups : tasks : Optional[List[str]], default None
            List of exact group names matching predefined task sets in the
            config.

        Returns
        -------
        Job

        Raises
        ------
        click.ClickException
            If invalid groups or tasks has been passed.
        """
        task_definitions = config.select(tasks, groups=groups)

        # instantiate the tasks
        tasks = {}
        versions = {'version': target.version,
                    'no_rc_version': target.no_rc_version,
                    'no_rc_semver_version': target.no_rc_semver_version}
        for task_name, task in task_definitions.items():
            artifacts = task.pop('artifacts', None) or []  # because of yaml
            artifacts = [fn.format(**versions) for fn in artifacts]
            tasks[task_name] = Task(artifacts=artifacts, **task)

        return cls(target=target, tasks=tasks)

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

            # TODO(kszucs): use logging
            click.echo('Waiting {} minutes and then checking again'
                       .format(poll_interval_minutes))
            time.sleep(poll_interval_minutes * 60)


class Config(dict):

    @classmethod
    def load_yaml(cls, path):
        with Path(path).open() as fp:
            return cls(yaml.load(fp))

    def select(self, tasks=None, groups=None):
        config_groups = dict(self['groups'])
        config_tasks = dict(self['tasks'])
        valid_groups = set(config_groups.keys())
        valid_tasks = set(config_tasks.keys())
        group_whitelist = list(groups or [])
        task_whitelist = list(tasks or [])

        # validate that the passed groups are defined in the config
        requested_groups = set(group_whitelist)
        invalid_groups = requested_groups - valid_groups
        if invalid_groups:
            msg = 'Invalid group(s) {!r}. Must be one of {!r}'.format(
                invalid_groups, valid_groups
            )
            raise ValueError(msg)

        # merge the tasks defined in the selected groups
        task_patterns = [list(config_groups[name]) for name in group_whitelist]
        task_patterns = set(sum(task_patterns, task_whitelist))

        # treat the task names as glob patterns to select tasks more easily
        requested_tasks = set(
            toolz.concat(
                fnmatch.filter(valid_tasks, p) for p in task_patterns
            )
        )

        # validate that the passed and matched tasks are defined in the config
        invalid_tasks = requested_tasks - valid_tasks
        if invalid_tasks:
            msg = 'Invalid task(s) {!r}. Must be one of {!r}'.format(
                invalid_tasks, valid_tasks
            )
            raise ValueError(msg)

        return {
            task_name: config_tasks[task_name] for task_name in requested_tasks
        }

    def validate(self):
        # validate that the task groups are properly referening the tasks
        for group_name, group in self['groups'].items():
            for pattern in group:
                tasks = self.select(tasks=[pattern])
                if not tasks:
                    raise ValueError(
                        "The pattern `{}` defined for task group `{}` is not "
                        "matching any of the tasks defined in the "
                        "configuration file.".format(pattern, group_name)
                    )

        # validate that the tasks are constructible
        for task_name, task in self['tasks'].items():
            try:
                Task(**task)
            except Exception as e:
                raise ValueError(
                    'Unable to construct a task object from the '
                    'definition  of task `{}`. The original error message '
                    'is: `{}`'.format(task_name, str(e))
                )

        # validate that the defined tasks are renderable, in order to to that
        # define the required object with dummy data
        target = Target(
            head='e279a7e06e61c14868ca7d71dea795420aea6539',
            branch='master',
            remote='https://github.com/apache/arrow',
            version='1.0.0dev123',
            email='dummy@example.ltd'
        )

        for task_name, task in self['tasks'].items():
            task = Task(**task)
            files = task.render_files(
                arrow=target,
                queue_remote_url='https://github.com/org/crossbow'
            )
            if not files:
                raise ValueError('No files have been rendered for task `{}`'
                                 .format(task_name))


class Report:

    def __init__(self, job):
        self.job = job

    def show(self):
        raise NotImplementedError()


class ConsoleReport(Report):
    """Report the status of a Job to the console using click"""

    # output table's header template
    HEADER = '[{state:>7}] {branch:<49} {content:>20}'

    # output table's row template for assets
    ARTIFACT_NAME = '{artifact:>70} '
    ARTIFACT_STATE = '[{state:>7}]'

    # state color mapping to highlight console output
    COLORS = {
        # from CombinedStatus
        'error': 'red',
        'failure': 'red',
        'pending': 'yellow',
        'success': 'green',
        # custom state messages
        'ok': 'green',
        'missing': 'red'
    }

    def lead(self, state, branch, n_uploaded, n_expected):
        line = self.HEADER.format(
            state=state.upper(),
            branch=branch,
            content='uploaded {} / {}'.format(n_uploaded, n_expected)
        )
        return click.style(line, fg=self.COLORS[state.lower()])

    def header(self):
        header = self.HEADER.format(
            state='state',
            branch='Task / Branch',
            content='Artifacts'
        )
        delimiter = '-' * len(header)
        return '{}\n{}'.format(header, delimiter)

    def artifact(self, state, pattern, asset):
        if asset is None:
            artifact = pattern
            state = 'pending' if state == 'pending' else 'missing'
        else:
            artifact = asset.name
            state = 'ok'

        name_ = self.ARTIFACT_NAME.format(artifact=artifact)
        state_ = click.style(
            self.ARTIFACT_STATE.format(state=state.upper()),
            self.COLORS[state]
        )
        return name_ + state_

    def show(self, outstream, asset_callback=None):
        echo = partial(click.echo, file=outstream)

        # write table's header
        echo(self.header())

        # write table's body
        for task_name, task in sorted(self.job.tasks.items()):
            # write summary of the uploaded vs total assets
            status = task.status()
            assets = task.assets()

            # mapping of artifact pattern to asset or None of not uploaded
            n_expected = len(task.artifacts)
            n_uploaded = len(assets.uploaded_assets())
            echo(self.lead(status.combined_state, task.branch, n_uploaded,
                           n_expected))

            # write per asset status
            for artifact_pattern, asset in assets.items():
                if asset_callback is not None:
                    asset_callback(task_name, task, asset)
                echo(self.artifact(status.combined_state, artifact_pattern,
                                   asset))


class EmailReport(Report):

    HEADER = textwrap.dedent("""
        Arrow Build Report for Job {job_name}

        All tasks: {all_tasks_url}
    """)

    TASK = textwrap.dedent("""
          - {name}:
            URL: {url}
    """).strip()

    EMAIL = textwrap.dedent("""
        From: {sender_name} <{sender_email}>
        To: {recipient_email}
        Subject: {subject}

        {body}
    """).strip()

    STATUS_HEADERS = {
        # from CombinedStatus
        'error': 'Errored Tasks:',
        'failure': 'Failed Tasks:',
        'pending': 'Pending Tasks:',
        'success': 'Succeeded Tasks:',
    }

    def __init__(self, job, sender_name, sender_email, recipient_email):
        self.sender_name = sender_name
        self.sender_email = sender_email
        self.recipient_email = recipient_email
        super().__init__(job)

    def url(self, query):
        repo_url = self.job.queue.remote_url.strip('.git')
        return '{}/branches/all?query={}'.format(repo_url, query)

    def listing(self, tasks):
        return '\n'.join(
            sorted(
                self.TASK.format(name=task_name, url=self.url(task.branch))
                for task_name, task in tasks.items()
            )
        )

    def header(self):
        url = self.url(self.job.branch)
        return self.HEADER.format(job_name=self.job.branch, all_tasks_url=url)

    def subject(self):
        return (
            "[NIGHTLY] Arrow Build Report for Job {}".format(self.job.branch)
        )

    def body(self):
        buffer = StringIO()
        buffer.write(self.header())

        tasks_by_state = toolz.groupby(
            lambda name_task_pair: name_task_pair[1].status().combined_state,
            self.job.tasks.items()
        )

        for state in ('failure', 'error', 'pending', 'success'):
            if state in tasks_by_state:
                tasks = dict(tasks_by_state[state])
                buffer.write('\n')
                buffer.write(self.STATUS_HEADERS[state])
                buffer.write('\n')
                buffer.write(self.listing(tasks))
                buffer.write('\n')

        return buffer.getvalue()

    def email(self):
        return self.EMAIL.format(
            sender_name=self.sender_name,
            sender_email=self.sender_email,
            recipient_email=self.recipient_email,
            subject=self.subject(),
            body=self.body()
        )

    def show(self, outstream):
        outstream.write(self.email())

    def send(self, smtp_user, smtp_password, smtp_server, smtp_port):
        import smtplib

        email = self.email()

        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        server.ehlo()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, self.recipient_email, email)
        server.close()


class GithubPage:

    def __init__(self, jobs):
        self.jobs = list(jobs)

    def _generate_page(self, links):
        links = ['<li><a href="{}">{}</a></li>'.format(url, name)
                 for name, url in sorted(links.items())]
        return '<html><body><ul>{}</ul></body></html>'.format(''.join(links))

    def _generate_toc(self, files):
        result, links = {}, {}
        for k, v in files.items():
            if isinstance(v, dict):
                result[k] = self._generate_toc(v)
                links[k] = '{}/'.format(k)
            else:
                result[k] = v

        if links:
            result['index.html'] = self._generate_page(links)

        return result

    def _is_failed(self, status, task_name):
        # for showing task statuses during the rendering procedure
        if status.combined_state == 'success':
            msg = click.style('[  OK] {}'.format(task_name), fg='green')
            failed = False
        else:
            msg = click.style('[FAIL] {}'.format(task_name), fg='yellow')
            failed = True

        click.echo(msg)
        return failed

    def render_nightlies(self):
        click.echo('\n\nRENDERING NIGHTLIES')
        nightly_files = {}

        for job in self.jobs:
            click.echo('\nJOB: {}'.format(job.branch))
            job_files = {}

            for task_name, task in sorted(job.tasks.items()):
                # TODO: also render check runs?
                status = task.status()

                task_files = {'status.json': status.github_status.as_json()}
                links = {'status.json': 'status.json'}

                if not self._is_failed(status, task_name):
                    # accumulate links to uploaded assets
                    for asset in task.assets().uploaded_assets():
                        links[asset.name] = asset.browser_download_url

                if links:
                    page_content = self._generate_page(links)
                    task_files['index.html'] = page_content

                job_files[task_name] = task_files

            nightly_files[str(job.date)] = job_files

        # write the most recent wheels under the latest directory
        if 'latest' not in nightly_files:
            nightly_files['latest'] = job_files

        return nightly_files

    def render_pypi_simple(self):
        click.echo('\n\nRENDERING PYPI')

        wheels = {}
        for job in self.jobs:
            click.echo('\nJOB: {}'.format(job.branch))

            for task_name, task in sorted(job.tasks.items()):
                if not task_name.startswith('wheel'):
                    continue
                status = task.status()
                if self._is_failed(status, task_name):
                    continue
                for asset in task.assets().uploaded_assets():
                    wheels[asset.name] = asset.browser_download_url

        return {'pyarrow': {'index.html': self._generate_page(wheels)}}

    def render(self):
        # directory structure for the github pages, only wheels are supported
        # at the moment
        files = self._generate_toc({
            'nightly': self.render_nightlies(),
            'pypi': self.render_pypi_simple(),
        })
        files['.nojekyll'] = ''
        return files


# configure yaml serializer
yaml = YAML()
yaml.register_class(Job)
yaml.register_class(Task)
yaml.register_class(Target)


# define default paths
DEFAULT_CONFIG_PATH = str(CWD / 'tasks.yml')
DEFAULT_ARROW_PATH = CWD.parents[1]
DEFAULT_QUEUE_PATH = CWD.parents[2] / 'crossbow'


@click.group()
@click.option('--github-token', '-t', default=None,
              help='OAuth token for GitHub authentication')
@click.option('--arrow-path', '-a',
              type=click.Path(), default=str(DEFAULT_ARROW_PATH),
              help='Arrow\'s repository path. Defaults to the repository of '
                   'this script')
@click.option('--queue-path', '-q',
              type=click.Path(), default=str(DEFAULT_QUEUE_PATH),
              help='The repository path used for scheduling the tasks. '
                   'Defaults to crossbow directory placed next to arrow')
@click.option('--queue-remote', '-qr', default=None,
              help='Force to use this remote URL for the Queue repository')
@click.option('--output-file', metavar='<output>',
              type=click.File('w', encoding='utf8'), default='-',
              help='Capture output result into file.')
@click.pass_context
def crossbow(ctx, github_token, arrow_path, queue_path, queue_remote,
             output_file):
    ctx.ensure_object(dict)
    ctx.obj['output'] = output_file
    ctx.obj['arrow'] = Repo(arrow_path)
    ctx.obj['queue'] = Queue(queue_path, remote_url=queue_remote,
                             github_token=github_token, require_https=True)


@crossbow.command()
@click.option('--config-path', '-c',
              type=click.Path(exists=True), default=DEFAULT_CONFIG_PATH,
              help='Task configuration yml. Defaults to tasks.yml')
def check_config(config_path):
    # load available tasks configuration and groups from yaml
    config = Config.load_yaml(config_path)
    config.validate()


@crossbow.command()
@click.argument('tasks', nargs=-1, required=False)
@click.option('--group', '-g', 'groups', multiple=True,
              help='Submit task groups as defined in task.yml')
@click.option('--job-prefix', default='build',
              help='Arbitrary prefix for branch names, e.g. nightly')
@click.option('--config-path', '-c',
              type=click.Path(exists=True), default=DEFAULT_CONFIG_PATH,
              help='Task configuration yml. Defaults to tasks.yml')
@click.option('--arrow-version', '-v', default=None,
              help='Set target version explicitly.')
@click.option('--arrow-remote', '-r', default=None,
              help='Set GitHub remote explicitly, which is going to be cloned '
                   'on the CI services. Note, that no validation happens '
                   'locally. Examples: https://github.com/apache/arrow or '
                   'https://github.com/kszucs/arrow.')
@click.option('--arrow-branch', '-b', default=None,
              help='Give the branch name explicitly, e.g. master, ARROW-1949.')
@click.option('--arrow-sha', '-t', default=None,
              help='Set commit SHA or Tag name explicitly, e.g. f67a515, '
                   'apache-arrow-0.11.1.')
@click.option('--dry-run/--push', default=False,
              help='Just display the rendered CI configurations without '
                   'submitting them')
@click.pass_obj
def submit(obj, tasks, groups, job_prefix, config_path, arrow_version,
           arrow_remote, arrow_branch, arrow_sha, dry_run):
    output = obj['output']
    queue, arrow = obj['queue'], obj['arrow']

    # load available tasks configuration and groups from yaml
    config = Config.load_yaml(config_path)
    config.validate()

    # Override the detected repo url / remote, branch and sha - this aims to
    # make release procedure a bit simpler.
    # Note, that the target resivion's crossbow templates must be
    # compatible with the locally checked out version of crossbow (which is
    # in case of the release procedure), because the templates still
    # contain some business logic (dependency installation, deployments)
    # which will be reduced to a single command in the future.
    target = Target.from_repo(arrow, remote=arrow_remote, branch=arrow_branch,
                              head=arrow_sha, version=arrow_version)

    # instantiate the job object
    job = Job.from_config(config=config, target=target, tasks=tasks,
                          groups=groups)

    if dry_run:
        yaml.dump(job, output)
    else:
        queue.fetch()
        queue.put(job, prefix=job_prefix)
        queue.push()
        yaml.dump(job, output)
        click.echo('Pushed job identifier is: `{}`'.format(job.branch))


@crossbow.command()
@click.argument('job-name', required=True)
@click.pass_obj
def status(obj, job_name):
    output = obj['output']
    queue = obj['queue']
    queue.fetch()

    job = queue.get(job_name)
    ConsoleReport(job).show(output)


@crossbow.command()
@click.argument('prefix', required=True)
@click.pass_obj
def latest_prefix(obj, prefix):
    queue = obj['queue']
    queue.fetch()

    latest = queue.latest_for_prefix(prefix)
    click.echo(latest.branch)


@crossbow.command()
@click.argument('job-name', required=True)
@click.option('--sender-name', '-n',
              help='Name to use for report e-mail.')
@click.option('--sender-email', '-e',
              help='E-mail to use for report e-mail.')
@click.option('--recipient-email', '-r',
              help='Where to send the e-mail report')
@click.option('--smtp-user', '-u',
              help='E-mail address to use for SMTP login')
@click.option('--smtp-password', '-P',
              help='SMTP password to use for report e-mail.')
@click.option('--smtp-server', '-s', default='smtp.gmail.com',
              help='SMTP server to use for report e-mail.')
@click.option('--smtp-port', '-p', default=465,
              help='SMTP port to use for report e-mail.')
@click.option('--poll/--no-poll', default=False,
              help='Wait for completion if there are tasks pending')
@click.option('--poll-max-minutes', default=180,
              help='Maximum amount of time waiting for job completion')
@click.option('--poll-interval-minutes', default=10,
              help='Number of minutes to wait to check job status again')
@click.option('--send/--dry-run', default=False,
              help='Just display the report, don\'t send it')
@click.pass_obj
def report(obj, job_name, sender_name, sender_email, recipient_email,
           smtp_user, smtp_password, smtp_server, smtp_port, poll,
           poll_max_minutes, poll_interval_minutes, send):
    """
    Send an e-mail report showing success/failure of tasks in a Crossbow run
    """
    output = obj['output']
    queue = obj['queue']
    queue.fetch()

    job = queue.get(job_name)
    report = EmailReport(
        job=job,
        sender_name=sender_name,
        sender_email=sender_email,
        recipient_email=recipient_email
    )

    if poll:
        job.wait_until_finished(
            poll_max_minutes=poll_max_minutes,
            poll_interval_minutes=poll_interval_minutes
        )

    if send:
        report.send(
            smtp_user=smtp_user,
            smtp_password=smtp_password,
            smtp_server=smtp_server,
            smtp_port=smtp_port
        )
    else:
        report.show(output)


@crossbow.group()
@click.pass_context
def github_page(ctx):
    # currently We only list links to nightly binary wheels
    pass


@github_page.command('generate')
@click.option('-n', default=10,
              help='Number of most recent jobs')
@click.option('--gh-branch', default='gh-pages',
              help='Github pages branch')
@click.option('--job-prefix', default='nightly',
              help='Job/tag prefix the wheel links should be generated for')
@click.option('--dry-run/--push', default=False,
              help='Just render the files without pushing')
@click.option('--github-push-token', '-t', default=None,
              help='OAuth token for GitHub authentication only used for '
                   'pushing to the crossbow repository, the API requests '
                   'will consume the token passed to the top level crossbow '
                   'command.')
@click.pass_context
def generate_github_page(ctx, n, gh_branch, job_prefix, dry_run,
                         github_push_token):
    queue = ctx.obj['queue']
    queue.fetch()

    # fail early if the requested branch is not available in the local checkout
    remote = 'origin'
    branch = queue.repo.branches['{}/{}'.format(remote, gh_branch)]
    head = queue.repo[branch.target]

    # $ at the end of the pattern is important because we're only looking for
    # branches belonging to jobs not branches belonging to tasks
    # the branches we're looking for are like 2020-01-01-0
    jobs = queue.jobs(pattern=r"^nightly-(\d{4})-(\d{2})-(\d{2})-(\d+)$")
    page = GithubPage(toolz.take(n, jobs))
    files = page.render()
    files.update(unflatten_tree(_default_tree))

    if dry_run:
        click.echo(files)
        return

    refname = 'refs/heads/{}'.format(gh_branch)
    message = 'Update nightly wheel links {}'.format(date.today())
    commit = queue.create_commit(files, parents=[head.id], message=message,
                                 reference_name=refname)
    click.echo('Updated `{}` branch\'s head to `{}`'
               .format(gh_branch, commit.id))
    queue.push([refname], github_token=github_push_token)


@crossbow.command()
@click.argument('job-name', required=True)
@click.option('-t', '--target-dir',
              default=str(DEFAULT_ARROW_PATH / 'packages'),
              type=click.Path(file_okay=False, dir_okay=True),
              help='Directory to download the build artifacts')
@click.pass_obj
def download_artifacts(obj, job_name, target_dir):
    """Download build artifacts from GitHub releases"""
    output = obj['output']

    # fetch the queue repository
    queue = obj['queue']
    queue.fetch()

    # query the job's artifacts
    job = queue.get(job_name)

    # create directory to download the assets to
    target_dir = Path(target_dir).absolute() / job_name
    target_dir.mkdir(parents=True, exist_ok=True)

    # download the assets while showing the job status
    def asset_callback(task_name, task, asset):
        if asset is not None:
            path = target_dir / task_name / asset.name
            path.parent.mkdir(exist_ok=True)
            asset.download(path)

    click.echo('Downloading {}\'s artifacts.'.format(job_name))
    click.echo('Destination directory is {}'.format(target_dir))
    click.echo()

    report = ConsoleReport(job)
    report.show(output, asset_callback=asset_callback)


@crossbow.command()
@click.option('--sha', required=True, help='Target committish')
@click.option('--tag', required=True, help='Target tag')
@click.option('--method', default='curl', help='Use cURL to upload')
@click.option('--pattern', '-p', 'patterns', required=True, multiple=True,
              help='File pattern to upload as assets')
@click.pass_obj
def upload_artifacts(obj, tag, sha, patterns, method):
    queue = obj['queue']
    queue.github_overwrite_release_assets(
        tag_name=tag, target_commitish=sha, method=method, patterns=patterns
    )


if __name__ == '__main__':
    crossbow(obj={}, auto_envvar_prefix='CROSSBOW')
