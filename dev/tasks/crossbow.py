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
import sys
import time
import click
import urllib
import pygit2
import github3

from io import StringIO
from pathlib import Path
from textwrap import dedent
from jinja2 import Template, StrictUndefined
from setuptools_scm import get_version
from ruamel.yaml import YAML


CWD = Path(__file__).parent.absolute()


class GitRemoteCallbacks(pygit2.RemoteCallbacks):

    def __init__(self, token):
        self.token = token
        self.attempts = 0
        super(GitRemoteCallbacks, self).__init__()

    def push_update_reference(self, refname, message):
        pass

    def update_tips(self, refname, old, new):
        pass

    def credentials(self, url, username_from_url, allowed_types):
        # its a libgit2 bug, that it infinitly retries the authentication
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


class Repo:
    """Base class for interaction with local git repositories

    A high level wrapper used for both reading revision information from
    arrow's repository and pushing continuous integration tasks to the queue
    repository.
    """

    def __init__(self, path, github_token=None):
        self.path = Path(path)
        self.repo = pygit2.Repository(str(self.path))
        self.github_token = github_token
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
    def origin(self):
        return self.repo.remotes['origin']

    def fetch(self):
        refspec = '+refs/heads/*:refs/remotes/origin/*'
        self.origin.fetch([refspec])

    def push(self):
        callbacks = GitRemoteCallbacks(self.github_token)
        self.origin.push(self._updated_refs, callbacks=callbacks)
        self.updated_refs = []

    @property
    def head(self):
        """Currently checked out commit's sha"""
        return self.repo.head

    @property
    def branch(self):
        """Currently checked out branch"""
        return self.repo.branches[self.repo.head.shorthand]

    @property
    def remote(self):
        """Currently checked out branch's remote counterpart"""
        return self.repo.remotes[self.branch.upstream.remote_name]

    @property
    def remote_url(self):
        """
        Currently checked out branch's remote counterpart URL

        If an SSH github url is set, it will be replaced by the https
        equivalent.
        """
        return self.remote.url.replace('git@github.com:', 'https://github.com/')

    @property
    def email(self):
        return next(self.repo.config.get_multivar('user.email'))

    @property
    def signature(self):
        name = next(self.repo.config.get_multivar('user.name'))
        return pygit2.Signature(name, self.email, int(time.time()))

    def create_branch(self, branch_name, files, parents=[], message=''):
        # 1. create tree
        builder = self.repo.TreeBuilder()

        for filename, content in files.items():
            # insert the file and creating the new filetree
            blob_id = self.repo.create_blob(content)
            builder.insert(filename, blob_id, pygit2.GIT_FILEMODE_BLOB)

        tree_id = builder.write()

        # 2. create commit with the tree created above
        author = committer = self.signature
        commit_id = self.repo.create_commit(None, author, committer, message,
                                            tree_id, parents)
        commit = self.repo[commit_id]

        # 3. create branch pointing to the previously created commit
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
        m = re.match('.*\/([^\/]+)\/([^\/\.]+)(\.git)?$', self.remote_url)
        user, repo = m.group(1), m.group(2)
        return user, repo

    def as_github_repo(self):
        """Converts it to a repository object which wraps the GitHub API"""
        username, reponame = self._parse_github_user_repo()
        gh = github3.login(token=self.github_token)
        return gh.repository(username, reponame)


class Queue(Repo):

    def _next_job_id(self, prefix):
        """Auto increments the branch's identifier based on the prefix"""
        pattern = re.compile('[\w\/-]*{}-(\d+)'.format(prefix))
        matches = list(filter(None, map(pattern.match, self.repo.branches)))
        if matches:
            latest = max(int(m.group(1)) for m in matches)
        else:
            latest = 0
        return '{}-{}'.format(prefix, latest + 1)

    def get(self, job_name):
        branch_name = 'origin/{}'.format(job_name)
        branch = self.repo.branches[branch_name]
        content = self.file_contents(branch.target, 'job.yml')
        buffer = StringIO(content.decode('utf-8'))
        return yaml.load(buffer)

    def put(self, job, prefix='build'):
        # TODO(kszucs): more verbose error handling
        assert isinstance(job, Job)
        assert job.branch is None
        assert len(job.tasks) > 0

        # auto increment and set next job id, e.g. build-85
        job.branch = self._next_job_id(prefix)

        # create tasks' branches
        for task_name, task in job.tasks.items():
            task.branch = '{}-{}'.format(job.branch, task_name)
            files = task.render_files(job=job, arrow=job.target)
            branch = self.create_branch(task.branch, files=files)
            self.create_tag(task.tag, branch.target)
            task.commit = str(branch.target)

        # create job's branch with its description
        return self.create_branch(job.branch, files=job.render_files())

    def github_statuses(self, job):
        repo = self.as_github_repo()
        return {name: repo.commit(task.commit).status()
                for name, task in job.tasks.items()}

    def github_assets(self, task):
        repo = self.as_github_repo()
        try:
            release = repo.release_from_tag(task.tag)
        except github3.exceptions.NotFoundError:
            return {}

        assets = {a.name: a for a in release.assets()}

        artifacts = {}
        for artifact in task.artifacts:
            # artifact can be a regex pattern
            pattern = re.compile(artifact)
            matches = list(filter(None, map(pattern.match, assets.keys())))
            num_matches = len(matches)

            # validate artifact pattern matches single asset
            if num_matches > 1:
                raise ValueError(
                    'Only a single asset should match pattern `{}`, there are '
                    'multiple ones: {}'.format(', '.join(matches))
                )
            elif num_matches == 1:
                artifacts[artifact] = assets[matches[0].group(0)]

        return artifacts

    def upload_assets(self, job, files, content_type):
        repo = self.as_github_repo()
        release = repo.release_from_tag(job.branch)
        assets = {a.name: a for a in release.assets()}

        for path in files:
            if path.name in assets:
                # remove already uploaded asset
                assets[path.name].delete()
            with path.open('rb') as fp:
                release.upload_asset(name=path.name, asset=fp,
                                     content_type=content_type)


class Target:

    def __init__(self, version, email=None):
        self.email = email
        self.version = version

    @property
    def source(self):
        raise NotImplementedError

    @property
    def deb_version(self):
        base_version, rc = self._split_version()
        if rc is None:
            return '{}-1'.format(base_version)
        else:
            # Real version is {base_version}~{rc}-1 but GitHub release
            # replaces "~" with "."...
            return '{}.{}-1'.format(base_version, rc)

    @property
    def rpm_version(self):
        base_version, rc = self._split_version()
        if rc is None:
            return '{}.1'.format(base_version)
        else:
            return '{}.0.{}'.format(base_version, rc)

    def _split_version(self):
        match = re.match(r'(.+)-(rc\d+)\Z', self.version)
        if match is None:
            return [self.version, None]
        else:
            base_version = match[1]
            rc = match[2]
            return [base_version, rc]


class GitTarget(Target):
    """Describes target repository and revision the builds run against

    This serializable data container holding information about arrow's
    git remote, branch, sha and version number as well as some metadata
    (currently only an email address where the notification should be sent).
    """

    def __init__(self, head, branch, remote, **kwargs):
        self.head = head
        self.branch = branch
        self.remote = remote
        super().__init__(**kwargs)

    @property
    def source(self):
        return 'git'

    @classmethod
    def from_repo(cls, repo):
        assert isinstance(repo, Repo)
        version = get_version(repo.path, local_scheme=lambda v: '')
        return cls(head=str(repo.head.target),
                   email=repo.email,
                   branch=repo.branch.branch_name,
                   remote=repo.remote_url,
                   version=version)


def _filename_from_url(url):
    parsed = urllib.parse.urlparse(url)
    return Path(parsed.path).name


class DistTarget(Target):
    """Describes target source archive the builds run against

    This serializable data container holding information about arrow's
    source distribution, tipically an apache release.
    """

    def __init__(self, archive_url, verify=True, **kwargs):
        assert archive_url.endswith('.tar.gz')
        self.archive_url = archive_url
        self.verify = verify
        super().__init__(**kwargs)

    @property
    def source(self):
        return 'dist'

    @property
    def signature_url(self):
        return self.archive_url + '.asc'

    @property
    def checksum_url(self):
        """SHA512 checksum"""
        return self.archive_url + '.sha512'

    @property
    def archive_filename(self):
        return _filename_from_url(self.archive_url)

    @property
    def checksum_filename(self):
        return _filename_from_url(self.checksum_url)

    @classmethod
    def from_url(cls, url, verify=True):
        filename = _filename_from_url(url)
        pattern = 'apache-arrow-(\d+\.\d+\.\d+([-\.]?(dev|rc)\d+)?)\.tar\.gz'
        m = re.match(pattern, filename)
        if m is None:
            raise ValueError('Cannot parse version number from url: `{}`'
                             .format(url))
        return cls(archive_url=url, verify=verify, version=m.group(1))


class Task:
    """Describes a build task and metadata required to render CI templates

    A task is represented as a single git commit and branch containing jinja2
    rendered files (currently appveyor.yml or .travis.yml configurations).

    A task can't be directly submitted to a queue, must belong to a job.
    Each task's unique identifier is its branch name, which is generated after
    submitting the job to a queue.
    """

    def __init__(self, platform, template, artifacts=None, params=None):
        assert platform in {'win', 'osx', 'linux'}
        self.platform = platform
        self.template = template
        self.artifacts = artifacts or []
        self.params = params or {}
        self.branch = None  # filled after adding to a queue
        self.commit = None

    def render_files(self, **extra_params):
        path = CWD / self.template
        template = Template(path.read_text(), undefined=StrictUndefined)
        rendered = template.render(task=self, **self.params, **extra_params)
        return {self.filename: rendered}

    @property
    def tag(self):
        return self.branch

    @property
    def ci(self):
        if self.platform == 'win':
            return 'appveyor'
        else:
            return 'travis'

    @property
    def filename(self):
        if self.ci == 'appveyor':
            return 'appveyor.yml'
        else:
            return '.travis.yml'


class Job:
    """Describes multiple tasks against a single target repository"""

    def __init__(self, target, tasks):
        assert isinstance(target, Target)
        assert all(isinstance(task, Task) for task in tasks.values())
        self.target = target
        self.tasks = tasks
        self.branch = None  # filled after adding to a queue

    def render_files(self):
        with StringIO() as buf:
            yaml.dump(self, buf)
            content = buf.getvalue()
        return {'job.yml': content}

    @property
    def email(self):
        return os.environ.get('CROSSBOW_EMAIL', self.target.email)


# configure yaml serializer
yaml = YAML()
yaml.register_class(Job)
yaml.register_class(Task)
yaml.register_class(GitTarget)
yaml.register_class(DistTarget)

# state color mapping to highlight console output
COLORS = {'ok': 'green',
          'error': 'red',
          'missing': 'red',
          'failure': 'red',
          'pending': 'yellow',
          'success': 'green'}

# define default paths
DEFAULT_CONFIG_PATH = CWD / 'tasks.yml'
DEFAULT_ARROW_PATH = CWD.parents[1]
DEFAULT_QUEUE_PATH = CWD.parents[2] / 'crossbow'


@click.group()
@click.option('--github-token', '-t', default=None,
              help='OAuth token for GitHub authentication')
@click.option('--arrow-repo', '-a',
              type=click.Path(exists=True), default=DEFAULT_ARROW_PATH,
              help='Arrow\'s repository path. Defaults to the repository of '
                   'this script')
@click.option('--queue-repo', '-q',
              type=click.Path(exists=True), default=DEFAULT_QUEUE_PATH,
              help='The repository path used for scheduling the tasks. '
                   'Defaults to crossbow directory placed next to arrow')
@click.pass_context
def crossbow(ctx, github_token, arrow_repo, queue_repo):
    if github_token is None:
        raise click.ClickException(
            'Could not determine GitHub token. Please set the '
            'CROSSBOW_GITHUB_TOKEN environment variable to a '
            'valid GitHub access token or pass one to --github-token.'
        )

    ctx.obj['arrow'] = Repo(Path(arrow_repo))
    ctx.obj['queue'] = Queue(Path(queue_repo), github_token=github_token)


def load_tasks_from_config(config_path, task_names, group_names):
    with Path(config_path).open() as fp:
        config = yaml.load(fp)

    valid_groups = set(config['groups'].keys())
    requested_groups = set(group_names)
    invalid_groups = requested_groups - valid_groups
    if invalid_groups:
        raise click.ClickException('Invalid group(s) {!r}. Must be one of {!r}'
                                   .format(invalid_groups, valid_groups))

    valid_tasks = set(config['tasks'].keys())
    requested_tasks = set(
        sum([config['groups'][g] for g in group_names], list(task_names))
    )
    invalid_tasks = requested_tasks - valid_tasks
    if invalid_tasks:
        raise click.ClickException('Invalid task(s) {!r}. Must be one of {!r}'
                                   .format(invalid_tasks, valid_tasks))

    return {t: config['tasks'][t] for t in requested_tasks}


@crossbow.command()
@click.argument('task', nargs=-1, required=False)
@click.option('--group', '-g', multiple=True,
              help='Submit task groups as defined in task.yml')
@click.option('--job-prefix', default='build',
              help='Arbitrary prefix for branch names, e.g. nightly')
@click.option('--config-path', '-c',
              type=click.Path(exists=True), default=DEFAULT_CONFIG_PATH,
              help='Task configuration yml. Defaults to tasks.yml')
@click.option('--arrow-dist', '-d', default=None,
              help='Build from apache source archive')
@click.option('--verify-dist/--no-verify', default=True)
@click.option('--arrow-version', '-v', default=None,
              help='Set target version explicitly')
@click.option('--dry-run/--push', default=False,
              help='Just display the rendered CI configurations without '
                   'submitting them')
@click.pass_context
def submit(ctx, task, group, job_prefix, config_path, dry_run,
           arrow_dist, verify_dist, arrow_version):
    queue, arrow = ctx.obj['queue'], ctx.obj['arrow']

    # instantiate build target
    if arrow_dist:
        target = DistTarget.from_url(arrow_dist, verify=verify_dist)
    else:
        target = GitTarget.from_repo(arrow)

    # explicitly set arrow version
    if arrow_version:
        target.version = arrow_version

    # task and group variables are lists, containing multiple values
    tasks = {}
    task_configs = load_tasks_from_config(config_path, task, group)
    for name, task in task_configs.items():
        # replace version number and create task instance from configuration
        variables = {
            'version': target.version,
            'deb_version': target.deb_version,
            'rpm_version': target.rpm_version,
        }
        artifacts = task.pop('artifacts', None) or []  # because of yaml
        artifacts = [fn.format(**variables) for fn in artifacts]
        tasks[name] = Task(**task, artifacts=artifacts)

    # create job instance, doesn't mutate git data yet
    if not tasks == 0:
        raise click.ClickException('No task has been specified')
    job = Job(target=target, tasks=tasks)

    if dry_run:
        yaml.dump(job, sys.stdout)
        delimiter = '-' * 79
        for task_name, task in job.tasks.items():
            files = task.render_files(job=job, arrow=job.target)
            for filename, content in files.items():
                click.echo('\n\n')
                click.echo(delimiter)
                click.echo('{:<29}{:>50}'.format(task_name, filename))
                click.echo(delimiter)
                click.echo(content)
    else:
        queue.fetch()
        queue.put(job, prefix=job_prefix)
        queue.push()
        yaml.dump(job, sys.stdout)
        click.echo('Pushed job identifier is: `{}`'.format(job.branch))


@crossbow.command()
@click.argument('job-name', required=True)
@click.pass_context
def status(ctx, job_name):
    queue = ctx.obj['queue']
    queue.fetch()

    tpl = '[{:>7}] {:<49} {:>20}'
    header = tpl.format('status', 'branch', 'artifacts')
    click.echo(header)
    click.echo('-' * len(header))

    job = queue.get(job_name)
    statuses = queue.github_statuses(job)

    for task_name, task in sorted(job.tasks.items()):
        status = statuses[task_name]
        assets = queue.github_assets(task)

        uploaded = 'uploaded {} / {}'.format(
            sum(a in assets for a in task.artifacts),
            len(task.artifacts)
        )
        leadline = tpl.format(status.state.upper(), task.branch, uploaded)
        click.echo(click.style(leadline, fg=COLORS[status.state]))

        for artifact in task.artifacts:
            try:
                asset = assets[artifact]
            except KeyError:
                state = 'pending' if status.state == 'pending' else 'missing'
                filename = '{:>70} '.format(artifact)
            else:
                state = 'ok'
                filename = '{:>70} '.format(asset.name)

            statemsg = '[{:>7}]'.format(state.upper())
            click.echo(filename + click.style(statemsg, fg=COLORS[state]))


@crossbow.command()
@click.argument('job-name', required=True)
@click.option('--gpg-homedir', default=None,
              help=('Full pathname to directory containing the public and '
                    'private keyrings. Default is whatever GnuPG defaults to'))
@click.option('--target-dir', default=DEFAULT_ARROW_PATH / 'packages',
              help='Directory to download the build artifacts')
@click.pass_context
def sign(ctx, job_name, gpg_homedir, target_dir):
    """Download and sign build artifacts from github releases"""
    import gnupg
    gpg = gnupg.GPG(gnupghome=gpg_homedir)

    # fetch the queue repository
    queue = ctx.obj['queue']
    queue.fetch()

    # query the job's artifacts
    job = queue.get(job_name)

    target_dir = Path(target_dir).absolute() / job_name
    target_dir.mkdir(parents=True, exist_ok=True)
    click.echo('Download {}\'s artifacts to {}'.format(job_name, target_dir))

    tpl = '{:<10} {:>68}'
    for task_name, task in sorted(job.tasks.items()):
        assets = queue.github_assets(task)
        artifact_dir = target_dir / task_name
        artifact_dir.mkdir(exist_ok=True)

        click.echo('\nDownloading and signing assets for task {}'
                    .format(task_name))
        click.echo('-' * 79)

        for artifact in task.artifacts:
            try:
                asset = assets[artifact]
            except KeyError:
                msg = click.style('[{:>8}]'.format('MISSING'),
                                  fg=COLORS['missing'])
                click.echo(tpl.format(msg, artifact))
                continue

            # download artifact
            artifact_path = artifact_dir / asset.name
            asset.download(artifact_path)

            # sign the artifact
            with artifact_path.open('rb') as fp:
                signature_path = Path(str(artifact_path) + '.sig')
                gpg.sign_file(fp, detach=True, clearsign=False,
                              output=str(signature_path))

            msg = click.style('[{:>8}]'.format('SIGNED'), fg=COLORS['ok'])
            click.echo(tpl.format(msg, asset.name))


if __name__ == '__main__':
    crossbow(obj={}, auto_envvar_prefix='CROSSBOW')
