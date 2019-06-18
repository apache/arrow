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
import time
import click
import hashlib
import gnupg
import toolz
import pygit2
import github3
import jira.client

from io import StringIO
from pathlib import Path
from textwrap import dedent
from datetime import datetime
from jinja2 import Template, StrictUndefined
from setuptools_scm.git import parse as parse_git_version
from ruamel.yaml import YAML


CWD = Path(__file__).parent.absolute()


NEW_FEATURE = 'New Features and Improvements'
BUGFIX = 'Bug Fixes'


def md(template, *args, **kwargs):
    """Wraps string.format with naive markdown escaping"""
    def escape(s):
        for char in ('*', '#', '_', '~', '`', '>'):
            s = s.replace(char, '\\' + char)
        return s
    return template.format(*map(escape, args), **toolz.valmap(escape, kwargs))


class JiraChangelog:

    def __init__(self, version, username, password,
                 server='https://issues.apache.org/jira'):
        self.server = server
        # clean version to the first numbers
        self.version = '.'.join(version.split('.')[:3])
        query = ("project=ARROW "
                 "AND fixVersion='{0}' "
                 "AND status = Resolved "
                 "AND resolution in (Fixed, Done) "
                 "ORDER BY issuetype DESC").format(self.version)
        self.client = jira.client.JIRA({'server': server},
                                       basic_auth=(username, password))
        self.issues = self.client.search_issues(query, maxResults=9999)

    def format_markdown(self):
        out = StringIO()

        issues_by_type = toolz.groupby(lambda i: i.fields.issuetype.name,
                                       self.issues)
        for typename, issues in sorted(issues_by_type.items()):
            issues.sort(key=lambda x: x.key)

            out.write(md('## {}\n\n', typename))
            for issue in issues:
                out.write(md('* {} - {}\n', issue.key, issue.fields.summary))
            out.write('\n')

        return out.getvalue()

    def format_website(self):
        # jira category => website category mapping
        categories = {
            'New Feature': 'feature',
            'Improvement': 'feature',
            'Wish': 'feature',
            'Task': 'feature',
            'Test': 'bug',
            'Bug': 'bug',
            'Sub-task': 'feature'
        }
        titles = {
            'feature': 'New Features and Improvements',
            'bugfix': 'Bug Fixes'
        }

        issues_by_category = toolz.groupby(
            lambda issue: categories[issue.fields.issuetype.name],
            self.issues
        )

        out = StringIO()

        for category in ('feature', 'bug'):
            title = titles[category]
            issues = issues_by_category[category]
            issues.sort(key=lambda x: x.key)

            out.write(md('## {}\n\n', title))
            for issue in issues:
                link = md('[{0}]({1}/browse/{0})', issue.key, self.server)
                out.write(md('* {} - {}\n', link, issue.fields.summary))
            out.write('\n')

        return out.getvalue()

    def render(self, old_changelog, website=False):
        old_changelog = old_changelog.splitlines()
        if website:
            new_changelog = self.format_website()
        else:
            new_changelog = self.format_markdown()

        out = StringIO()

        # Apache license header
        out.write('\n'.join(old_changelog[:18]))

        # Newly generated changelog
        today = datetime.today().strftime('%d %B %Y')
        out.write(md('\n\n# Apache Arrow {} ({})\n\n', self.version, today))
        out.write(new_changelog)
        out.write('\n'.join(old_changelog[19:]))

        return out.getvalue().strip()


class GitRemoteCallbacks(pygit2.RemoteCallbacks):

    def __init__(self, token):
        self.token = token
        self.attempts = 0
        super().__init__()

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
        try:
            self.origin.push(self._updated_refs, callbacks=callbacks)
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
        return self.repo.branches[self.repo.head.shorthand]

    @property
    def remote(self):
        """Currently checked out branch's remote counterpart"""
        if self.branch.upstream is None:
            raise RuntimeError('Cannot determine git remote to push to, try '
                               'to push the branch first to have a remote '
                               'tracking counterpart.')
        else:
            return self.repo.remotes[self.branch.upstream.remote_name]

    @property
    def remote_url(self):
        """Currently checked out branch's remote counterpart URL

        If an SSH github url is set, it will be replaced by the https
        equivalent usable with Github OAuth token.
        """
        return self.remote.url.replace('git@github.com:',
                                       'https://github.com/')

    @property
    def user_name(self):
        try:
            return next(self.repo.config.get_multivar('user.name'))
        except StopIteration:
            return os.environ.get('GIT_COMMITTER_NAME', 'unkown')

    @property
    def user_email(self):
        try:
            return next(self.repo.config.get_multivar('user.email'))
        except StopIteration:
            return os.environ.get('GIT_COMMITTER_EMAIL', 'unkown')

    @property
    def signature(self):
        return pygit2.Signature(self.user_name, self.user_email,
                                int(time.time()))

    def create_branch(self, branch_name, files, parents=[], message='',
                      signature=None):
        # 1. create tree
        builder = self.repo.TreeBuilder()

        for filename, content in files.items():
            # insert the file and creating the new filetree
            blob_id = self.repo.create_blob(content)
            builder.insert(filename, blob_id, pygit2.GIT_FILEMODE_BLOB)

        tree_id = builder.write()

        # 2. create commit with the tree created above
        # TODO(kszucs): pass signature explicitly
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
        m = re.match(r'.*\/([^\/]+)\/([^\/\.]+)(\.git)?$', self.remote_url)
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
        pattern = re.compile(r'[\w\/-]*{}-(\d+)'.format(prefix))
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
        if not isinstance(job, Job):
            raise ValueError('`job` must be an instance of Job')
        if job.branch is not None:
            raise ValueError('`job.branch` is automatically generated, thus '
                             'it must be blank')

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


def get_version(root, **kwargs):
    """
    Parse function for setuptools_scm that ignores tags for non-C++
    subprojects, e.g. apache-arrow-js-XXX tags.
    """
    kwargs['describe_command'] =\
        'git describe --dirty --tags --long --match "apache-arrow-[0-9].*"'
    return parse_git_version(root, **kwargs)


class Target:
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
            version = get_version(repo.path).format_with('{tag}.dev{distance}')
        if email is None:
            email = repo.user_email

        return cls(head=head, email=email, branch=branch, remote=remote,
                   version=version)


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
        params = toolz.merge(self.params, extra_params)
        template = Template(path.read_text(), undefined=StrictUndefined)
        rendered = template.render(task=self, **params)
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
        if not tasks:
            raise ValueError('no tasks were provided for the job')
        if not all(isinstance(task, Task) for task in tasks.values()):
            raise ValueError('each `tasks` mus be an instance of Task')
        if not isinstance(target, Target):
            raise ValueError('`target` must be an instance of Target')
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
yaml.register_class(Target)

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
@click.option('--arrow-path', '-a',
              type=click.Path(exists=True), default=DEFAULT_ARROW_PATH,
              help='Arrow\'s repository path. Defaults to the repository of '
                   'this script')
@click.option('--queue-path', '-q',
              type=click.Path(exists=True), default=DEFAULT_QUEUE_PATH,
              help='The repository path used for scheduling the tasks. '
                   'Defaults to crossbow directory placed next to arrow')
@click.pass_context
def crossbow(ctx, github_token, arrow_path, queue_path):
    if github_token is None:
        raise click.ClickException(
            'Could not determine GitHub token. Please set the '
            'CROSSBOW_GITHUB_TOKEN environment variable to a '
            'valid GitHub access token or pass one to --github-token.'
        )

    ctx.obj['arrow'] = Repo(Path(arrow_path))
    ctx.obj['queue'] = Queue(Path(queue_path), github_token=github_token)


@crossbow.command()
@click.option('--changelog-path', '-c', type=click.Path(exists=True),
              default=DEFAULT_ARROW_PATH / 'CHANGELOG.md',
              help='Path of changelog to update')
@click.option('--arrow-version', '-v', default=None,
              help='Set target version explicitly')
@click.option('--is-website', '-w', default=False)
@click.option('--jira-username', '-u', default=None, help='JIRA username')
@click.option('--jira-password', '-P', default=None, help='JIRA password')
@click.option('--dry-run/--write', default=False,
              help='Just display the new changelog, don\'t write it')
@click.pass_context
def changelog(ctx, changelog_path, arrow_version, is_website, jira_username,
              jira_password, dry_run):
    changelog_path = Path(changelog_path)
    target = Target.from_repo(ctx.obj['arrow'])
    version = arrow_version or target.version

    changelog = JiraChangelog(version, username=jira_username,
                              password=jira_password)
    new_content = changelog.render(changelog_path.read_text(),
                                   website=is_website)

    if dry_run:
        click.echo(new_content)
    else:
        changelog_path.write_text(new_content)
        click.echo('New changelog successfully generated, see git diff for the'
                   'changes')


def load_tasks_from_config(config_path, task_names, group_names):
    with Path(config_path).open() as fp:
        config = yaml.load(fp)

    groups = config['groups']
    tasks = config['tasks']

    valid_groups = set(groups.keys())
    valid_tasks = set(tasks.keys())

    requested_groups = set(group_names)
    invalid_groups = requested_groups - valid_groups
    if invalid_groups:
        raise click.ClickException('Invalid group(s) {!r}. Must be one of {!r}'
                                   .format(invalid_groups, valid_groups))

    requested_tasks = [list(groups[name]) for name in group_names]
    requested_tasks = set(sum(requested_tasks, list(task_names)))
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
@click.option('--arrow-version', '-v', default=None,
              help='Set target version explicitly.')
@click.option('--arrow-remote', '-r', default=None,
              help='Set Github remote explicitly, which is going to be cloned '
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
@click.option('--output', metavar='<output>',
              type=click.File('w', encoding='utf8'), default='-',
              help='Capture output result into file.')
@click.pass_context
def submit(ctx, task, group, job_prefix, config_path, arrow_version,
           arrow_remote, arrow_branch, arrow_sha, dry_run, output):
    queue, arrow = ctx.obj['queue'], ctx.obj['arrow']

    # Override the detected repo url / remote, branch and sha - this aims to
    # make release procedure a bit simpler.
    # Note, that the target resivion's crossbow templates must be
    # compatible with the locally checked out version of crossbow (which is
    # in case of the release procedure), because the templates still
    # contain some business logic (dependency installation, deployments)
    # which will be reduced to a single command in the future.
    target = Target.from_repo(arrow, remote=arrow_remote, branch=arrow_branch,
                              head=arrow_sha, version=arrow_version)
    params = {
        'version': target.version,
        'no_rc_version': target.no_rc_version,
    }

    # task and group variables are lists, containing multiple values
    tasks = {}
    task_configs = load_tasks_from_config(config_path, task, group)
    for name, task in task_configs.items():
        # replace version number and create task instance from configuration
        artifacts = task.pop('artifacts', None) or []  # because of yaml
        artifacts = [fn.format(**params) for fn in artifacts]
        tasks[name] = Task(artifacts=artifacts, **task)

    # create job instance, doesn't mutate git data yet
    job = Job(target=target, tasks=tasks)

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
@click.option('--output', metavar='<output>',
              type=click.File('w', encoding='utf8'), default='-',
              help='Capture output result into file.')
@click.pass_context
def status(ctx, job_name, output):
    queue = ctx.obj['queue']
    queue.fetch()

    tpl = '[{:>7}] {:<49} {:>20}'
    header = tpl.format('status', 'branch', 'artifacts')
    click.echo(header, file=output)
    click.echo('-' * len(header), file=output)

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
        click.echo(click.style(leadline, fg=COLORS[status.state]), file=output)

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
            click.echo(filename + click.style(statemsg, fg=COLORS[state]),
                       file=output)


def hashbytes(bytes, algoname):
    """Hash `bytes` using the algorithm named `algoname`.

    Parameters
    ----------
    bytes : bytes
        The bytes to hash
    algoname : str
        The name of class in the hashlib standard library module

    Returns
    -------
    str
        Hexadecimal digest of `bytes` hashed using `algoname`
    """
    algo = getattr(hashlib, algoname)()
    algo.update(bytes)
    result = algo.hexdigest()
    return result


@crossbow.command()
@click.argument('job-name', required=True)
@click.option('-g', '--gpg-homedir', default=None,
              type=click.Path(exists=True, file_okay=False, dir_okay=True),
              help=('Full pathname to directory containing the public and '
                    'private keyrings. Default is whatever GnuPG defaults to'))
@click.option('-t', '--target-dir', default=DEFAULT_ARROW_PATH / 'packages',
              type=click.Path(file_okay=False, dir_okay=True),
              help='Directory to download the build artifacts')
@click.option('-a', '--algorithm',
              default=['sha256', 'sha512'],
              show_default=True,
              type=click.Choice(sorted(hashlib.algorithms_guaranteed)),
              multiple=True,
              help=('Algorithm(s) used to generate checksums. Pass multiple '
                    'algorithms by passing -a/--algorithm multiple times'))
@click.pass_context
def sign(ctx, job_name, gpg_homedir, target_dir, algorithm):
    """Download and sign build artifacts from github releases"""
    gpg = gnupg.GPG(gnupghome=gpg_homedir)

    # fetch the queue repository
    queue = ctx.obj['queue']
    queue.fetch()

    # query the job's artifacts
    job = queue.get(job_name)

    target_dir = Path(target_dir).absolute() / job_name
    target_dir.mkdir(parents=True, exist_ok=True)
    click.echo('Download {}\'s artifacts to {}'.format(job_name, target_dir))

    tpl = '{:<10} {:>73}'

    task_items = sorted(job.tasks.items())
    ntasks = len(task_items)

    for i, (task_name, task) in enumerate(task_items, start=1):
        assets = queue.github_assets(task)
        artifact_dir = target_dir / task_name
        artifact_dir.mkdir(exist_ok=True)

        basemsg = 'Downloading and signing assets for task {}'.format(
            click.style(task_name, bold=True)
        )
        click.echo(
            '\n{} {:>{size}}' .format(
                basemsg,
                click.style('{}/{}'.format(i, ntasks), bold=True),
                size=89 - (len(basemsg) + 1) + 2 * len(
                    click.style('', bold=True))
            )
        )
        click.echo('-' * 89)

        for artifact in task.artifacts:
            try:
                asset = assets[artifact]
            except KeyError:
                msg = click.style('[{:>13}]'.format('MISSING'),
                                  fg=COLORS['missing'])
                click.echo(tpl.format(msg, artifact))
            else:
                click.echo(click.style(artifact, bold=True))

                # download artifact
                artifact_path = artifact_dir / asset.name
                asset.download(artifact_path)

                # sign the artifact
                signature_path = Path(str(artifact_path) + '.asc')
                with artifact_path.open('rb') as fp:
                    gpg.sign_file(fp, detach=True, clearsign=False,
                                  binary=False,
                                  output=str(signature_path))

                # compute checksums for the artifact
                artifact_bytes = artifact_path.read_bytes()
                for algo in algorithm:
                    suffix = '.{}'.format(algo)
                    checksum_path = Path(str(artifact_path) + suffix)
                    checksum = '{}  {}'.format(
                        hashbytes(artifact_bytes, algo), artifact_path.name
                    )
                    checksum_path.write_text(checksum)
                    msg = click.style(
                        '[{:>13}]'.format('{} HASHED'.format(algo)),
                        fg='blue'
                    )
                    click.echo(tpl.format(msg, checksum_path.name))

                msg = click.style('[{:>13}]'.format('SIGNED'), fg=COLORS['ok'])
                click.echo(tpl.format(msg, str(signature_path.name)))


if __name__ == '__main__':
    crossbow(obj={}, auto_envvar_prefix='CROSSBOW')
