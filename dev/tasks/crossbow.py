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
import yaml
import time
import click
import pygit2
import github3
import logging

from enum import Enum
from pathlib import Path
from textwrap import dedent
from jinja2 import Template
from setuptools_scm import get_version


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


class Repo(object):

    def __init__(self, repo_path):
        self.path = Path(repo_path).absolute()
        self.repo = pygit2.Repository(str(self.path))

    def __str__(self):
        tpl = dedent('''
            Repo: {remote}@{branch}
            Commit: {head}
        ''')
        return tpl.format(
            remote=self.remote.url,
            branch=self.branch.branch_name,
            head=self.head
        )

    def fetch(self):
        self.origin.fetch()

    @property
    def head(self):
        """Currently checked out commit's sha"""
        return self.repo.head.target

    @property
    def branch(self):
        """Currently checked out branch"""
        reference = self.repo.head.shorthand
        return self.repo.branches[reference]

    @property
    def remote(self):
        """Currently checked out branch's remote counterpart"""
        remote_name = self.branch.upstream.remote_name
        return self.repo.remotes[remote_name]

    @property
    def origin(self):
        return self.repo.remotes['origin']

    @property
    def email(self):
        return next(self.repo.config.get_multivar('user.email'))

    @property
    def signature(self):
        name = next(self.repo.config.get_multivar('user.name'))
        return pygit2.Signature(name, self.email, int(time.time()))

    def parse_user_repo(self):
        m = re.match('.*\/([^\/]+)\/([^\/\.]+)(\.git)?$', self.remote.url)
        user, repo = m.group(1), m.group(2)
        return user, repo


class Queue(Repo):

    def __init__(self, repo_path):
        super(Queue, self).__init__(repo_path)
        self._updated_refs = []

    def next_job_id(self, prefix):
        """Auto increments the branch's identifier based on the prefix"""
        pattern = re.compile(prefix + '-(\d+)')
        matches = list(filter(None, map(pattern.match, self.repo.branches)))
        if matches:
            latest = max(int(m.group(1)) for m in matches)
        else:
            latest = 0
        return '{}-{}'.format(prefix, latest + 1)

    def _create_branch(self, branch_name, files, parents=[], message=''):
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

    def _create_tag(self, tag_name, commit_id, message=''):
        tag_id = self.repo.create_tag(tag_name, commit_id,
                                      pygit2.GIT_OBJ_COMMIT, self.signature,
                                      message)

        # append to the pushable references
        self._updated_refs.append('refs/tags/{}'.format(tag_name))

        return self.repo[tag_id]

    def put(self, job, prefix='build'):
        assert isinstance(job, Job)
        assert job.branch is not None

        # create tasks' branches
        for task_name, task in job.tasks.items():
            branch = self._create_branch(task.branch, files=task.files())
            task.commit = str(branch.target)

        # create job's branch
        branch = self._create_branch(job.branch, files=job.files())
        self._create_tag(job.branch, branch.target)

        return branch

    def push(self, token):
        callbacks = GitRemoteCallbacks(token)
        self.origin.push(self._updated_refs, callbacks=callbacks)
        self.updated_refs = []


class Platform(Enum):
    # in alphabetical order
    LINUX = 0
    OSX = 1
    WIN = 2

    @property
    def ci(self):
        if self is self.WIN:
            return 'appveyor'
        else:
            return 'travis'

    @property
    def filename(self):
        if self.ci == 'appveyor':
            return 'appveyor.yml'
        else:
            return '.travis.yml'


class Task(object):

    def __init__(self, platform, template, commit=None, branch=None, **params):
        assert isinstance(platform, Platform)
        assert isinstance(template, Path)
        self.platform = platform
        self.template = template
        self.branch = branch
        self.commit = commit
        self.params = params

    def to_dict(self):
        return {'branch': self.branch,
                'commit': str(self.commit),
                'platform': self.platform.name,
                'template': str(self.template),
                'params': self.params}

    @classmethod
    def from_dict(cls, data):
        return Task(platform=Platform[data['platform'].upper()],
                    template=Path(data['template']),
                    commit=data.get('commit'),
                    branch=data.get('branch'),
                    **data.get('params', {}))

    def files(self):
        template = Template(self.template.read_text())
        rendered = template.render(**self.params)
        return {self.platform.filename: rendered}


class Job(object):

    def __init__(self, tasks, branch=None):
        assert all(isinstance(task, Task) for task in tasks.values())
        self.branch = branch
        self.tasks = tasks

    def to_dict(self):
        tasks = {name: task.to_dict() for name, task in self.tasks.items()}
        return {'branch': self.branch,
                'tasks': tasks}

    @classmethod
    def from_dict(cls, data):
        tasks = {name: Task.from_dict(task)
                 for name, task in data['tasks'].items()}
        return Job(tasks=tasks, branch=data.get('branch'))

    def files(self):
        return {'job.yml': yaml.dump(self.to_dict(), default_flow_style=False)}


# this should be the mailing list
MESSAGE_EMAIL = 'szucs.krisztian@gmail.com'

CWD = Path(__file__).absolute()

DEFAULT_CONFIG_PATH = CWD.parent / 'tasks.yml'
DEFAULT_ARROW_PATH = CWD.parents[2]
DEFAULT_QUEUE_PATH = CWD.parents[3] / 'crossbow'


@click.group()
def crossbow():
    pass


def github_token_validation_callback(ctx, param, value):
    if value is None:
        raise click.ClickException(
            'Could not determine GitHub token. Please set the '
            'CROSSBOW_GITHUB_TOKEN environment variable to a '
            'valid github access token or pass one to --github-token.'
        )
    return value


github_token = click.option(
    '--github-token',
    default=None,
    envvar='CROSSBOW_GITHUB_TOKEN',
    help='OAuth token for Github authentication',
    callback=github_token_validation_callback,
)


def config_path_validation_callback(ctx, param, value):
    with Path(value).open() as fp:
        config = yaml.load(fp)
    task_names = ctx.params['task_names']
    valid_tasks = set(config['tasks'].keys())
    invalid_tasks = {task for task in task_names if task not in valid_tasks}
    if invalid_tasks:
        raise click.ClickException(
            'Invalid task(s) {!r}. Must be one of {!r}'.format(
                invalid_tasks,
                valid_tasks
            )
        )
    return value


@crossbow.command()
@click.argument('task-names', nargs=-1, required=True)
@click.option('--job-prefix', default='build',
              help='Arbitrary prefix for branch names, e.g. nightly')
@click.option('--config-path', default=DEFAULT_CONFIG_PATH,
              type=click.Path(exists=True),
              callback=config_path_validation_callback,
              help='Task configuration yml. Defaults to tasks.yml')
@click.option('--dry-run/--push', default=False,
              help='Just display the rendered CI configurations without '
                   'submitting them')
@click.option('--arrow-path', default=DEFAULT_ARROW_PATH,
              help='Arrow\'s repository path. Defaults to the repository of '
                   'this script')
@click.option('--queue-path', default=DEFAULT_QUEUE_PATH,
              help='The repository path used for scheduling the tasks. '
                   'Defaults to crossbow directory placed next to arrow')
@github_token
def submit(task_names, job_prefix, config_path, dry_run, arrow_path,
           queue_path, github_token):
    target = Repo(arrow_path)
    queue = Queue(queue_path)

    logging.info(target)
    logging.info(queue)

    queue.fetch()

    version = get_version(arrow_path, local_scheme=lambda v: '')
    job_id = queue.next_job_id(prefix=job_prefix)

    variables = {
        # these should be renamed
        'PLAT': 'x86_64',
        'EMAIL': os.environ.get('CROSSBOW_EMAIL', target.email),
        'BUILD_TAG': job_id,
        'BUILD_REF': str(target.head),
        'ARROW_SHA': str(target.head),
        'ARROW_REPO': target.remote.url,
        'ARROW_BRANCH': target.branch.branch_name,
        'ARROW_VERSION': version,
        'PYARROW_VERSION': version,
    }

    with Path(config_path).open() as fp:
        config = yaml.load(fp)

    # create and filter tasks
    tasks = {name: Task.from_dict(task)
             for name, task in config['tasks'].items()}
    tasks = {name: tasks[name] for name in task_names}

    for task_name, task in tasks.items():
        task.branch = '{}-{}'.format(job_id, task_name)
        task.params.update(variables)

    # create job
    job = Job(tasks)
    job.branch = job_id

    yaml_format = yaml.dump(job.to_dict(), default_flow_style=False)
    click.echo(yaml_format.strip())

    if not dry_run:
        queue.put(job)
        queue.push(token=github_token)
        click.echo('Pushed job identifier is: `{}`'.format(job_id))


@crossbow.command()
@click.argument('job-name', required=True)
@click.option('--queue-path', default=DEFAULT_QUEUE_PATH,
              help='The repository path used for scheduling the tasks. '
                   'Defaults to crossbow directory placed next to arrow')
@github_token
def status(job_name, queue_path, github_token):
    queue = Queue(queue_path)
    username, reponame = queue.parse_user_repo()

    gh = github3.login(token=github_token)
    repo = gh.repository(username, reponame)
    content = repo.file_contents('job.yml', job_name)

    job = Job.from_dict(yaml.load(content.decoded))

    tpl = '[{:>7}] {:<24} {:<40}'
    header = tpl.format('status', 'branch', 'sha')
    click.echo(header)
    click.echo('-' * len(header))

    for name, task in job.tasks.items():
        commit = repo.commit(task.commit)
        status = commit.status()

        click.echo(tpl.format(status.state, task.branch, task.commit))


@crossbow.command()
@click.argument('job-name', required=True)
@click.option('--target-dir', default=DEFAULT_ARROW_PATH,
              help='Directory to download the build artifacts')
@click.option('--queue-path', default=DEFAULT_QUEUE_PATH,
              help='The repository path used for scheduling the tasks. '
                   'Defaults to crossbow directory placed next to arrow')
@github_token
def artifacts(job_name, target_dir, queue_path, github_token):
    queue = Queue(queue_path)
    username, reponame = queue.parse_user_repo()

    gh = github3.login(token=github_token)
    repo = gh.repository(username, reponame)
    release = repo.release_from_tag(job_name)

    for asset in release.assets():
        click.echo('Downloading asset {} ...'.format(asset.name))
        asset.download(target_dir / asset.name)


if __name__ == '__main__':
    crossbow(auto_envvar_prefix='CROSSBOW')
