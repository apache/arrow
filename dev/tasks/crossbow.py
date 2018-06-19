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

import re
import sys
import yaml
import time
import click
import pygit2
import logging

from enum import Enum
from pathlib import Path
from textwrap import dedent
from jinja2 import Template
from setuptools_scm import get_version
from setuptools_scm.version import simplified_semver_version, meta


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s Crossbow %(message)s",
    datefmt="%H:%M:%S",
    stream=click.get_text_stream('stdout')
)


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


class Target(object):

    def __init__(self, repo_path, template_directory=None):
        self.path = Path(repo_path).absolute()

        # relative to repository's path
        if template_directory is None:
            self.templates = self.path
        else:
            self.templates = self.path / template_directory

        # initialize a repo object to interact with arrow's git data
        self.repo = pygit2.Repository(str(self.path))

        msg = dedent('''
            Repository: {remote}@{branch}
            Commit SHA: {sha}
            Version: {version}
        ''')
        logging.info(msg.format(
            remote=self.current_remote.url,
            branch=self.current_branch.branch_name,
            sha=self.sha,
            version=self.version
        ))

    @property
    def sha(self):
        """Currently checked out commit's sha"""
        return self.repo.head.target

    @property
    def version(self):
        """Generate version number based on version control history"""
        # TODO(kszucs) use self.repo.describe() instead
        return get_version(self.path)

    @property
    def current_remote(self):
        remote_name = self.current_branch.upstream.remote_name
        return self.repo.remotes[remote_name]

    @property
    def current_branch(self):
        reference = self.repo.head.shorthand
        return self.repo.branches[reference]

    @property
    def description(self):
        return '[BUILD] {} of {}@{}'.format(self.version,
                                            self.current_remote.url,
                                            self.current_branch.branch_name)


class Build(object):

    def __init__(self, target, name, platform, template, **params):
        assert isinstance(target, Target)
        assert isinstance(platform, Platform)

        self.name = name
        self.target = target
        self.platform = platform
        self.template = template
        self.params = params

    def render(self):
        path = Path(self.template)
        template = Template(path.read_text())
        return template.render(**self.params)

    def config_files(self):
        return {self.platform.filename: self.render()}

    @property
    def branch(self):
        return self.name

    @property
    def description(self):
        return self.target.description


class Queue(object):

    def __init__(self, repo_path):
        self.path = Path(repo_path).absolute()
        self.repo = pygit2.Repository(str(self.path))
        self.updated_branches = []

    def _get_parent_commit(self):
        """Currently this always returns the HEAD of master"""
        master = self.repo.branches['master']
        return self.repo[master.target]

    def _get_or_create_branch(self, name):
        try:
            return self.repo.branches[name]
        except KeyError:
            parent = self._get_parent_commit()
            return self.repo.branches.create(name, parent)

    def _create_tree(self, files):
        parent = self._get_parent_commit()

        # creating the tree we are going to push based on master's tree
        builder = self.repo.TreeBuilder(parent.tree)

        for filename, content in files.items():
            # insert the file and creating the new filetree
            blob_id = self.repo.create_blob(content)
            builder.insert(filename, blob_id, pygit2.GIT_FILEMODE_BLOB)

        tree_id = builder.write()
        return tree_id

    def put(self, build):
        assert isinstance(build, Build)

        branch = self._get_or_create_branch(build.branch)
        tree_id = self._create_tree(build.config_files())

        # creating the new commit
        timestamp = int(time.time())

        name = next(self.repo.config.get_multivar('user.name'))
        email = next(self.repo.config.get_multivar('user.email'))

        author = pygit2.Signature('crossbow', 'mailing@list.com',
                                  int(timestamp))
        committer = pygit2.Signature(name, email, int(timestamp))
        message = build.description

        reference = 'refs/heads/{}'.format(branch.branch_name)
        commit_id = self.repo.create_commit(reference, author, committer,
                                            message, tree_id, [branch.target])
        logging.info('{} created on {}'.format(
            commit_id, branch.branch_name))

        self.updated_branches.append(branch)

    def push(self, token):
        callbacks = GitRemoteCallbacks(token)

        remote = self.repo.remotes['origin']
        refs = [branch.name for branch in self.updated_branches]
        shorthands = [b.shorthand for b in self.updated_branches]

        remote.push(refs, callbacks=callbacks)
        self.updated_branches = []

        logging.info('\n - '.join(['\nUpdated branches:'] + shorthands))


# this should be the mailing list
MESSAGE_EMAIL = 'szucs.krisztian@gmail.com'


@click.command()
@click.argument('task-regex', required=False)
@click.option('--config', help='Task configuration yml. Defaults to tasks.yml')
@click.option('--dry-run/--push', default=False,
              help='Just display the rendered CI configurations without '
                   'submitting them')
@click.option('--arrow-repo', default=None,
              help='Arrow\'s repository path. Defaults to the repository of '
                   'this script')
@click.option('--queue-repo', default=None,
              help='The repository path used for scheduling the tasks. '
                   'Defaults to crossbow directory placed next to arrow')
@click.option('--github-token', default=False,
              help='Oauth token for Github authentication')
def build(task_regex, config, dry_run, arrow_repo, queue_repo, github_token):
    if config is None:
        config = Path(__file__).absolute().parent / 'tasks.yml'
    else:
        config = Path(config)

    if arrow_repo is None:
        arrow_repo = Path(__file__).absolute().parents[2]
    else:
        arrow_repo = Path(arrow_repo)

    if queue_repo is None:
        queue_repo = arrow_repo.parent / 'crossbow'
    else:
        queue_repo = Path(queue_repo)

    arrow = Target(arrow_repo, template_directory='cd')
    queue = Queue(queue_repo)

    variables = {
        # these should be renamed
        'PLAT': 'x86_64',
        'EMAIL': MESSAGE_EMAIL,
        'BUILD_REF': arrow.sha,
        'ARROW_SHA': arrow.sha,
        'ARROW_REPO': arrow.current_remote.url,
        'ARROW_BRANCH': arrow.current_branch.branch_name,
        'ARROW_VERSION': arrow.version,
        'PYARROW_VERSION': arrow.version,
    }

    with config.open() as fp:
        tasks = yaml.load(fp)['tasks']

    for task in tasks:
        name = task['name']
        template = config.parent / task['template']
        platform = Platform[task['platform'].upper()]
        params = task.get('params') or {}
        params.update(variables)

        build = Build(arrow, name=name, platform=platform, template=template,
                      **params)

        # Regex pattern the task name is matched against
        if task_regex is None or re.search(task_regex, build.name):
            if dry_run:
                logging.info('{}\n\n{}'.format(build.name, build.render()))
            else:
                queue.put(build)  # create the commit

    if not dry_run:
        # push the changed branches
        queue.push(token=github_token)


if __name__ == '__main__':
    build(auto_envvar_prefix='CROSSBOW')
