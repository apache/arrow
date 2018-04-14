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
import click
import pygit2
# later we can remove these dependencies if required

from pathlib import Path
from datetime import datetime
from jinja2 import FileSystemLoader, Environment
from setuptools_scm import get_version


CWD = Path(__file__).parent


def list_templates(pattern):
    for template in CWD.glob('*.yml'):
        if pattern is None or re.search(pattern, template.stem):
            yield template


def render_template(path, params):
    env = Environment(loader=FileSystemLoader(str(path.parent)))
    template = env.get_template(path.name)
    return template.render(**params)


def create_commit(repo, branch_name, filename, content, message):
    master = repo.branches['master']
    master_head = repo[master.target]

    try:
        branch = repo.branches[branch_name]
    except KeyError:
        branch = repo.branches.create(branch_name, master_head)

    # creating the tree we are going to push based on master's tree
    builder = repo.TreeBuilder(master_head.tree)

    # insert the file and creating the new filetree
    blob_id = repo.create_blob(content)
    blob = repo[blob_id]

    builder.insert(filename, blob_id, pygit2.GIT_FILEMODE_BLOB)
    tree_id = builder.write()

    # creating the new commit
    timestamp = int((datetime.now() - datetime(1970, 1, 1)).total_seconds())
    name = next(repo.config.get_multivar('user.name'))
    email = next(repo.config.get_multivar('user.email'))

    author = pygit2.Signature('crossbow', 'mailing@list.com', timestamp)
    committer = pygit2.Signature(name, email, timestamp)

    reference = 'refs/heads/{}'.format(branch_name)
    commit_id = repo.create_commit(reference, author, committer, message,
                                   tree_id, [branch.target])

    return branch


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


def push_branches(repo, branches, token):
    callbacks = GitRemoteCallbacks(token)

    remote = repo.remotes['origin']
    refs = [branch.name for branch in branches]

    return remote.push(refs, callbacks=callbacks)


# this should be the mailing list
EMAIL = 'szucs.krisztian@gmail.com'


@click.command()
@click.argument('pattern', required=False)
@click.option('--dry-run/--push', default=False,
              help='Just display the rendered CI configurations without '
                   'submitting them')
@click.option('--queue-repo', default=None,
              help='The repository path or url used for scheduling the builds.'
                   'Defaults to ../crossbow')
@click.option('--github-token', default=False, envvar='CROSSBOW_GITHUB_TOKEN',
              help='Oauth token for Github authentication')
def build(pattern, dry_run, queue_repo, github_token):
    # initialize a repo object to interact with arrow's git data
    path = Path(__file__).absolute().parents[1]
    repo = pygit2.Repository(str(path))

    # get the currently checked out commit's sha and generate
    # the corresponding version number (same as pyarrow's)
    sha = repo.head.target
    version = get_version(path)

    origin = repo.remotes['origin']
    branch = repo.branches[repo.head.shorthand]
    ghrepo = origin.url

    click.echo('Repository: {}@{}'.format(origin.url, branch.branch_name))
    click.echo('Commit SHA: {}'.format(sha))
    click.echo('Version: {}'.format(version))

    # initializing or cloning the scheduling repository
    if queue_repo is None:
        queue_repo = '/'.join(origin.url.split('/')[:-1] + ['crossbow'])

    try:
        queue_repo = pygit2.Repository(queue_repo)
    except pygit2.GitError:
        queue_repo = pygit2.Repository(str(path.parent / 'crossbow'))

    # reading the build templates and constructing the CI configurations
    updated_branches = []
    for template_path in list_templates(pattern):
        filename = template_path.name
        if filename.startswith('travis'):
            target_filename = '.travis.yml'
        elif filename.startswith('appveyor'):
            target_filename = 'appveyor.yml'
        else:
            ValueError('Unkown CI service provider for {}'.format(filename))

        params = {
            'PLAT': 'x86_64',
            'EMAIL': EMAIL,
            'BUILD_REF': sha,
            'ARROW_REPO': origin.url,
            'ARROW_BRANCH': branch.branch_name,
            'PYARROW_VERSION': version,
        }

        content = render_template(template_path, params)
        message = '[BUILD] {} of {}@{}'.format(version, origin.url,
                                               branch.branch_name)

        if dry_run:
            click.echo('\n')
            click.echo('-' * 79)
            click.echo(content)
        else:
            target_branch_name = template_path.stem
            updated_branch = create_commit(queue_repo, target_branch_name,
                                           target_filename, content, message)
            updated_branches.append(updated_branch)

    if not dry_run:
        push_branches(queue_repo, updated_branches, token=github_token)

    click.echo('Pushed branches:')
    for updated_branch in updated_branches:
        click.echo(' - {}'.format(updated_branch.shorthand))


if __name__ == '__main__':
    build()
