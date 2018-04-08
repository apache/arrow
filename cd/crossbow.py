#!/usr/bin/env python

# TODO: create a docker container too for this sscript with dependencies pre-installed
# TODO: probably should turn off auto cancellation feature of travis
# TODO: dry-run / render feature

import re
import sys
import pygit2

from pathlib import Path
from datetime import datetime
from jinja2 import FileSystemLoader, Environment
from pygit2 import Signature, Repository, UserPass, RemoteCallbacks


CWD = Path(__file__).parent


def get_sha_version():
    arrow_path = Path(__file__).absolute().parents[1]
    repo = Repository(str(arrow_path))

    sha = repo.head.target
    version = '0.9.0'  # TODO

    return sha, version


def read_templates(pattern=None):
    for template in CWD.glob('*.yml'):
        if pattern is None or re.search(pattern, template.stem):
            yield template


def render_template(path, params):
    env = Environment(loader=FileSystemLoader(str(path.parent)))
    template = env.get_template(path.name)
    return template.render(**params)


def create_commit(repo, branch_name, filename, content):
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
    message = 'disco!'

    author = Signature('Crossbow', 'mailing@list.com', timestamp)
    committer = Signature(name, email, timestamp)

    reference = 'refs/heads/{}'.format(branch_name)
    commit_id = repo.create_commit(reference, author, committer, message,
                                   tree_id, [branch.target])

    return repo[commit_id]


class GitRemoteCallbacks(pygit2.RemoteCallbacks):

    def __init__(self):
        self.attempts = 0
        super(GitRemoteCallbacks, self).__init__()

    def push_update_reference(self, refname, message):
        print(refname, message)

    def update_tips(self, refname, old, new):
        print(refname, old, new)

    def credentials(self, url, username_from_url, allowed_types):
        # its a libgit2 bug, that it infinitly retries the authentication
        self.attempts += 1

        if self.attempts >= 5:
            # pygit2 doesn't propagate the exception properly
            msg = 'Wrong oauth personal access token'
            print(msg)
            raise ValueError(msg)

        if allowed_types & pygit2.credentials.GIT_CREDTYPE_USERPASS_PLAINTEXT:
            return UserPass(token, 'x-oauth-basic')
        else:
            return None


def push_branches(repo, branches, token):
    callbacks = GitRemoteCallbacks()

    remote = repo.remotes['origin']
    refs = ['refs/heads/{}'.format(branch) for branch in branches]
    return remote.push(refs, callbacks=callbacks)


ARROW_REPO = 'https://github.com/kszucs/arrow'
ARROW_BRANCH = 'cd'
EMAIL = 'szucs.krisztian@gmail.com'
PLAT = 'x86_64'
BUILD_REF, PYARROW_VERSION = get_sha_version()


if len(sys.argv) > 1:
    pattern = sys.argv[1]
else:
    pattern = None


dry_run = False
for arg in sys.argv:
    if arg == '--dry-run':
        dry_run = True


repo = Repository('/Users/krisz/Workspace/crossbow')
branches = []

for path in read_templates(pattern):
    # TODO create the build variants based on the arguments
    branch_name = path.stem
    branches.append(branch_name)

    params = dict(
        ARROW_REPO=ARROW_REPO,
        ARROW_BRANCH=ARROW_BRANCH,
        PLAT=PLAT,
        BUILD_REF=BUILD_REF,
        PYARROW_VERSION=PYARROW_VERSION,
        EMAIL=EMAIL
    )
    content = render_template(path, params)

    if branch_name.startswith('travis'):
        filename = '.travis.yml'
    elif branch_name.startswith('appveyor'):
        filename = 'appveyor.yml'
    else:
        ValueError('raise something')

    if dry_run:
        print(content)
    else:
        create_commit(repo, branch_name, filename, content)


token = '<top secret>'

if not dry_run:
    push_branches(repo, branches, token=token)
