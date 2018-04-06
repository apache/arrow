#!/usr/bin/env python

# we might weaken the dependencies later, but a release manager probably
# can have these installed
import pygit2

from pygit2 import Signature, Repository, UserPass, RemoteCallbacks
from pathlib import Path

# we should handle this later
signature = Signature('John Doe', 'jdoe@example.com', 12346, 0)
author = committer = signature

message = 'disco!'


# --remote arg when it will clone otherwise locally
repo = Repository('/Users/krisz/Workspace/crossbow')

master = repo.branches['master']
head = repo[master.target]


remote = repo.remotes['origin']
refspecs = []

# create the build variants based on the arguments

# iterate over the variants
cwd = Path(__file__).parent


for config in cwd.glob('*.yml'):
    branch_name = config.stem
    reference = 'refs/heads/{}'.format(branch_name)
    refspecs.append(reference)


    if branch_name in repo.branches:
        # choose the appropiate branch
        branch = repo.branches[branch_name]
    else:
        # otherwise we initialize it
        branch = repo.branches.create(branch_name, head)

    # # do the actual checkout
    # repo.checkout(branch)

    # creating the tree we are going to push
    builder = repo.TreeBuilder(head.tree)

    print('creating blob')

    # creating the file inside git object db
    content = config.read_text()
    blob_id = repo.create_blob(content)
    blob = repo[blob_id]

    if 'travis' in str(config):
        target = '.travis.yml'
    elif 'appveyor' in str(config):
        target = 'appveyor.yml'
    else:
        ValueError('raise sommething')

    print('adding file {}'.format(target))
    builder.insert(target, blob_id, pygit2.GIT_FILEMODE_BLOB)
    tree_id = builder.write()


    commit_id = repo.create_commit(reference, author, committer, message, tree_id,
                                   [branch.target])

    commit = repo[commit_id]
    print(commit)


def acquire_credentials_cb(url, username_from_url, allowed_types):
    print('credentials', url, username_from_url, allowed_types)
    # its a libgit2 bug, that it infinitly retries the authentication
    token = '5f3ea541b44a23150d95eaa9b87a65edabf91ff3'
    return UserPass(token, 'x-oauth-basic')


class GitRemoteCallbacks(pygit2.RemoteCallbacks):


    def push_update_reference(self, refname, message):
        print(refname, message)

    def update_tips(self, refname, old, new):
        print(refname, old, new)


callbacks = GitRemoteCallbacks(credentials=acquire_credentials_cb)

remote.push(refspecs, callbacks=callbacks)
