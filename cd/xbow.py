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

head = repo[repo.head.target]  # master's head
tree = head.tree

print(tree)

remote = repo.remotes['origin']
refspecs = []

# create the build variants based on the arguments

# iterate over the variants
cwd = Path(__file__).parent


for config in cwd.glob('*.yml'):
    branch_name = config.stem
    refspecs.append('refs/heads/{}'.format(branch_name))


    if branch_name in repo.branches:
        # choose the appropiate branch
        branch = repo.branches[branch_name]
    else:
        # otherwise we initialize it
        branch = repo.branches.create(branch_name, head)

    # do the actual checkout
    repo.checkout(branch)

    # creating the tree we are going to push
    builder = repo.TreeBuilder(head.tree)

    # creating the file inside git object db
    content = config.read_text()
    blob_sha = repo.create_blob(content)
    blob = repo[blob_sha]

    if 'travis' in str(config):
        target = '.travis.yml'
    elif 'appveyor' in str(config):
        target = 'appveyor.yml'
    else:
        ValueError('raise sommething')

    builder.insert(target, blob_sha, pygit2.GIT_FILEMODE_BLOB)
    tree_sha = builder.write()

    print(tree_sha)

    parent_shas = [repo.head.target]  # point to the currently checked out branch's head
    commit_sha = repo.create_commit('HEAD', author, committer, message, tree_sha,
                                    [repo.head.target])

    commit = repo[commit_sha]
    print(commit)


def acquire_credentials_cb(url, username_from_url, allowed_types):
    print('credentials', url, username_from_url, allowed_types)
    token = '<top secret>'
    return UserPass(token, 'x-oauth-basic')


class GitRemoteCallbacks(pygit2.RemoteCallbacks):


    def push_update_reference(self, refname, message):
        print(refname, message)

    def update_tips(self, refname, old, new):
        print(refname, old, new)


callbacks = GitRemoteCallbacks(credentials=acquire_credentials_cb)

remote.push(refspecs, callbacks=callbacks)
