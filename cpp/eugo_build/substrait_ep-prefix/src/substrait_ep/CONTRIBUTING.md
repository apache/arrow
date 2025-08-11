# Contributing to Substrait

Welcome!

## Dependencies

There's no formal set of dependencies for Substrait, but here are some that are useful to have:

* [`buf`](https://docs.buf.build/installation) for easy generation of proto serialization/deserialization code
* [`protoc`](https://grpc.io/docs/protoc-installation/), used by `buf` and usable independent of `buf`
* A Python environment with [the website's `requirements.txt`](https://github.com/substrait-io/substrait/blob/main/site/requirements.txt) dependencies installed if you want to see changes to the website locally

## Commit Conventions

Substrait follows [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/) for commit message structure. You can use [`pre-commit`](https://pre-commit.com/) to check your messages for you, but note that you must install pre-commit using `pre-commit install --hook-type commit-msg` for this to work. CI will also lint your commit messages. Please also ensure that your PR title and initial comment together form a valid commit message; that will save us some work formatting the merge commit message when we merge your PR.

Examples of commit messages can be seen [here](https://www.conventionalcommits.org/en/v1.0.0/#examples).
