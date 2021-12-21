<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Arrow Developer Scripts

This directory contains scripts useful to developers when packaging,
testing, or committing to Arrow.

Merging a pull request requires being a committer on the project. In addition
you need to have linked your GitHub and ASF accounts on
https://gitbox.apache.org/setup/ to be able to push to GitHub as the main
remote.

NOTE: It may take some time (a few hours) between when you complete
the setup at GitBox, and when your GitHub account will be added as a
committer.

## How to merge a Pull request

Please don't merge PRs using the Github Web interface.  Instead, set up
your git clone such as to have a remote named ``apache`` pointing to the
official Arrow repository:
```
git remote add apache git@github.com:apache/arrow.git
```

and then run the following command:
```
./dev/merge_arrow_pr.sh
```

This creates a new Python virtual environment under `dev/.venv[PY_VERSION]`
and installs all the necessary dependencies to run the Arrow merge script.
After installed, it runs the merge script.

(we don't provide a wrapper script for Windows yet, so under Windows you'll
have to install Python dependencies yourself and then run `dev/merge_arrow_pr.py`
directly)

The merge script uses the GitHub REST API; if you encounter rate limit issues,
you may set a `ARROW_GITHUB_API_TOKEN` environment variable to use a Personal
Access Token.

You can specify the username and the password of your JIRA account in
`APACHE_JIRA_USERNAME` and `APACHE_JIRA_PASSWORD` environment variables.
If these aren't supplied, the script will ask you the values of them.

Note that the directory name of your Arrow git clone must be called `arrow`.

example output:
```
Which pull request would you like to merge? (e.g. 34):
```
Type the pull request number (from https://github.com/apache/arrow/pulls) and hit enter.
```
=== Pull Request #X ===
title	Blah Blah Blah
source	repo/branch
target	master
url	https://api.github.com/repos/apache/arrow/pulls/X

Proceed with merging pull request #3? (y/n):
```
If this looks good, type y and hit enter.
```
From git-wip-us.apache.org:/repos/asf/arrow.git
 * [new branch]      master     -> PR_TOOL_MERGE_PR_3_MASTER
Switched to branch 'PR_TOOL_MERGE_PR_3_MASTER'

Merge complete (local ref PR_TOOL_MERGE_PR_3_MASTER). Push to apache? (y/n):
```
A local branch with the merge has been created.
type y and hit enter to push it to apache master
```
Counting objects: 67, done.
Delta compression using up to 4 threads.
Compressing objects: 100% (26/26), done.
Writing objects: 100% (36/36), 5.32 KiB, done.
Total 36 (delta 17), reused 0 (delta 0)
To git-wip-us.apache.org:/repos/arrow-mr.git
   b767ac4..485658a  PR_TOOL_MERGE_PR_X_MASTER -> master
Restoring head pointer to b767ac4e
Note: checking out 'b767ac4e'.

You are in 'detached HEAD' state. You can look around, make experimental
changes and commit them, and you can discard any commits you make in this
state without impacting any branches by performing another checkout.

If you want to create a new branch to retain commits you create, you may
do so (now or later) by using -b with the checkout command again. Example:

  git checkout -b new_branch_name

HEAD is now at b767ac4... Update README.md
Deleting local branch PR_TOOL_MERGE_PR_X
Deleting local branch PR_TOOL_MERGE_PR_X_MASTER
Pull request #X merged!
Merge hash: 485658a5

Would you like to pick 485658a5 into another branch? (y/n):
```
For now just say n as we have 1 branch

## Verifying Release Candidates

We have provided a script to assist with verifying release candidates on Linux
and macOS:

```shell
bash dev/release/verify-release-candidate.sh 0.7.0 0
```

Read the script and check the notes in dev/release for information about system 
dependencies.

On Windows, we have a script that verifies C++ and Python (requires Visual
Studio 2015):

```
dev/release/verify-release-candidate.bat apache-arrow-0.7.0.tar.gz
```

# Integration testing

Build the following base image used by multiple tests:

```shell
docker build -t arrow_integration_xenial_base -f docker_common/Dockerfile.xenial.base .
```

## HDFS C++ / Python support

```shell
docker-compose build conda-cpp
docker-compose build conda-python
docker-compose build conda-python-hdfs
docker-compose run --rm conda-python-hdfs
```

## Apache Spark Integration Tests

Tests can be run to ensure that the current snapshot of Java and Python Arrow
works with Spark. This will run a docker image to build Arrow C++
and Python in a Conda environment, build and install Arrow Java to the local
Maven repository, build Spark with the new Arrow artifact, and run Arrow
related unit tests in Spark for Java and Python. Any errors will exit with a
non-zero value. To run, use the following command:

```shell
docker-compose build conda-cpp
docker-compose build conda-python
docker-compose build conda-python-spark
docker-compose run --rm conda-python-spark
```

If you already are building Spark, these commands will map your local Maven
repo to the image and save time by not having to download all dependencies.
Be aware, that docker write files as root, which can cause problems for maven
on the host.

```shell
docker-compose run --rm -v $HOME/.m2:/root/.m2 conda-python-spark
```

NOTE: If the Java API has breaking changes, a patched version of Spark might
need to be used to successfully build.
