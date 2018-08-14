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

* How to merge a Pull request:
have an apache and apache-github remote setup
```
git remote add apache-github https://github.com/apache/arrow.git
git remote add apache git@github.com:apache/arrow.git
```
run the following command
```
dev/merge_arrow_pr.py
```

Note:
* The directory name of your Arrow git clone must be called arrow
* Without jira-python installed you'll have to close the JIRA manually

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

We have provided a script to assist with verifying release candidates:

```shell
bash dev/release/verify-release-candidate.sh 0.7.0 0
```

Currently this only works on Linux (patches to expand to macOS welcome!). Read
the script for information about system dependencies.

On Windows, we have a script that verifies C++ and Python (requires Visual
Studio 2015):

```
dev/release/verify-release-candidate.bat apache-arrow-0.7.0.tar.gz
```

### Verifying the JavaScript release

For JavaScript-specific releases, use a different verification script:

```shell
bash dev/release/js-verify-release-candidate.sh 0.7.0 0
```
# Integration testing

Build the following base image used by multiple tests:

```shell
docker build -t arrow_integration_xenial_base -f docker_common/Dockerfile.xenial.base .
```

## HDFS C++ / Python support

```shell
run_docker_compose.sh hdfs_integration
```

## Apache Spark Integration Tests

Tests can be run to ensure that the current snapshot of Java and Python Arrow
works with Spark. This will run a docker image to build Arrow C++
and Python in a Conda environment, build and install Arrow Java to the local
Maven repositiory, build Spark with the new Arrow artifact, and run Arrow
related unit tests in Spark for Java and Python. Any errors will exit with a
non-zero value. To run, use the following command:

```shell
./run_docker_compose.sh spark_integration

```

Alternatively, you can build and run the Docker images seperately. If you
already are building Spark, these commands will map your local Maven repo
to the image and save time by not having to download all dependencies. These
should be run in a directory one level up from your Arrow repository:

```shell
docker build -f arrow/dev/spark_integration/Dockerfile -t spark-arrow .
docker run -v $HOME/.m2:/root/.m2 spark-arrow
```

NOTE: If the Java API has breaking changes, a patched version of Spark might
need to be used to successfully build.
