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

Merging a pull request requires being a committer on the project.

* How to merge a Pull request:
have an apache and apache-github remote setup
```
git remote add apache-github https://github.com/apache/arrow.git
git remote add apache https://git-wip-us.apache.org/repos/asf/arrow.git
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

## Creating API documentation

The generation of API documentation for `C++`, `C Glib`, `Python` 
and `Java` has been Dockerized. To generate the API documentation
run the following command:

```shell
bash dev/gen_apidocs.sh
```

This script assumes that the `parquet-cpp` Git repository 
https://github.com/apache/parquet-cpp has been cloned
besides the Arrow repository and a `dist` directory can be created
at the same level by the current user. Please note that most of the
software must be built in order to create the documentation, so this
step may take some time to run, especially the first time around as the
Docker container will also have to be built.

After successfully creating the API documentation the website can be
run locally to browse the API documentation from the top level
`Documentation` menu. To run the website issue the command:

```shell
bash dev/run_site.sh
```

The local URL for the website running inside the docker container
will be shown as `Server address:` in the output of the command.
To stop the server press `Ctrl-C` in that window.