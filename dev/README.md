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

The merge script uses the GitHub REST API. You must set a
`ARROW_GITHUB_API_TOKEN` environment variable to use a 
[Personal Access Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token). 
You need to add `workflow` scope to the Personal Access Token.

You can specify the 
[Personal Access Token](https://confluence.atlassian.com/enterprise/using-personal-access-tokens-1026032365.html)
of your JIRA account in the 
`APACHE_JIRA_TOKEN` environment variable.
If the variable is not set, the script will ask you for it.

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
url	https://api.github.com/apache/arrow/pulls/X
=== JIRA ARROW-#Y ===
Summary		Blah Blah Blah
Assignee	Name
Components	C++
Status		In Progress
URL		https://issues.apache.org/jira/browse/ARROW-#Y

Proceed with merging pull request #X? (y/n):
```

```
=== Pull Request #X ===
title	GH-#Y: [Component] Title
source	repo/branch
target	master
url	https://api.github.com/apache/arrow/pulls/X
=== GITHUB #Y ===
Summary		[Component] Title
Assignee	Name
Components	Python
Status		open
URL		https://github.com/apache/arrow/issues/Y

Proceed with merging pull request #X? (y/n): y
```

If this looks good, type y and hit enter.
```
Author 1: Name
Pull request #X merged!
Merge hash: #hash

Would you like to update the associated JIRA? (y/n): y
Enter comma-separated fix version(s) [11.0.0]:
```

```
Author 1: Name
Pull request #X merged!
Merge hash: #hash

Would you like to update the associated issue? (y/n): y
Enter fix version [11.0.0]:
```

You can just hit enter and the associated JIRA or GitHub issue
will be resolved with the current fix version.

```
Successfully resolved ARROW-#Y!
=== JIRA ARROW-#Y ===
Summary		Blah Blah Blah
Assignee	Name
Components	C++
Status		Resolved
URL		https://issues.apache.org/jira/browse/ARROW-#Y
```

```
Successfully resolved #Y!
=== GITHUB #Y ===
Summary		[Component] Title
Assignee	Name
Components	Python
Status		closed
URL		https://github.com/apache/arrow/issues/Y```
```

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
