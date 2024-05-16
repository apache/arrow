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

## How to Merge a Pull Request

Please don't merge PRs using the GitHub Web interface. Instead, run
the following command:

```bash
dev/merge_arrow_pr.sh
```

This creates a new Python virtual environment under `dev/.venv[PY_VERSION]`
and installs all the necessary dependencies to run the Arrow merge script.
After installed, it runs the merge script.

(We don't provide a wrapper script for Windows yet, so under Windows
you'll have to install Python dependencies yourself and then run
`dev/merge_arrow_pr.py` directly.)

The merge script uses the GitHub REST API. You must set a
`ARROW_GITHUB_API_TOKEN` environment variable to use a 
[Personal Access Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token). 
You need to add `workflow` scope to the Personal Access Token.

You can specify the 
[Personal Access Token](https://confluence.atlassian.com/enterprise/using-personal-access-tokens-1026032365.html)
of your JIRA account in the 
`APACHE_JIRA_TOKEN` environment variable.
If the variable is not set, the script will ask you for it.

Example output:

```text
Which pull request would you like to merge? (e.g. 34):
```

Type the pull request number (from
https://github.com/apache/arrow/pulls) and hit enter:

```text
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

If this looks good, type `y` and hit enter:

```text
Author 1: Name
Pull request #X merged!
Merge hash: #hash

Would you like to update the associated issue? (y/n): y
Enter fix version [11.0.0]:
```

You can just hit enter and the associated GitHub issue
will be resolved with the current fix version.

```text
Successfully resolved #Y!
=== GITHUB #Y ===
Summary		[Component] Title
Assignee	Name
Components	Python
Status		closed
URL		https://github.com/apache/arrow/issues/Y
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
