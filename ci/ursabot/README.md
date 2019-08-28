<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Ursabot project configuration for Arrow

This directory contains all the relevant code required to reproduce the
ursabot CI setup hosted at https://ci.ursalabs.org.

## Arrow's build system

Arrow is used on a wide range of platforms, it has libraries in many languages,
and these all have multiple build options and compiler flags. We want to ensure
that patches don't break a build -- preferably before merging them -- and we
want to ensure that we don't regress.
While we use Travis-CI and Appveyor for some build testing, we can't test on
all platforms there, and there is some inefficiency because it is difficult to
express build stage dependencies and collect build artifacts (for example, of
the C++ library for use in Python/R/etc.).

Similarly, we want to prevent performance regressions and aim to prevent
merging patches that degrade performance, and in order to do this, we need to
run benchmarks. These benchmarks can't reliably be run on public CI
infrastructure because they need dedicated, consistent hardware in order to
return comparable results.

To ensure the quality of the software we ship, and to facilitate Arrow
developers and maintainers, we have a system of scripts and automation that
perform key functions. This document outlines the goals of our build and
automation system and describes how to work with it, as it is currently
implemented.

### Goals

- Prevent "breaking the build". For a given patch, run all tests and
  integrations, with all relevant build configurations and on all necessary
  platforms, that might be affected by the patch.
- Prevent performance regressions
- Ensure that the release process, including building binaries, also does not
  break
- For the strongest prevention, and to reduce human cognitive burden, these
  checks should be as automated as possible.
- For faster iteration while developing and debugging, it should also be
  possible to run these checks locally

### Constraints

- CI time: running every build configuration on every commit would increase
  delays in the patch review process.
- CI compute resources: our Travis-CI bandwidth is limited, as would be any
  bespoke build cluster we maintain. Some of this can be relaxed by throwing
  money at it, but there are diminishing returns (and budget constraints).
- Dev time: whatever we invest of ourselves in building this system takes away
  from time we’d spend adding value to the Arrow project itself. To the extent
  that investing in this system saves developer time elsewhere, it is
  worthwhile.

## Implementation

Currently We have a three-tiered system.

- Use Travis-CI and Appveyor for a set of language/build configurations that
  run on every commit and pull request on GitHub. The configuration files are
  maintained in the [arrow][arrow-repo] repository.
- [Crossbow][crossbow-readme] for binary
  packaging and nightly tests. Crossbow provides a command line interface to
  programatically trigger Travis and Appveyor builds by creating git branches
  in [ursalabs/crossbow][crossbow-repo] repository.
  It is maintained within [arrow][arrow-repo], for more see its
  [guide][crossbow-readme].
- [Ursabot][ursabot-repo] implemented on top of [buildbot][buildbot-docs] CI
  framework, hosted at [ci.ursalabs.org][ursabot-url] on Ursalabs' servers.
  It provides:

    - A set of builds testing the C++ implementation and Python bindings on
      multiple operating systems and architectures (most of these are executed
      automatically on each commit and pull request).
    - On-demand builds requested by commenting on a pull request.
    - On-demand benchmarks requested by commenting on a pull request, the
      benchmark results are reported as github comments. The recently developed
      [Archery][archery-readme] command-line tool is responsible for running
      and comparing the benchmarks.
    - Special builds for triggering other systems' builds, like
      [crossbow][crossbow-readme]'s packaging tasks by commenting on a pull
      request.
    - Locally reproducible builds via simple CLI commands.

## Driving Ursabot

Allowing PR reviewers to request additional checks on demand within the review
process makes it easier for us to apply extra scrutiny at review time while
also conserving CI bandwidth by using human expertise to know which checks are
needed.

### via Comments

Ursabot receives github events through a webhook. It listens on pull request
comments mentioning @ursabot. It follows the semantics of a command line
interface, to see the available commands add a comment on the pull request:
`@ursabot --help`.

The @ursabot GitHub user will respond or [react][github-reactions] that it has
started a build for you. Unfortunately, it does not currently report back
on the build status. The reporters are already implemented. They will be
enabled once the proper github integration permissions are set for the
[apache/arrow][arrow-repo] repository. Until that you have to search around the
[buildbot UI][ursabot-url] for it. The command parser is implemented in
[commands.py](commands.py).

Currently available commands:

  - `@ursabot build`: Triggers all the ursabot tests. These tests are run
    automatically, but this is a convinient way to force a re-build.
  - `@ursabot benchmark`: Triggers C++ benchmarks and sends back the results as
    a github comment and highlights the regressions.
  - `@ursabot crossbow test cpp-python`: Triggers the `cpp-python` test group
    defined in [test.yml][crossbow-tests] and responds with a URL pointing to
    submitted crossbow branches at the github UI showing the build statuses.
  - `@ursabot crossbow package -g wheel -g conda`: Triggers the `wheel` and
    `conda` crossbow packaging groups defined in [tasks.yml][crossbow-tasks].
  - `@ursabot crossbow package wheel-win-cp35m wheel-win-cp36m`: Triggers only
    two tasks passed explicitly.

Note that the commands won't trigger any builds if the commit message contains
a skip pattern, like `[skip ci]` or `[ci skip]`. In order to drive ursabot
the user must have either 'OWNER', 'MEMBER' or 'CONTRIBUTOR
[roles][github-author-association].

### via the Web UI

You can also initiate a build for a specific architecture/configuration in the
[buildbot UI][ursabot-url]. Navigate to [Builds > Builders][ursabot-builders],
select a builder, and click `Build apache/arrow` buttin at the top right. This
triggers the force schedulers where you can specify a branch and/or commit to
build. In the future specialized builders will have different fields to provide
the neccessary information.

## Locally running the buildmaster

The following commands are spinning up a long-running local buildmaster,
including as web interface and all of the configured services (authentication,
authorization, reporters, pollers etc.).

Firt master's database must be initialized:

```bash
$ ursabot -v upgrade-master
```

Start/stop/restart the master:

```bash
$ ursabot -v start|stop|restart
```

## Command line interface

Ursabot has multiple CLI commands and a lot of options, so try to read the
available options and arguments with passing `--help` before using a command.

Install it with:

```bash
$ pip install -e ursabot
$ ursabot --help
```

### Describe the configurations

Ursabot uses two kind of configs, `MasterConfig` and `ProjectConfig`.
`ProjectConfig` contains all of the required information to setup a project
like Arrow, whereas `MasterConfig` is used for configuring the buildmaster,
including the database, web interface and authentication services and the
projects it needs to handle. Formally: `MasterConfig(..., projects=[...])`.
For more see [how to configure ursabot][ursabot-config].

Describe the master configuration:

```bash
$ ursabot desc  # loads the master.cfg from the current directory
```

Describe the project configuration:

```bash
# for master configs with a single project
$ ursabot project desc
# for master configs with multiple projects
$ ursabot project -p apache/arrow desc
```

Arrow's [master.cfg][master.cfg] only contains a single project called arrow
and a master configuration usable for local testing (both via CLI and webUI).

### Validate the configuration

```bash
$ ursabot checkconfig
```

`ursabot` command loads `master.cfg` from the current directory by default, but
`--config` argument can be passed to explicitly define a configuration file.

```bash
$ ursabot -c arrow/master.cfg checkconfig
```

## Reproduce the builds locally

Testing `AMD64 Conda C++` builder on master:

```bash
$ ursabot project build 'AMD64 Conda C++'
```

Testing `AMD64 Conda C++` builder with github pull request number 140:

```bash
$ ursabot project build -pr 140 'AMD64 Conda C++'
```

Testing `AMD64 Conda C++` with local repository:

```bash
$ ursabot project build -s ~/Workspace/arrow:. 'AMD64 Conda C++'
```

Where `~/Workspace/arrow` is the path of the local Arrow repository and `.`
is the destination directory under the worker's build directory (in this case:
`/buildbot/AMD64_Conda_C__/.`)

Buildbot uses `properties` to lazily control various services and
functionalities. Properties are really flexible and the Arrow configuration
uses them extensively.
An example how to pass one or more properties for the build:

```bash
$ ursabot project build -p prop=value -p myprop=myvalue 'AMD64 Conda C++'
```

### Attach on failure

Ursabot supports debugging failed builds with attach attaching ordinary shells
to the still running workers - where the build has previously failed.

Use the `--attach-on-failure` or `-a` flags.

```bash
$ ursabot project build --attach-on-failure `AMD64 Conda C++`
```

## List and build docker images

Listing images:

```bash
$ ursabot docker list
```

Filtering images:

```bash
$ ursabot docker --arch amd64 list
$ ursabot docker --arch amd64 --variant conda list
$ ursabot docker --arch amd64 --variant conda --name cpp list
$ ursabot docker --arch amd64 --variant conda --name cpp --tag worker list
$ ursabot docker --arch amd64 --variant conda --name cpp --os debian-9 list
```

Building the images:

```bash
$ ursabot docker --arch amd64 --variant conda build
```

Pushing the images:

```bash
$ ursabot docker --arch amd64 --variant conda build --push
```

### Adding a new dependency to the docker images

Most of the dependency requirements are factored out to easily editable text
files under the [docker](docker) directory.

For plain (non-conda) docker images append the appropiate package to
[pkgs-alpine.txt](docker/pkgs-alpine.txt) and
[pkgs-ubuntu.txt](docker/pkgs-ubuntu.txt).

For conda images add the newly introduced dependency either to
[conda-linux.txt](docker/conda-linux.txt),
[conda-cpp.txt](docker/conda-cpp.txt),
[conda-python.txt](docker/conda-cpp.txt) or
[conda-sphinx.txt](docker/conda-sphinx.txt)
depending on which images should contain the new dependency.

In order to add a new pip dependency to the python images edit
[requirements.txt](docker/requirements.txt) or
[requirements-test.txt](docker/requirements-test.txt).

Then build and push the new images:

```bash
$ ursabot -v docker -dh tcp://amd64-host:2375 -a amd64 build -p
$ ursabot -v docker -dh tcp://arm64-host:2375 -a arm64v8 build -p
```

## Defining new builders

See ursabot's [documentation][ursabot-adding-builders].

## Defining new docker images

See ursabot's [documentation][ursabot-adding-images].


[ursabot-config]: https://github.com/ursa-labs/ursabot#configuring-ursabot
[ursabot-adding-builders]: https://github.com/ursa-labs/ursabot#adding-a-new-builders
[ursabot-adding-images]: https://github.com/ursa-labs/ursabot#define-docker-images
