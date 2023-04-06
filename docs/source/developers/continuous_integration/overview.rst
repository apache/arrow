.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. _continuous-integration:

Continuous Integration
======================

Continuous Integration for Arrow is fairly complex as it needs to run across different combinations of package managers, compilers, versions of multiple sofware libraries, operating systems, and other potential sources of variation.  In this article, we will give an overview of its main components and the relevant files and directories.

Some files central to Arrow CI are:

- ``docker-compose.yml`` - here we define docker services which can be configured using either enviroment variables, or the default values for these variables.
- ``.env`` - here we define default values to configure the services in ``docker-compose.yml``
- ``.travis.yml`` - here we define workflows which run on Travis
- ``appveyor.yml`` - here we define workflows that run on Appveyor

We use :ref:`Docker<docker-builds>` in order to have portable and reproducible Linux builds, as well as running Windows builds in Windows containers.  We use :ref:`Archery<Archery>` and :ref:`Crossbow<Crossbow>` to help co-ordinate the various CI tasks.

One thing to note is that some of the services defined in ``docker-compose.yml`` are interdependent.  When running services locally, you must either manually build its dependencies first, or build it via the use of ``archery run ...`` which automatically finds and builds dependencies.

There are numerous important directories in the Arrow project which relate to CI:

- ``.github/worflows`` - workflows that are run via GitHub actions and are triggered by things like pull requests being submitted or merged
- ``dev/tasks`` - containing extended jobs triggered/submitted via ``archery crossbow submit ...``, typically nightly builds or relating to the release process
- ``ci/`` - containing scripts, dockerfiles, and any supplemental files, e.g. patch files, conda environment files, vcpkg triplet files.

Instead of thinking about Arrow CI in terms of files and folders, it may be conceptually simpler to instead divide it into 2 main categories:

- **action-triggered builds**: CI jobs which are triggered based on specific actions on GitHub (pull requests opened, pull requests merged, etc)
- **extended builds**: manually triggered with many being run on a nightly basis

Action-triggered builds
-----------------------

The ``.yml`` files in ``.github/worflows`` are workflows which are run on GitHub in response to specific actions.  The majority of workflows in this directory are Arrow implementation-specific and are run when changes are made which affect code relevant to that language's implementation, but other workflows worth noting are:

- ``archery.yml`` - if changes are made to the Archery tool or tasks which it runs, this workflow runs the necessary validation checks
- ``comment_bot.yml`` - triggers certain actions by listening on github pull request comments for the following strings:

  - ``@github-actions crossbow submit ...`` - runs the specified Crossbow command
  - ``@github-actions autotune`` - runs a number of stylers/formatters, builds some of the docs, and commits the results
  - ``@github-actions rebase`` - rebases the PR onto the main branch
- ``dev.yml`` - runs any time there is activity on a PR, or a PR is merged; it runs the linter and tests that the PR can be merged
- ``dev_pr.yml`` - runs any time a PR is opened or updated; checks the formatting of the PR title, adds assignee to the appropriate GitHub issue if needed (or adds a comment requesting the user to include the issue id in the title), and adds any relevant GitHub labels

There are two other files which define action-triggered builds:

- ``.travis.yml`` - runs on all commits and is used to test on architectures such as ARM and S390x
- ``appveyor.yml`` - runs on commits related to Python or C++

Extended builds
-----------------------

Crossbow is a subcomponent of Archery and can be used to manually trigger builds.  The tasks which can be run on Crossbow can be found in the ``dev/tasks`` directory.  This directory contains:

- the file ``dev/tasks/tasks.yml`` containing the configuration for various tasks which can be run via Crossbow
- subdirectories containing different task templates (specified using `jinja2 syntax <https://jinja.palletsprojects.com/>`_), divided roughly by language or package management system.

Most of these tasks are run as part of the nightly builds, though they can also be triggered manually by add a comment to a PR which begins with ``@github-actions crossbow submit`` followed by the name of the task to be run.

For convenience purpose, the tasks in ``dev/tasks/tasks.yml`` are defined in groups, which makes it simpler for multiple tasks to be submitted to Crossbow at once.  The task definitions here contain information about which service defined in ``docker-compose.yml`` to run, the CI service to run the task on, and which template file to use as the basis for that task.
