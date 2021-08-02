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

Packaging and Testing with Crossbow
===================================

The content of ``arrow/dev/tasks`` directory aims for automating the process of
Arrow packaging and integration testing.

Packages:
  - C++ and Python `conda-forge packages`_ for Linux, Mac and Windows
  - Python `Wheels`_ for Linux, Mac and Windows
  - C++ and GLib `Linux packages`_ for multiple distributions
  - Java for Gandiva

Integration tests:
  - Various docker tests
  - Pandas
  - Dask
  - Turbodbc
  - HDFS
  - Spark

Architecture
------------

Executors
~~~~~~~~~

Individual jobs are executed on public CI services, currently:

- Linux: TravisCI, CircleCI, Azure Pipelines
- Mac: TravisCI, Azure Pipelines
- Windows: AppVeyor, Azure Pipelines

Queue
~~~~~

Because of the nature of how the CI services work, the scheduling of
jobs happens through an additional git repository, which acts like a job
queue for the tasks. Anyone can host a ``queue`` repository which is usually
called as ``crossbow``.

A job is a git commit on a particular git branch, containing only the required
configuration file to run the requested build (like ``.travis.yml``,
``appveyor.yml`` or ``azure-pipelines.yml``).

Scheduler
~~~~~~~~~

Crossbow handles version generation, task rendering and
submission. The tasks are defined in ``tasks.yml``.

Install
-------

The following guide depends on GitHub, but theoretically any git
server can be used.

If you are not using the `ursacomputing/crossbow <https://github.com/ursacomputing/crossbow>`_
repository, you will need to complete the first two steps, otherwise procede
to step 3:

1. `Create the queue repository`_

2. Enable `TravisCI`_, `Appveyor`_, `Azure Pipelines`_ and `CircleCI`_
   integrations on for the newly created queue repository.

   -  turn off Travis’ `auto cancellation`_ feature on branches

3. Clone either ursacomputing/crossbow if you are using that, or the newly
   created repository next to the arrow repository:

   By default the scripts looks for ``crossbow`` next to arrow repository, but
   this can configured through command line arguments.

   .. code:: bash

      git clone https://github.com/<user>/crossbow crossbow

   **Important note:** Crossbow only supports GitHub token based
   authentication. Although it overwrites the repository urls provided with ssh
   protocol, it's advisable to use the HTTPS repository URLs.

4. `Create a Personal Access Token`_ with ``repo`` and ``workflow`` permissions (other
   permissions are not needed)

5. Locally export the token as an environment variable:

   .. code:: bash

      export CROSSBOW_GITHUB_TOKEN=<token>

   or pass as an argument to the CLI script ``--github-token``

6. Export the previously created GitHub token on both CI services:

   Use ``CROSSBOW_GITHUB_TOKEN`` encrypted environment variable. You can
   set them at the following URLs, where ``ghuser`` is the GitHub
   username and ``ghrepo`` is the GitHub repository name (typically
   ``crossbow``):

   -  TravisCI: ``https://travis-ci.org/<ghuser>/<ghrepo>/settings``
   -  Appveyor:
      ``https://ci.appveyor.com/project/<ghuser>/<ghrepo>/settings/environment``
   -  CircleCI:
      ``https://circleci.com/gh/<ghuser>/<ghrepo>/edit#env-vars``

   On Appveyor check the ``skip branches without appveyor.yml`` checkbox
   on the web UI under crossbow repository’s settings.

7. Install Python (minimum supported version is 3.6):

   Miniconda is preferred, see installation instructions:
   https://conda.io/docs/user-guide/install/index.html

8. Install the archery toolset containing crossbow itself:

   .. code:: bash

      pip install -e "arrow/dev/archery[crossbow]"

9. Try running it:

   .. code:: bash

      $ archery crossbow --help

Usage
-----

The script does the following:

1. Detects the current repository, thus supports forks. The following
   snippet will build kszucs’s fork instead of the upstream apache/arrow
   repository.

   .. code:: bash

      $ git clone https://github.com/kszucs/arrow
      $ git clone https://github.com/kszucs/crossbow

      $ cd arrow/dev/tasks
      $ archery crossbow submit --help  # show the available options
      $ archery crossbow submit conda-win conda-linux conda-osx

2. Gets the HEAD commit of the currently checked out branch and
   generates the version number based on `setuptools_scm`_. So to build
   a particular branch check out before running the script:

   .. code:: bash

      git checkout ARROW-<ticket number>
      archery crossbow submit --dry-run conda-linux conda-osx

   Note that the arrow branch must be pushed beforehand, because the
   script will clone the selected branch.

3. Reads and renders the required build configurations with the
   parameters substituted.

4. Create a branch per task, prefixed with the job id. For example to
   build conda recipes on linux it will create a new branch:
   ``crossbow@build-<id>-conda-linux``.

5. Pushes the modified branches to GitHub which triggers the builds. For
   authentication it uses GitHub OAuth tokens described in the install
   section.

Query the build status
~~~~~~~~~~~~~~~~~~~~~~

Build id (which has a corresponding branch in the queue repository) is returned
by the ``submit`` command.

.. code:: bash

   archery crossbow status <build id / branch name>

Download the build artifacts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

   archery crossbow artifacts <build id / branch name>

Examples
~~~~~~~~

Submit command accepts a list of task names and/or a list of task-group names
to select which tasks to build.

Run multiple builds:

.. code:: bash

   $ archery crossbow submit debian-stretch conda-linux-gcc-py37-r40
   Repository: https://github.com/kszucs/arrow@tasks
   Commit SHA: 810a718836bb3a8cefc053055600bdcc440e6702
   Version: 0.9.1.dev48+g810a7188.d20180414
   Pushed branches:
    - debian-stretch
    - conda-linux-gcc-py37-r40

Just render without applying or committing the changes:

.. code:: bash

   $ archery crossbow submit --dry-run task_name

Run only ``conda`` package builds and a Linux one:

.. code:: bash

   $ archery crossbow submit --group conda centos-7

Run ``wheel`` builds:

.. code:: bash

   $ archery crossbow submit --group wheel

There are multiple task groups in the ``tasks.yml`` like docker, integration
and cpp-python for running docker based tests.

``archery crossbow submit`` supports multiple options and arguments, for more
see its help page:

.. code:: bash

  $ archery crossbow submit --help


.. _conda-forge packages: conda-recipes
.. _Wheels: python-wheels
.. _Linux packages: linux-packages
.. _Create the queue repository: https://help.github.com/articles/creating-a-new-repository
.. _TravisCI: https://travis-ci.org/getting_started
.. _Appveyor: https://www.appveyor.com/docs/
.. _CircleCI: https://circleci.com/docs/2.0/getting-started/
.. _Azure Pipelines: https://docs.microsoft.com/en-us/azure/devops/pipelines/get-started/pipelines-sign-up
.. _auto cancellation: https://docs.travis-ci.com/user/customizing-the-build/#Building-only-the-latest-commit
.. _Create a Personal Access Token: https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/
.. _setuptools_scm: https://pypi.python.org/pypi/setuptools_scm
