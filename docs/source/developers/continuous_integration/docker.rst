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

.. _docker-builds:

Running Docker Builds
=====================

Most of our Linux based Continuous Integration tasks are decoupled from public
CI services using `Docker <https://docs.docker.com/>`_ and
`docker-compose <https://docs.docker.com/compose/>`_.  Keeping the CI configuration
minimal makes local reproducibility possible.

Usage
-----

There are multiple ways to execute the docker based builds.
The recommended way is to use the :ref:`Archery <archery>` tool:

Examples
~~~~~~~~

**List the available images:**

.. code:: bash

    archery docker images

**Execute a build:**

.. code:: bash

    archery docker run conda-python

Archery calls the following docker-compose commands:

.. code:: bash

    docker-compose pull --ignore-pull-failures conda-cpp
    docker-compose pull --ignore-pull-failures conda-python
    docker-compose build conda-cpp
    docker-compose build conda-python
    docker-compose run --rm conda-python

**Show the docker-compose commands instead of executing them:**

.. code:: bash

    archery docker run --dry-run conda-python

**To disable the image pulling:**

.. code:: bash

    archery docker run --no-cache conda-python

Which translates to:

.. code:: bash

    docker-compose build --no-cache conda-cpp
    docker-compose build --no-cache conda-python
    docker-compose run --rm conda-python

**To disable the cache only for the leaf image:**

Useful to force building the development version of a dependency.
In case of the example below the command builds the
``conda-cpp > conda-python > conda-python-pandas`` branch of the image tree
where the leaf image is ``conda-python-pandas``.

.. code:: bash

    PANDAS=upstream_devel archery docker run --no-leaf-cache conda-python-pandas

Which translates to:

.. code:: bash

    export PANDAS=upstream_devel
    docker-compose pull --ignore-pull-failures conda-cpp
    docker-compose pull --ignore-pull-failures conda-python
    docker-compose build conda-cpp
    docker-compose build conda-python
    docker-compose build --no-cache conda-python-pandas
    docker-compose run --rm conda-python-pandas

Note that it doesn't pull the conda-python-pandas image and disable the cache
when building it.

``PANDAS`` is a :ref:`build parameter <docker-build-parameters>`, see the
defaults in the ``.env`` file.

**To entirely skip building the image:**

The layer-caching mechanism of docker-compose can be less reliable than
docker's, depending on the version, the ``cache_from`` build entry, and the
backend used (docker-py, docker-cli, docker-cli and buildkit). This can lead to
different layer hashes - even when executing the same build command
repeatedly - eventually causing cache misses full image rebuilds.

*If the image has been already built but the cache doesn't work properly*, it
can be useful to skip the build phases:

.. code:: bash

    # first run ensures that the image is built
    archery docker run conda-python

    # if the second run tries the build the image again and none of the files
    # referenced in the relevant dockerfile have changed, then it indicates a
    # cache miss caused by the issue described above
    archery docker run conda-python

    # since the image is properly built with the first command, there is no
    # need to rebuild it, so manually disable the pull and build phases to
    # spare the some time
    archery docker run --no-pull --no-build conda-python

**Pass environment variables to the container:**

Most of the build scripts used within the containers can be configured through
environment variables. Pass them using ``--env`` or ``-e`` CLI options -
similar to the ``docker run`` and ``docker-compose run`` interface.

.. code:: bash

    archery docker run --env CMAKE_BUILD_TYPE=release ubuntu-cpp

For the available environment variables in the C++ builds see the
``ci/scripts/cpp_build.sh`` script.

**Run the image with custom command:**

Custom docker commands may be passed as the second argument to
``archery docker run``.

The following example starts an interactive ``bash`` session in the container
- useful for debugging the build interactively:

.. code:: bash

    archery docker run ubuntu-cpp bash

Docker Volume Caches
~~~~~~~~~~~~~~~~~~~~

Most of the compose container have specific directories mounted from the host
to reuse ``ccache`` and ``maven`` artifacts. These docker volumes are placed
in the ``.docker`` directory.

In order to clean up the cache simply delete one or more directories (or the
whole ``.docker`` directory).


Development
-----------

The docker-compose configuration is tuned towards reusable development
containers using hierarchical images. For example multiple language bindings
are dependent on the C++ implementation, so instead of redefining the
C++ environment multiple Dockerfiles, we can reuse the exact same base C++
image when building Glib, Ruby, R and Python bindings.
This reduces duplication and streamlines maintenance, but makes the
docker-compose configuration more complicated.

.. _docker-build-parameters:

Docker Build Parameters
~~~~~~~~~~~~~~~~~~~~~~~

The build time parameters are pushed down to the dockerfiles to make the
image building more flexible. These parameters are usually called as docker
build args, but we pass these values as environment variables to
docker-compose.yml. The build parameters are extensively used for:

- defining the docker registry used for caching
- platform architectures
- operation systems and versions
- defining various versions if dependencies

The default parameter values are stored in the top level .env file.
For detailed examples see the docker-compose.yml.

Build Scripts
~~~~~~~~~~~~~

The scripts maintainted under ci/scripts directory should be kept
parametrizable but reasonably minimal to clearly encapsulate the tasks it is
responsible for. Like:

- ``cpp_build.sh``: build the C++ implementation without running the tests.
- ``cpp_test.sh``: execute the C++ tests.
- ``python_build.sh``: build the Python bindings without running the tests.
- ``python_test.sh``: execute the Python tests.
- ``docs_build.sh``: build the Sphinx documentation.
- ``integration_dask.sh``: execute the dask integration tests.
- ``integration_pandas.sh``: execute the pandas integration tests.
- ``install_minio.sh``: install minio server for multiple platforms.
- ``install_conda.sh``: install miniconda for multiple platforms.
- ``install_gcs_testbench.sh``: install the GCS testbench for multiple platforms.

The parametrization (like the C++ CMake options) is achieved via environment
variables with useful defaults to keep the build configurations declarative.

A good example is ``cpp_build.sh`` build script which forwards environment
variables as CMake options - so the same scripts can be invoked in various
configurations without the necessity of changing it. For examples see how the
environment variables are passed in the docker-compose.yml's C++ images.

Adding New Images
~~~~~~~~~~~~~~~~~

See the inline comments available in the docker-compose.yml file.
