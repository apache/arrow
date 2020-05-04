.. raw:: html

   <!--
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
   -->

Running Docker Builds
=====================

Most of our Linux based continuous integration tasks are decoupled from public
CI services using docker and docker-compose. Keeping the CI configuration
minimal makes local reproducibility possible.

Usage
-----

There are multiple ways to execute the docker based builds. The recommended is
to use the archery tool:

Installation
~~~~~~~~~~~~

Requires ``python>=3.5``. It is recommended to install archery in ``editable``
mode to automatically update the intallation by pulling the arrow repository.

.. code:: bash

    pip install -e dev/archery[docker]

Inspect the available commands and options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    archery docker --help
    archery docker run --help

``archery docker run`` tries to provide a similar interface to
``docker-compose run``, but with builtin support for building hierarchical
images.

Examples
~~~~~~~~

**Execute a build:**

.. code:: bash

    archery docker run conda-python

Archery calls the following docker-compose commands:

.. code:: bash

    docker-compose pull --ignore-pull-failures conda-cpp
    docker-compose build conda-cpp
    docker-compose pull --ignore-pull-failures conda-python
    docker-compose build conda-python
    docker-compose run --rm conda-python

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
The leaf image is ``conda-python-pandas`` in the example.

.. code:: bash

    PANDAS=master archery docker run --no-cache-leaf conda-python-pandas

Which translates to:

.. code:: bash

    export PANDAS=master
    docker-compose pull --ignore-pull-failures conda-cpp
    docker-compose build conda-cpp
    docker-compose pull --ignore-pull-failures conda-python
    docker-compose build conda-python
    docker-compose build --no-cache conda-python-pandas
    docker-compose run --rm conda-python-pandas

Note that it doens't pull the conda-python-pandas image and disable the cache
when building it.

``PANDAS`` is a `build parameter <Docker Build Parameters>`_, see the
defaults in the .env file.

**To entirely skip building the image:**

.. code:: bash

    archery docker run --no-build conda-python

In order to alter the runtime parameters pass docker environment variables to
the container during its execution:

.. code:: bash

    archery docker run --env CMAKE_BUILD_TYPE=release ubuntu-cpp

See the available C++ in the ``ci/scripts/cpp_build.sh`` script.

**Run the image with custom command:**

Custom docker commands may be passed as the second argument. The following
example starts an interactive ``bash`` session in the container - useful for
debugging the build interactively:

.. code:: bash

    archery docker run ubuntu-cpp bash


Development
-----------

The docker-compose configuration is tuned towards reusable development
containers using hierarchical images. For example multiple language bindings
are dependent on the C++ implementation, so instead of redefining the
C++ environment multiple Dockerfiles, we can reuse the exact same base C++
image when building Glib, Ruby, R and Python bindings.
This helps reducing duplications and preventing a series of maintenance, but
makes the docker-compose configuration more complicated.

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

The scripts maintainted under ci/scripts directory should be kept as minimal as
possible to be responsible for only a subset of tasks.

The parametrization (like the C++ CMake options) is achieved via environment
variables with useful defaults to keep the build configurations declarative.

Note that these parameters are different from the ones described previously,
these are affecting the runtime behaviour of the builds scripts within the
containers.

A good example is ``cpp_build.sh`` build script which forwards environment
variables as CMake options - so the same scripts can be invoked in various
configurations without the necessity of chaning it. For examples see how the
environment variables are passed in the docker-compose.yml's C++ images.

Adding New Images
~~~~~~~~~~~~~~~~~

See more in the docker-compose.yml
