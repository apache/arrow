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

There are multiple ways to execute the docker based builds. The recommented is
to use the archery tool:

Installation:

.. code:: bash

    pip install -e dev/archery[docker]

Execute a build:

.. code::bash

    archery docker run conda-python

Archery calls the following docker-compose commands:

.. code::bash

    docker-compose pull --ignore-pull-failures conda-cpp
    docker-compose build conda-cpp
    docker-compose pull --ignore-pull-failures conda-python
    docker-compose build conda-python
    docker-compose run --rm conda-python

To entirely disable the pulling / caching:

.. code::bash

    archery docker run --no-cache conda-python

Which translates to:

.. code::bash

    docker-compose build --no-cache conda-cpp
    docker-compose build --no-cache conda-python
    docker-compose run --rm conda-python

To disable the cache only for the leaf image:

.. code::bash

    archery docker run --no-cache-leaf conda-python

This is useful to force using the development version of a dependency, like
pandas:

.. code::bash

    PANDAS=master archery docker run --no-cache-leaf conda-python

For the available build time parameters see the .env file.

In order to alter the runtime parameters pass environment variables to the
container in execution time:

.. code::bash

    archery docker run -e CMAKE_BUILD_TYPE=release ubuntu-cpp

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

The build time parameters are pushed down to the [dockerfiles] to make the
image building more flexible. These are usually called docker build args.
The docker-compose.yml uses these build args extensively for:

- defining the docker registry used for caching
- platform architectures
- operation systems and versions
- defining various versions if dependencies

The default values are stored in the top level .env file. For examples see the
docker-compose.yml

Build Scripts
~~~~~~~~~~~~~

The scripts maintainted under ci/scripts directory should be kept as minimal as
possible to be responsible only a subset of tasks.

The parametrization is done through environment variables with sane defaults to
keep the build configurations declerative. Note that these parameters are
different from the ones described previously, these are affecting the runtime
behaviour of the builds scripts within the containers.

A good example is cpp_build.sh build script which forwards environment
variables as CMake options - so the same scripts can be invoked in various
configurations without the necessity of chaning it. For examples see how the
environment variables are passed in the docker-compose.yml's C++ images.

Adding New Images
~~~~~~~~~~~~~~~~~

See more in the docker-compose.yml
