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

.. _integration:

Integration Testing
===================

Prerequisites
-------------

Arrow uses `Docker <https://docs.docker.com/>`_ and
`docker-compose <https://docs.docker.com/compose/>`_ for integration testing.
You can follow the installation `instructions <https://docs.docker.com/compose/install/>`_.

Docker images (services)
------------------------

The docker-compose services are defined in the ``docker-compose.yml`` file.
Each service usually correspond to a language binding or an important service to
test with Arrow.

Services are configured with 2 local mounts, ``/arrow`` for the top-level source
directory and ``/build`` for caching build artifacts. The source level
directory mount can be paired with git checkout to test a specific commit. The
build mount is used for caching and sharing state between staged images.

- *c_glib*: Builds the GLib bindings
- *cpp*: Builds the C++ project
- *go*: Builds the go project
- *java*: Builds the Java project
- *js*: Builds the Javascript project
- *python*: Builds the python bindings
- *r*: Builds the R bindings
- *rust*: Builds the rust project
- *lint*: Run various lint on the C++ sources
- *iwyu*: Run include-what-you-use on the C++ sources
- *clang-format*: Run clang-format on the C++ sources, modifying in place
- *clang-tidy*: Run clang-tidy on the C++ sources, outputting recommendations
- *docs*: Builds this documentation

You can build and run a service by using the `build` and `run` docker-compose
sub-command, e.g. `docker-compose build python && docker-compose run python`.
We do not publish the build images, you need to build them manually. This
method requires the user to build the images in reverse dependency order. To
simplify this, we provide a Makefile.

.. code-block:: shell

   # Build and run manually
   docker-compose build cpp
   docker-compose build python
   docker-compose run python

   # Using the makefile with proper image dependency resolution
   make -f Makefile.docker python
