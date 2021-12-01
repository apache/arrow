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

Continuous Integration
======================

Continunous Integration for Arrow is fairly complex as it needs to run across different combinations of package managers, compilers, versions of multiple sofware libraries,  operating systems, and other potential sources of variation.  In this article, we will give an overview of its main components and the relevant files and directories.

Two files central to Arrow CI are:

* `docker-compose.yml` - here we define docker services which can be configured using either enviroment variables, or the default values for these variables
* `.env` - here we define default values to configure the services in `docker-compose.yml`

There are three important directories in the Arrow project which relate to CI:

* `.github/` - workflows that are via GitHub actions and are triggered by things like pull requests being submitted or merged
* `dev/` - containing jobs which are run via Archery and Crossbow, typically nightly builds or relating to the release process
* `ci/` - containing scripts supporting the various builds


It can easily be divided into two main categories:

* Action triggered builds
* Nightly builds

Action-triggered builds
-----------------------


Nightly builds
--------------



