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

.. _support:

==============
Support Policy
==============

Official Support Policy
=======================

The Apache Arrow project houses a collection of libraries for different
programming languages supporting various build systems, operating systems and architectures.
This has generated a growing ecosystem of Continuous Integration (CI) jobs
to maintain in order to ensure the matrix of supported platforms is properly tested.

This page documents how a platform (Operation System, architecture or build system)
is supported in Arrow along with a list of platforms that are currently supported for
the Arrow C++ implementation.

Support tiers
=============

Platform support is broken down into tiers. Each tier comes with different requirements
which lead to different promises being made about support.

To be promoted to a tier, committer support is required and is expected to be driven by consensus.
A vote may be called if there is no agreement about the promotion but general consensus
on the GitHub issue is sufficient.

Demotion to a lower tier occurs when a CI job is unmaintained for an extended period of time or the
platform is going to be EOLed.

If the job is unmaintained a discussion on the mailing list and if necessary a vote
should be called in order to remove the platform from its current tier.

When a Platform is EOLed, it should be demoted to Tier 3 immediately unless there is
a strong reason to keep it supported.

Tier 1
------
    - Is tested on CI on PRs or via periodic daily runs.
    - Changes which would break the main branch are not allowed to be merged; any breakage should be fixed or reverted immediately.
    - CI failures block releases.
    - All committers are responsible to keep these CI jobs working.

Tier 2
------
    - Is tested on CI via periodic daily runs and can be tested on Pull Requests via optional jobs.
    - The CI jobs are prefixed with ``Tier 2``.
    - Are maintained as a best-effort basis.
    - CI failures do not block releases.
    - At least one committer should look over the CI jobs and be responsible to keep these platforms working.

Tier 3
------
    - Are not tested on CI.
    - Unsupported though we might want to accept patches for them depending on the maintenance overhead.


.. list-table::
   :header-rows: 1

   * - Platform constraint
     - Tier
