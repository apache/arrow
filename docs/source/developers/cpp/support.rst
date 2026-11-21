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

Official Support Policy for Apache Arrow C++
============================================

The Apache Arrow C++ implementation supports various build systems, operating systems,
and architectures. This has generated a growing ecosystem of Continuous Integration (CI)
jobs to maintain in order to ensure the matrix of supported platforms is properly tested.

This page documents how a platform (operation system, architecture or build system)
is supported in the Arrow C++ implementation along with a list of platforms that
are currently supported for the Arrow C++ implementation.

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

Demotions other than for EOL should be announced on the mailing list early enough
for stakeholders to offer resources to keep the platform supported.
Each tier change should have its own GitHub issue and be recorded in the platform history below.

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

Platform axes
=============

A platform is described along independent axes: operating system,
architecture, compiler, SIMD level and build system. Each value on each
axis is assigned a tier.

A given configuration is supported at the **lowest tier among its axes**.
For example, Linux (Tier 1) on s390x (Tier 3) is Tier 3.

Tier 1 does not promise that every combination of Tier 1 values is tested.
It promises that every Tier 1 value is exercised by at least one Tier 1 CI
configuration.

Operating systems
-----------------

Only releases that are not end-of-life upstream are supported. When an
operating system release reaches its upstream end-of-life it is demoted to
Tier 3 unless there is a strong reason, and a committed maintainer, to keep
it supported.

.. list-table::
   :header-rows: 1

   * - Operating system
     - Tier
     - Notes
   * - Ubuntu (LTS releases)
     - 1
     -
   * - Debian (stable releases)
     - 1
     -
   * - AlmaLinux, CentOS Stream, Amazon Linux
     - 1
     - Covered by the binary packages, tested on a schedule
   * - macOS
     - 1
     -
   * - Windows
     - 1
     -
   * - Alpine Linux (musl)
     - 2
     -
   * - Emscripten
     - 2
     -
   * - Any other
     - 3
     -

Architectures
-------------

.. list-table::
   :header-rows: 1

   * - Architecture
     - Tier
     - Notes
   * - x86_64 (amd64)
     - 1
     - Linux and Windows
   * - aarch64 (arm64)
     - 1
     - Linux and macOS
   * - x86_64 (amd64)
     - 2
     - macOS
   * - aarch64 (arm64)
     - 2
     - Windows
   * - Any other (ppc64le, s390x, 32-bit, riscv64, ...)
     - 3
     -

Compilers
---------

.. list-table::
   :header-rows: 1

   * - Compiler
     - Tier
   * - GCC 12 or newer
     - 1
   * - Clang 18 or newer
     - 1
   * - AppleClang (Xcode version shipped by the oldest supported macOS)
     - 1
   * - MSVC (Visual Studio 2022 or newer)
     - 1
   * - MinGW (MSYS2)
     - 2

SIMD levels
-----------

.. list-table::
   :header-rows: 1

   * - SIMD level
     - Tier
   * - None (scalar fallback)
     - 1
   * - SSE4.2
     - 1
   * - AVX2
     - 1
   * - NEON
     - 1
   * - AVX512
     - 1
   * - SVE128 (Linux, runtime dispatch)
     - 1
   * - Any other
     - 3

Build systems
-------------

.. list-table::
   :header-rows: 1

   * - Build system
     - Tier
   * - CMake
     - 1
   * - Meson
     - 2

Binary distribution
===================

The Linux packages (``.deb`` and ``.rpm``) published by the project for the
operating systems and architectures listed above are Tier 1. They are built
and tested on a scheduled basis rather than on every Pull Request.

This policy does not cover Python wheels or other language bindings.

Platform history
================

.. list-table::
   :header-rows: 1

   * - Platform
     - Change
     - Arrow version
     - Issue
   * - macOS x86_64
     - Tier 1 to Tier 2
     - 27.0.0
     - `Mailing list discussion <https://lists.apache.org/thread.html/l3vmknbjhwf60crrfp3hwb7p5m4qjw7z>`__
