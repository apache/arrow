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

.. _release_verification:

============================
Release Verification Process
============================

This page provides detailed information on the steps followed to perform
a release verification on the major platforms.

Principles
==========

The Apache Arrow Release Approval process follows the guidelines defined at the
`Apache Software Foundation Release Approval <https://www.apache.org/legal/release-policy.html#release-approval>`_.

For a release vote to pass, a minimum of three positive binding votes and more
positive binding votes than negative binding votes MUST be cast.
Releases may not be vetoed. Votes cast by PMC members are binding, however,
non-binding votes are greatly encouraged and a sign of a healthy project.

Running the release verification
================================

Linux and macOS
---------------

In order to run the verification script either for the source release or the
binary artifacts see the following guidelines:

.. code-block::

   # this will create and automatically clean up a temporary directory for the verification environment and will run the source verification
   TEST_DEFAULT=0 TEST_SOURCE=1 verify-release-candidate.sh $VERSION $RC_NUM
   
   # this will create and automatically clean up a temporary directory for the verification environment and will run the binary verification
   TEST_DEFAULT=0 TEST_BINARIES=1 dev/release/verify-release-candidate.sh $VERSION $RC_NUM
   
   # to verify only certain implementations use the TEST_DEFAULT=0 and TEST_* variables
   # here are a couple of examples, but see the source code for the available options
   TEST_DEFAULT=0 TEST_CPP=1 verify-release-candidate.sh $VERSION $RC_NUM  # only C++ tests
   TEST_DEFAULT=0 TEST_CPP=1 TEST_PYTHON=1 verify-release-candidate.sh $VERSION $RC_NUM  # C++ and Python tests
   TEST_DEFAULT=0 TEST_INTEGRATION_CPP=1 TEST_INTEGRATION_JAVA=1 verify-release-candidate.sh $VERSION $RC_NUM  # C++ and Java integration tests
   
   # to verify certain binaries use the TEST_* variables as:
   TEST_DEFAULT=0 TEST_WHEELS=1 verify-release-candidate.sh $VERSION $RC_NUM  # only Wheels
   TEST_DEFAULT=0 TEST_APT=1 verify-release-candidate.sh $VERSION $RC_NUM  # only APT packages
   TEST_DEFAULT=0 TEST_YUM=1 verify-release-candidate.sh $VERSION $RC_NUM  # only YUM packages
   TEST_DEFAULT=0 TEST_JARS=1 verify-release-candidate.sh $VERSION $RC_NUM  # only JARS

Windows
-------

In order to run the verification script on Windows you have to download
the source tarball from the SVN dist system that you wish to verify:

.. code-block::

   dev\release\verify-release-candidate.bat %VERSION% %RC_NUM%

System Configuration Instructions
=================================

You will need some tools installed like curl, git, etcetera.

Ubuntu
------

You might have to install some packages on your system. The following
utility script can be used to set your Ubuntu system. This wil install
the required packages to perform a source verification on a clean
Ubuntu:

.. code-block::

   # From the arrow clone
   sudo dev/release/setup-ubuntu.sh

macOS ARM
---------

.. code-block::

   # From the arrow clone
   brew install gpg
   brew bundle --file=cpp/Brewfile
   brew bundle --file=c_glib/Brewfile
   brew uninstall node
   # You might need to add node, ruby java and maven to the PATH, follow
   # instructions from brew after installing.
   brew install node@20
   brew install ruby
   brew install openjdk
   brew install maven

Windows 11
----------

To be defined

Casting your vote
=================

Once you have performed the verification you can cast your vote by responding
to the vote thread on dev@arrow.apache.org and supply your result.

If the verification was successful you can send your +1 vote. We usually send
along with the vote the command that was executed and the local versions used.
As an example:

.. code-block::
   +1

   I've verified successfully the sources and binaries with:

   TEST_DEFAULT=0 TEST_SOURCE=1 dev/release/verify-release-candidate.sh 15.0.0 1
   TEST_DEFAULT=0 TEST_BINARIES=1 dev/release/verify-release-candidate.sh 15.0.0 1
   with:
   * Python 3.10.12
   * gcc (Ubuntu 11.4.0-1ubuntu1~22.04) 11.4.0
   * NVIDIA CUDA Build cuda_11.5.r11.5/compiler.30672275_0
   * openjdk version "17.0.9" 2023-10-17
   * ruby 3.0.2p107 (2021-07-07 revision 0db68f0233) [x86_64-linux-gnu]
   * dotnet 7.0.115
   * Ubuntu 22.04 LTS

If there were some issues during verification please report them on the
mail thread to diagnose the issue.
