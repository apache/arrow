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

.. _cpp-fuzzing:

=================
Fuzzing Arrow C++
=================

To make the handling of invalid input more robust, we have enabled
fuzz testing on several parts of the Arrow C++ feature set, currently:

* the IPC stream format
* the IPC file format
* the Parquet file format

We welcome any contribution to expand the scope of fuzz testing and cover
areas ingesting potentially invalid or malicious data.

Fuzz Targets and Utilities
==========================

By passing the ``-DARROW_FUZZING=ON`` CMake option, you will build
the fuzz targets corresponding to the aforementioned Arrow features, as well
as additional related utilities.

Generating the seed corpus
--------------------------

Fuzzing essentially explores the domain space by randomly mutating previously
tested inputs, without having any high-level understanding of the area being
fuzz-tested.  However, the domain space is so huge that this strategy alone
may fail to actually produce any "interesting" inputs.

To guide the process, it is therefore important to provide a *seed corpus*
of valid (or invalid, but remarkable) inputs from which the fuzzing
infrastructure can derive new inputs for testing.  A script is provided
to automate that task.  Assuming the fuzzing executables can be found in
``build/debug``, the seed corpus can be generated thusly:

.. code-block:: shell

   $ ./build-support/fuzzing/generate_corpuses.sh build/debug

Continuous fuzzing infrastructure
=================================

The process of fuzz testing is computationally intensive and therefore
benefits from dedicated computing facilities.  Arrow C++ is exercised by
the `OSS-Fuzz`_ continuous fuzzing infrastructure operated by Google.

Issues found by OSS-Fuzz are notified and available to a limited set of
`core developers <https://github.com/google/oss-fuzz/blob/master/projects/arrow/project.yaml>`_.
If you are a Arrow core developer and want to be added to that list, you can
ask on the :ref:`mailing-list <contributing>`.

.. _OSS-Fuzz: https://google.github.io/oss-fuzz/

Reproducing locally
===================

When a crash is found by fuzzing, it is often useful to download the data
used to produce the crash, and use it to reproduce the crash so as to debug
and investigate.

Assuming you are in a subdirectory inside ``cpp``, the following command
would allow you to build the fuzz targets with debug information and the
various sanitizer checks enabled.

.. code-block:: shell

   $ cmake .. -GNinja \
       -DCMAKE_BUILD_TYPE=Debug \
       -DARROW_USE_ASAN=on \
       -DARROW_USE_UBSAN=on \
       -DARROW_FUZZING=on

Then, assuming you have downloaded the crashing data file (let's call it
``testcase-arrow-ipc-file-fuzz-123465``), you can reproduce the crash
by running the affected fuzz target on that file:

.. code-block:: shell

   $ build/debug/arrow-ipc-file-fuzz testcase-arrow-ipc-file-fuzz-123465

(you may want to run that command under a debugger so as to inspect the
program state more closely)
