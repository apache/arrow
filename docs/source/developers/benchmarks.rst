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

.. _benchmarks:

**********
Benchmarks
**********

Archery
=======

``archery`` is a python library and command line utility made to interact with
Arrow's sources. The main feature is the benchmarking process.

Installation
~~~~~~~~~~~~

The simplest way to install archery is with pip from the top-level directory.
It is recommended to use the ``-e,--editable`` flag so that pip don't copy
the module files but uses the actual sources.

.. code-block:: shell

  pip install -e dev/archery
  archery --help

  # optional: enable bash/zsh autocompletion
  eval "$(_ARCHERY_COMPLETE=source archery)"

Comparison
==========

One goal with benchmarking is to detect performance regressions. To this end,
``archery`` implements a benchmark comparison facility via the ``benchmark
diff`` command.

In the default invocation, it will compare the current source (known as the
current workspace in git) with local master branch.

For more information, invoke the ``archery benchmark diff --help`` command for
multiple examples of invocation.

Iterating efficiently
~~~~~~~~~~~~~~~~~~~~~

Iterating with benchmark development can be a tedious process due to long
build time and long run times. ``archery benchmark diff`` provides 2 methods
to reduce this overhead.

First, the benchmark command supports comparing existing
build directories, This can be paired with the ``--preserve`` flag to
avoid rebuilding sources from zero.

.. code-block:: shell

  # First invocation clone and checkouts in a temporary directory. The
  # directory is preserved with --preserve
  archery benchmark diff --preserve

  # Modify C++ sources

  # Re-run benchmark in the previously created build directory.
  archery benchmark diff /tmp/arrow-bench*/{WORKSPACE,master}/build

Second, the benchmark command supports filtering suites (``--suite-filter``)
and benchmarks (``--benchmark-filter``), both options supports regular
expressions.

.. code-block:: shell

  # Taking over a previous run, but only filtering for benchmarks matching
  # `Kernel` and suite matching `compute-aggregate`.
  archery benchmark diff                                       \
    --suite-filter=compute-aggregate --benchmark-filter=Kernel \
    /tmp/arrow-bench*/{WORKSPACE,master}/build

Both methods can be combined.

Regression detection
====================

Writing a benchmark
~~~~~~~~~~~~~~~~~~~

1. The benchmark command will filter (by default) benchmarks with the regular
   expression ``^Regression``. This way, not all benchmarks are run by default.
   Thus, if you want your benchmark to be verified for regression
   automatically, the name must match.

2. The benchmark command will run with the ``--benchmark_repetitions=K``
   options for statistical significance. Thus, a benchmark should not override
   the repetitions in the (C++) benchmark's arguments definition.

3. Due to #2, a benchmark should run sufficiently fast. Often, when the input
   does not fit in memory (L2/L3), the benchmark will be memory bound instead
   of CPU bound. In this case, the input can be downsized.

Scripting
=========

``archery`` is written as a python library with a command line frontend. The
library can be imported to automate some tasks.

Some invocation of the command line interface can be quite verbose due to build
output. This can be controlled/avoided with the ``--quiet`` option, e.g.

.. code-block:: shell

  archery --quiet benchmark diff --benchmark-filter=Kernel
  {"benchmark": "BenchSumKernel/32768/0", "change": -0.6498, "regression": true, ...
  {"benchmark": "BenchSumKernel/32768/1", "change": 0.01553, "regression": false, ...
  ...
