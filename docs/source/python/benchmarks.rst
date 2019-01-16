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

Benchmarks
==========

The ``pyarrow`` package comes with a suite of benchmarks meant to
run with `ASV`_.  You'll need to install the ``asv`` package first
(``pip install asv`` or ``conda install -c conda-forge asv``).

Running the benchmarks
----------------------

To run the benchmarks for a locally-built Arrow, run ``asv dev`` or
``asv run --python=same``.

Running for arbitrary Git revisions
-----------------------------------

ASV allows to store results and generate graphs of the benchmarks over
the project's evolution.  You need to have the latest development version of ASV:

.. code::

    pip install git+https://github.com/airspeed-velocity/asv

The build scripts assume that Conda's ``activate`` script is on the PATH
(the ``conda activate`` command unfortunately isn't available from
non-interactive scripts).

Now you should be ready to run ``asv run`` or whatever other command
suits your needs.  Note that this can be quite long, as each Arrow needs
to be rebuilt for each Git revision you're running the benchmarks for.

Compatibility
-------------

We only expect the benchmarking setup to work with Python 3.6 or later,
on a Unix-like system with bash.

.. _asv: https://asv.readthedocs.org/
