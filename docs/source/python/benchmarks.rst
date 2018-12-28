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
run with `asv`_.  You'll need to install the ``asv`` package first
(``pip install asv`` or ``conda install -c conda-forge asv``).

The benchmarks are run using `asv`_ which is also their only requirement.

Running the benchmarks
----------------------

To run the benchmarks, call ``asv run --python=same``. You cannot use the
plain ``asv run`` command at the moment as asv cannot handle python packages
in subdirectories of a repository.

Running with arbitrary revisions
--------------------------------

ASV allows to store results and generate graphs of the benchmarks over
the project's evolution.  For this you have the latest development version of ASV:

.. code::

    pip install git+https://github.com/airspeed-velocity/asv

Now you should be ready to run ``asv run`` or whatever other command
suits your needs.

Compatibility
-------------

We only expect the benchmarking setup to work with Python 3.6 or later,
on a Unix-like system.

.. _asv: https://asv.readthedocs.org/
