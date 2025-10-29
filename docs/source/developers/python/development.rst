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

.. currentmodule:: pyarrow
.. highlight:: console
.. _develop_pyarrow:

==================
Developing PyArrow
==================

.. _python-coding-style:

Coding Style
============

We follow a similar PEP8-like coding style to the `pandas project
<https://github.com/pandas-dev/pandas>`_.  To fix style issues, use the
``pre-commit`` command:

.. code-block::

   $ pre-commit run --show-diff-on-failure --color=always --all-files python

.. _python-unit-testing:

Unit Testing
============

We are using `pytest <https://docs.pytest.org/en/latest/>`_ to develop our unit
test suite. After `building the project <build_pyarrow>`_ you can run its unit tests
like so:

.. code-block::

   $ pushd arrow/python
   $ python -m pytest pyarrow
   $ popd

Package requirements to run the unit tests are found in
``requirements-test.txt`` and can be installed if needed with ``pip install -r
requirements-test.txt``.

If you get import errors for ``pyarrow._lib`` or another PyArrow module when
trying to run the tests, run ``python -m pytest arrow/python/pyarrow`` and check
if the editable version of pyarrow was installed correctly.

The project has a number of custom command line options for its test
suite. Some tests are disabled by default, for example. To see all the options,
run

.. code-block::

   $ python -m pytest pyarrow --help

and look for the "custom options" section.

.. note::

   There are a few low-level tests written directly in C++. These tests are
   implemented in `pyarrow/src/arrow/python/python_test.cc <https://github.com/apache/arrow/blob/main/python/pyarrow/src/arrow/python/python_test.cc>`_,
   but they are also wrapped in a ``pytest``-based
   `test module <https://github.com/apache/arrow/blob/main/python/pyarrow/tests/test_cpp_internals.py>`_
   run automatically as part of the PyArrow test suite.

Test Groups
-----------

We have many tests that are grouped together using pytest marks. Some of these
are disabled by default. To enable a test group, pass ``--$GROUP_NAME``,
e.g. ``--parquet``. To disable a test group, prepend ``disable``, so
``--disable-parquet`` for example. To run **only** the unit tests for a
particular group, prepend ``only-`` instead, for example ``--only-parquet``.

The test groups currently include:

* ``dataset``: Apache Arrow Dataset tests
* ``flight``: Flight RPC tests
* ``gandiva``: tests for Gandiva expression compiler (uses LLVM)
* ``hdfs``: tests that use libhdfs to access the Hadoop filesystem
* ``hypothesis``: tests that use the ``hypothesis`` module for generating
  random test cases. Note that ``--hypothesis`` doesn't work due to a quirk
  with pytest, so you have to pass ``--enable-hypothesis``
* ``large_memory``: Test requiring a large amount of system RAM
* ``orc``: Apache ORC tests
* ``parquet``: Apache Parquet tests
* ``s3``: Tests for Amazon S3
* ``tensorflow``: Tests that involve TensorFlow

Doctest
=======

We are using `doctest <https://docs.python.org/3/library/doctest.html>`_
to check that docstring examples are up-to-date and correct. You can
also do that locally by running:

.. code-block::

   $ pushd arrow/python
   $ python -m pytest --doctest-modules
   $ python -m pytest --doctest-modules path/to/module.py # checking single file
   $ popd

for ``.py`` files or

.. code-block::

   $ pushd arrow/python
   $ python -m pytest --doctest-cython
   $ python -m pytest --doctest-cython path/to/module.pyx # checking single file
   $ popd

for ``.pyx`` and ``.pxi`` files. In this case you will also need to
install the `pytest-cython <https://github.com/lgpage/pytest-cython>`_ plugin.

Debugging
=========

Debug build
-----------

Since PyArrow depends on the Arrow C++ libraries, debugging can
frequently involve crossing between Python and C++ shared libraries.
For the best experience, make sure you've built both Arrow C++
(``-DCMAKE_BUILD_TYPE=Debug``) and PyArrow (``export PYARROW_BUILD_TYPE=debug``)
in debug mode.

Using gdb on Linux
------------------

To debug the C++ libraries with gdb while running the Python unit
tests, first start pytest with gdb:

.. code-block:: console

   $ gdb --args python -m pytest pyarrow/tests/test_to_run.py -k $TEST_TO_MATCH

To set a breakpoint, use the same gdb syntax that you would when
debugging a C++ program, for example:

.. code-block:: console

   (gdb) b src/arrow/python/arrow_to_pandas.cc:1874
   No source file named src/arrow/python/arrow_to_pandas.cc.
   Make breakpoint pending on future shared library load? (y or [n]) y
   Breakpoint 1 (src/arrow/python/arrow_to_pandas.cc:1874) pending.

.. seealso::

   The :ref:`GDB extension for Arrow C++ <cpp_gdb_extension>`.

Similarly, use lldb when debugging on macOS.

Benchmarking
============

For running the benchmarks, see :ref:`python-benchmarks`.
