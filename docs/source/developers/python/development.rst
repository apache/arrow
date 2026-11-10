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
* ``gandiva``: tests for Gandiva expression compiler (uses LLVM, deprecated since version 24.0.0)
* ``hdfs``: tests that use libhdfs to access the Hadoop filesystem
* ``hypothesis``: tests that use the ``hypothesis`` module for generating
  random test cases. Note that ``--hypothesis`` doesn't work due to a quirk
  with pytest, so you have to pass ``--enable-hypothesis``
* ``large_memory``: Test requiring a large amount of system RAM
* ``orc``: Apache ORC tests
* ``parquet``: Apache Parquet tests
* ``s3``: Tests for Amazon S3
* ``tensorflow``: Tests that involve TensorFlow

Type Checking
=============

PyArrow provides type stubs (``*.pyi`` files) for static type checking. These
stubs are located in the ``pyarrow-stubs/`` directory, which is laid out as a
:pep:`561` stub-only package for ``pyarrow``, so type checkers and IDEs pick
it up from the ``arrow/python`` directory. The stubs aren't included in the
distributed wheel packages until they are complete (see
`GH-49831 <https://github.com/apache/arrow/issues/49831>`_).

Running Type Checkers
---------------------

We support multiple type checkers. Their configurations are in
``pyproject.toml``.

The easiest way to run all of them is with ``pre-commit``, which installs
the type checkers and stub packages pinned in ``requirements-stubs.txt`` (with
uv). This also lints and formats the stubs with ruff:

.. code-block::

   $ pre-commit run --hook-stage manual python-stubs --all-files

The type checkers only run on request, while ruff also runs on commit. The
Lint CI job runs all of them as well. CI also runs the type checkers, the type
stub tests and stubtest (see below) against the PyArrow it builds, with
``ci/scripts/python_test_type_annotations.sh``.

To run a type checker directly instead, install the pinned type checkers and
stub packages in your PyArrow development environment:

.. code-block::

   $ pip install -r requirements-stubs.txt

Checking the PyArrow tests also needs their dependencies from
``requirements-test.txt``.

**mypy**

To run mypy on the PyArrow codebase:

.. code-block::

   $ cd arrow/python
   $ mypy

The mypy configuration is in the ``[tool.mypy]`` section of ``pyproject.toml``.
mypy only uses the stubs when it checks them too, so pass ``pyarrow-stubs``
along with any other files you check:

.. code-block::

   $ mypy pyarrow-stubs pyarrow/tests/test_array.py

**pyright**

To run pyright:

.. code-block::

   $ cd arrow/python
   $ pyright

The pyright configuration is in the ``[tool.pyright]`` section of ``pyproject.toml``.

**ty**

To run ty (note: currently only partially configured):

.. code-block::

   $ cd arrow/python
   $ ty check

**stubtest**

mypy's stubtest compares the stubs with the signatures of the installed
PyArrow. Run it with:

.. code-block::

   $ cd arrow/python
   $ python scripts/run_stubtest.py

Stubs that aren't written yet are skipped. The known mismatches are listed in
``pyarrow-stubs-tests/stubtest_allowlist.txt``; remove the entries of the
mismatches you fix, as stubtest fails on unused entries.

Maintaining Type Stubs
-----------------------

Type stubs for PyArrow are maintained in the ``pyarrow-stubs/``
directory. These stubs mirror the structure of the main ``pyarrow/`` package.

When adding or modifying public APIs:

1. **Update the corresponding ``.pyi`` stub file** in ``pyarrow-stubs/``
   to reflect the new or changed function/class signatures.

2. **Include type annotations** where possible. For Cython modules or
   dynamically generated APIs such as compute kernels add the corresponding
   stub in ``pyarrow-stubs/``.

3. **Run type checkers** to ensure the stubs are correct and complete.

The stub files are automatically copied into the built wheel during the build
process and will be included when users install PyArrow, enabling type checking
in downstream projects and for users' IDEs.

Note: ``py.typed`` marker file in the ``pyarrow/`` directory indicates to type
checkers that PyArrow supports type checking according to :pep:`561`.

Testing Type Stubs
------------------

The tests in ``pyarrow-stubs-tests/`` pin down the annotated types of public
APIs. Each check pairs :func:`typing.assert_type`, which every type checker
verifies statically, with a runtime ``isinstance`` check:

.. code-block:: python

   from typing import TYPE_CHECKING

   import pyarrow as pa

   from _check import assert_type, check

   def test_list_type() -> None:
       lst = pa.list_(pa.int32())
       check(assert_type(lst, pa.ListType), pa.ListType)
       check(assert_type(lst.value_type, pa.DataType), pa.DataType)

       if TYPE_CHECKING:
           pa.list_(1)  # type: ignore[call-overload]

The type checkers check these files as part of the regular runs above, and
pytest runs them against the installed PyArrow to make sure the stubs agree
with what PyArrow actually returns:

.. code-block::

   $ cd arrow/python
   $ pytest pyarrow-stubs-tests

Code under ``if TYPE_CHECKING:`` is never executed. It holds the usage that
should fail type checking, which is asserted with a
``# type: ignore[<mypy code>]`` comment: unused ignores are reported as errors
in these tests, so the test fails once the usage is no longer rejected. ty
doesn't honor mypy error codes, so it doesn't check invalid usage.

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

.. note::
   Cython ``.pxi`` files are included in ``.pyx`` files at compile time,
   so ``--doctest-cython`` cannot be run directly on ``.pxi`` files.
   In PyArrow, all ``.pxi`` files are included into ``lib.pyx``, so run
   doctests on that file::

      $ python -m pytest --doctest-cython path/to/lib.pyx

   Any doctest errors originating from ``.pxi`` files will appear under
   ``lib.pyx``, not the original ``.pxi`` filename.

Testing Documentation Examples
-------------------------------

Documentation examples in ``.rst`` files under ``docs/source/python/`` use
doctest syntax and can be tested locally using:

.. code-block::

   $ pushd arrow/python
   $ pytest --doctest-glob="*.rst" docs/source/python/file.rst # checking single file
   $ pytest --doctest-glob="*.rst" docs/source/python # checking entire directory
   $ popd

The examples use standard doctest syntax with ``>>>`` for Python prompts and
``...`` for continuation lines. The ``conftest.py`` fixture automatically
handles temporary directory setup for examples that create files.


Debugging
=========

Debug build
-----------

Since PyArrow depends on the Arrow C++ libraries, debugging can
frequently involve crossing between Python and C++ shared libraries.
For the best experience, make sure you've built both Arrow C++
(``-DCMAKE_BUILD_TYPE=Debug``) and PyArrow
(``pip install --no-build-isolation -C cmake.build-type=Debug .``)
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

For running the benchmarks, see :ref:`benchmarks`.
