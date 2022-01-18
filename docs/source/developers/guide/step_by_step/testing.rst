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


.. SCOPE OF THIS SECTION
.. This section should include extra description to the
.. language-specific documentation. Possible topics to
.. add: How to run one test, test file or all the tests
.. together and why is it good to do that.
.. What if the unconnected tests start failing? etc.


.. _testing:

***********
Testing 🧪
***********

In this section we outline steps needed for unit testing in Arrow.

.. tabs::

   .. tab:: Pytest

      We use `pytest <https://docs.pytest.org/en/latest/>`_ for
      unit tests in Python. For more info about the required
      packages follow
      :ref:`Python unit testing section <python-unit-testing>`.

      To run a specific unit test, use this command in
      the terminal from the ``arrow/python`` folder:

      .. code:: console

         $ python -m pytest pyarrow/tests/test_file.py -k test_your_unit_test

      Run all the tests from one file:

      .. code:: console

         $ python -m pytest pyarrow/tests/test_file.py

      Run all the tests:

      .. code:: console

         $ python -m pytest pyarrow

      If the tests start failing, try to recompile
      PyArrow or C++.

      .. note::

         **Recompiling Cython**

         If you only make changes to `.py` files, you do not need to
         recompile PyArrow. However, you should recompile it if you make
         changes in `.pyx` or `.pxd` files.

         To do that run this command again:

         .. code:: console

            $ python setup.py build_ext --inplace

      .. note::

         **Recompiling C++**

         Similarly, you will need to recompile the C++ code if you have
         made changes to any C++ files. In this case,
         re-run the cmake commands again.

   .. tab:: R tests

      We use `testthat <https://testthat.r-lib.org/index.html>`_ for unit testing in R. More specifically, we use the `3rd edition of testthat <https://testthat.r-lib.org/articles/third-edition.html>`_. On rare occasions we might want the behaviour of the 2nd edition of testthat, which is indicated by ``testthat::local_edition(2)``.

      **Structure**

      Expect the usual testthat folder structure:

      .. code-block:: R

         tests
          ├── testthat      # test files live here
          └── testthat.R    # runs tests when R CMD check runs (e.g. with devtools::check())

      This is the fundamental structure of testing in R with ``testthat``. Files such as ``testthat.R`` are not expected to change very often. For the ``arrow`` R package ``testthat.R`` also defines how the results of the various tests are displayed / reported in the console.

      Usually, most files in the ``R/`` sub-folder have a corresponding test file in ``tests/testthat``.

      **Running tests**

      To run all tests in a package locally call

      .. code-block:: R

         devtools::test()

      in the R console. Alternatively, you can use

      .. code:: console

         $ make test

      in the shell.

      You can run the tests in a single test file you have open with

      .. code-block:: R

         devtools::test_active_file()

      All tests are also run as part of our continuous integration (CI) pipelines.

      The `Arrow R Developer guide also has a section <https://arrow.apache.org/docs/r/articles/developing.html#running-tests>`_ on running tests.

      **Good practice**

      In general any change to source code needs to be accompanied by unit tests. All tests are expected to pass before a pull request is merged.

      * Add functionality -> add unit tests
      * Modify functionality -> update unit tests
      * Solve a bug -> add unit test before solving it, which helps prove the bug and its fix
      * Performance improvements should be reflected in benchmarks (which are also tests)
      * An exception could be refactoring functionality that is fully covered by unit tests

      A good rule of thumb is: If the new functionality is a user-facing or API change, you will almost certainly need to change tests — if no tests need to be changed, it might mean the tests aren't right! If the new functionality is a refactor and no APIs are changing, there might not need to be test changes.

      **Testing helpers**

      To complement the ``testthat`` functionality, the ``arrow`` R package has defined a series of specific utility functions (called helpers), such as:

      * expectations - these start with ``expect_`` and are used to compare objects
            - for example, the ``expect_…_roundtrip()`` functions take an input, convert it to some other format (e.g. arrow, altrep) and then convert it back, confirming that the values are the same.

      .. code-block:: R

         x <- c(1, 2, 3, NA_real_)
         expect_altrep_roundtrip(x, min, na.rm = TRUE)

      * ``skip_`` - skips a unit test - think of them as acceptable fails. Situations in which we might want to skip unit tests:

        - ``skip_if_r_version()`` - this is a specific ``arrow`` skip. For example, we use this to skip a unit test when the R version is 3.5.0 and below (``skip_if_r_version(“3.5.0”)``). You will likely see it used when the functionality we are testing depends on features introduced after version 3.5.0 of R (such as the alternative representation of vectors, Altrep, introduced in R 3.5.0, but with significant additions in subsequent releases). As part of our CI workflow we test against different versions of R and this is where this feature comes in.
        - ``skip_if_not_available()`` - another specific {arrow} skip. Arrow (libarrow) has a number of optional features that can be switched on or off (which happens at build time). If a unit test depends on such a feature and this feature is not available (i.e. was not selected when libarrow was built) the test is skipped, as opposed to having a failed test.
        - ``skip_if_offline()`` - will not run tests that require an internet connection
        - ``skip_on_os()`` - for unit tests that are OS specific.

      *Important*: Once the conditions for a ``skip_()`` statement is met, no other line of code in the same ``test_that()`` test block will get executed. If the ``skip`` is outside of a ``test_that()`` code block, it will skip the rest of the file.

      For more information about unit testing in R in general:

      * the ``testthat`` `website <https://testthat.r-lib.org/index.html>`_
      * the **R Packages** `book <https://r-pkgs.org>`_ by Hadley Wickham and Jenny Bryan
