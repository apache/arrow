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
.. The aim of this section is to provide extra description to
.. the process of building Arrow library. It could include:
.. what does building mean, what is CMake, what are flags and why
.. do we use them, is building Arrow supposed to be straightforward?
.. etc.

.. Be sure not to duplicate with existing documentation!
.. All language-specific instructions about building, testing,
.. installing dependencies, etc. should go into language-specific
.. documentation.


.. _build-arrow-guide:

************************************
Building the Arrow libraries 🏋🏿‍♀️
************************************

The Arrow project contains a number of libraries that enable
work in many languages. Most libraries (C++, C#, Go, Java,
JavaScript, Julia, and Rust) already contain distinct implementations
of Arrow. 

This is different for C (Glib), MATLAB, Python, R, and Ruby as they
are built on top of the C++ library. In this section of the guide
we will try to make a friendly introduction to the build,
dealing with some of these libraries as well has how they work with
the C++ library.

If you decide to contribute to Arrow you might need to compile the
C++ source code. This is done using a tool called CMake, which you
may or may not have experience with. If not, this section of the
guide will help you better understand CMake and the process
of building Arrow's C++ code.

This content is intended to help explain the concepts related to 
and tools required for building Arrow's C++ library from source.
If you are looking for the specific required steps, or already feel comfortable 
with compiling Arrow's C++ library, then feel free to proceed
to the :ref:`C++ <building-arrow-cpp>`, :ref:`PyArrow <build_pyarrow>` or
`R package build section <https://arrow.apache.org/docs/r/articles/developing.html>`_.

Building Arrow C++
==================

Why build Arrow C++ from source?
--------------------------------

For Arrow implementations which are built on top of the C++ implementation
(e.g. Python and R), wrappers and interfaces have been written to the
underlying C++ functions. If you want to work on PyArrow or the R package,
you may need to edit the source code of the C++ library too.

Detailed instructions on building C++ library from source can
be found :ref:`here <building-arrow-cpp>`.

About CMake
-----------

CMake is a cross-platform build system generator and it defers
to another program such as ``make`` or ``ninja`` for the actual build.
If you are running into errors with the build process, the first thing to
do is to look at the error message thoroughly and check the building
documentation for any similar error advice. Also changing the CMake flags
for compiling Arrow could be useful.

CMake presets
^^^^^^^^^^^^^^^^^^^

You could also try to build with CMake presets which are a collection of
build and test recipes for Arrow's CMake. They are a very useful
starting points.

More detailed information about CMake presets can be found in
the :ref:`cmake_presets` section.

Optional flags and environment variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Flags used in the CMake build are used to include additional components
and to handle third-party dependencies.
The build for C++ library can be minimal with no use of flags or can
be changed with adding optional components from the
:ref:`list <cpp_build_optional_components>`.

.. seealso::
	Full list of optional flags: :ref:`cpp_build_optional_components`

R and Python have specific lists of flags in their respective builds
that need to be included. You can find the links at the end
of this section.

In general on Python side, the options are set with CMake flags and
paths with environment variables. In R the environment variables are used
for all things connected to the build, also for setting CMake flags.

Building from source vs. using binaries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Using binaries is a fast and simple way of working with the last release
of Arrow. However, if you use these it means that you will be unable to
make changes to the Arrow C++ library.

**Note:** every language has its own way of dealing with binaries.
To get more information navigate to the section of the language you are
interested to find more information.

.. tabs::

   .. tab:: Building Pyarrow

      After building the Arrow C++ library, you need to build PyArrow on top
      of it also. The reason is the same; so you can edit the code and run
      tests on the edited code you have locally.

      **Why do we have to do builds separately?**

      As mentioned at the beginning of this page, the Python part of the Arrow
      project is built on top of the C++ library. In order to make changes in
      the Python part of Arrow as well as the C++ part of Arrow, you need to
      build them separately..

      We hope this introduction was enough to help you start with the building
      process.

      .. seealso::
         Follow the instructions to build PyArrow together with the C++ library

         - :ref:`build_pyarrow`

         Or

         - :ref:`build_pyarrow_win`

   .. tab:: Building the R package

