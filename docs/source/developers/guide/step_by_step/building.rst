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


.. _build-arrow:

************************************
Building the Arrow's libraries 🏋🏿‍♀️
************************************

The Arrow project contains good number of libraries that enable
work in many languages. Most libraries (C++, C#, Go, Java,
JavaScript, Julia, and Rust) already contain distinct implementations
of Arrow. 

It is different for C (Glib), MATLAB, Python, R, and Ruby as they
are built on top of the C++ library. In this section we will be
dealing with this second part of the libraries plus some of C++.

In this case, if you decide to contribute to Arrow you will meet
the topic of compiling the source code and use of CMake. You may
have some experience with it or not. If not, it is good to read
through this part so you understand what is happening in the process
of building Arrow better.

If you feel comfortable with compiling then feel free to proceed
to the :ref:`C++ <building-arrow-cpp>`, :ref:`PyArrow <build_pyarrow>` or
`R package build section <https://arrow.apache.org/docs/r/articles/developing.html>`_.

Building Arrow C++
==================

Why build Arrow C++ from source?
--------------------------------

For Arrow C++ implementation and its bindings (Python and R for example)
these bindings are wrapping the underlying C++ functions. Even if you
want to work on PyArrow or R package the source code of C++ may have to
be edited also.

About CMake
-----------

CMake is a cross platform build system generator and it defers
to another program such as ``make`` or ``ninja`` for the actual build.
If running into errors with the build process, the first thing to try is
to tweak some CMake flags for compiling Arrow.

.. TODO
.. CMake presets
.. Ex: "We now have CMake Presets which are useful starting points."
.. https://github.com/apache/arrow/blob/master/docs/source/developers/cpp/building.rst#cmake-presets


Optional flags and why might we use them
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. TODO short description of the use of flags
.. R and Python have specific lists of flags in their respective building docs that can be referenced.

.. seealso::
	Full list of optional flags: :ref:`cpp_build_optional_components`

.. Environment variables useful for developers
.. ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. TODO short description of the use of env vars

Building from source vs. using binaries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Using binaries is a fast and simple way of working with the latest
Arrow version. But you do not have the possibility to add or change
the code and as a contributor you will need to.

Detailed instructions on building C++ library from source can
be found :ref:`here <building-arrow-cpp>`.

.. _build-pyarrow:

Building PyArrow
================

After building Arrow C++ part of Arrow you have to build PyArrow on top
of it also. The reason is the same, so you can edit the code and run
tests on the edited code you have locally.

**Why do we have to do builds separately?**

As mentioned in the beginning of this page, Python part of the Arrow
project is built on top of C++. In order to make changes in Python part
of Arrow as well as C++ part of Arrow, we need to build them separately.

We hope this introduction was enough to help you start with the building
process.

.. seealso::
	Follow the instructions to build PyArrow together with the C++ library

	- :ref:`build_pyarrow`
	Or

	- :ref:`build_pyarrow_win`

.. _build-rapackage:

Building R package
==================