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
.. This section is intended to give some ideas on how to
.. work and find way around the Arrow library depending
.. on the type of the problem (simple binding, adding a
.. new feature, writing a test, …).


.. _arrow-codebase:

********************************
Working on the Arrow codebase 🧐
********************************

Finding your way around Arrow
=============================

The Apache Arrow repository includes implementations for
most of the libraries for which Arrow is available.

Languages like GLib (``c_glib/``), C++ (``cpp/``), C# (``csharp/``),
Go (``go/``), Java (``java/``), JavaScript (``js/``), MATLAB
(``matlab/``), Python (``python/``), R (``r/``) and Ruby (``ruby/``)
have their own subdirectories in the main folder as written here.

Rust has its own repository available `here <https://github.com/apache/arrow-rs>`_.

In the **language-specific subdirectories** you can find the code
connected to that language. For example:

- The ``python/`` folder includes ``pyarrow/`` folder which contains
  the code for the pyarrow package and requirements files that you
  need when building pyarrow.

  The ``pyarrow/`` includes Python and Cython code.

  The ``pyarrow/`` also includes ``test/`` folder where all the tests
  for the pyarrow modules are located.

- The ``r/`` directory contains the R package.

Other subdirectories included in the arrow repository are:

- ``ci/`` contains scripts used by the various continuous
  integration (CI) jobs.
- ``dev/`` contains scripts useful to developers when packaging,
  testing, or committing to Arrow, as well as definitions for
  extended continuous integration (CI) tasks.
- ``.github/`` contains workflows run on GitHub continuous
  integration (CI), triggered by certain actions such as opening a PR.
- ``docs/`` contains most of the documentation. Read more on
  :ref:`documentation`.
- ``format/`` contains binary protocol definitions for the
  Arrow columnar format and other parts of the project,
  like the Flight RPC framework.


Bindings, features, fixes and tests
===================================

You can read through this section to get some ideas on how
to work around the library on the issue you have.

Depending on the problem you want to solve (adding a simple
binding, adding a feature, writing a test, …) there are
different ways to get the necessary information.

**For all the cases** you can help yourself with
searching for functions via some kind of search tool.
In our experience there are two good ways:

#. Via **GitHub Search** in the Arrow repository (not a forked one)
   This way is great as GitHub lets you search for function
   definitions and references also.

#. **IDE** of your choice.

**Bindings**

The term "binding" is used to refer to a function in the C++ implementation which
can be called from a function in another language.  After a function is defined in
C++ we must create the binding manually to use it in that implementation.

.. note::
	There is much you can learn by checking **Pull Requests**
	and **unit tests** for similar issues.

.. tab-set::

   .. tab-item:: Python

      **Adding a fix in Python**

      If you are updating an existing function, the
      easiest way is to run Python interactively or run Jupyter
      Notebook and research
      the issue until you understand what needs to be done.

      After, you can search on GitHub for the function name, to
      see where the function is defined.

      Also, if there are errors produced, the errors will most
      likely point you towards the file you need to take a look at.

      **Python - Cython - C++**

      It is quite likely that you will bump into Cython code when
      working on Python issues. It's less likely is that the C++ code
      needs updating, though it can happen.

      As mentioned before, the underlying code is written in C++.
      Python then connects to it via Cython. If you
      are not familiar with it you can ask for help and remember,
      **look for similar Pull Requests and GitHub issues!**

      **Adding tests**

      There are some issues where only tests are missing. Here you
      can search for similar functions and see how the unit tests for
      those functions are written and how they can apply in your case.

      This also holds true for adding a test for the issue you have solved.

      **New feature**

      If you are adding a new future in Python you can look at
      the :ref:`tutorial <python_tutorial>` for ideas.

   .. tab-item:: R

      **Philosophy behind R bindings**

      When writing bindings between C++ compute functions and R functions,
      the aim is to expose the C++ functionality via the same interface as
      existing R functions.

      To read the full content on the topic of R bindings read through the
      `Writing Bindings article <https://arrow.apache.org/docs/r/articles/developers/bindings.html>`_.
