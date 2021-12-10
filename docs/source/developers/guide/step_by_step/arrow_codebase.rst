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
Go (``go/``), Java (``java/``), JavaScript (``js/``),
Julia (``julia/``), MATLAB (``matlab/``, Python (``python/``),
R (``r/``) and Ruby (``ruby/``) have their own subdirectories in
the main folder as written here.

Rust has it's own repository available `here <https://github.com/apache/arrow-rs>`_.

In the **language-specific subdirectories** you can find the code
connected to that language. For example:

- The ``python/`` folder includes ``pyarrow/`` folder which contains
  the code for the pyarrow package and requirements files that you
  need when building pyarrow.

  The ``pyarrow/`` includes all pyarrow modules, for example
  ``compute.py`` for the ``pyarrow.compute`` module. The ``pyarrow/``
  includes Python and Cython code.

  The ``pyarrow/`` also includes ``test/`` folder where all the tests
  for the pyarrow modules are located.

- The ``R/`` folder holds the code and the documentation for the R package.

  The documentation can be found in ``vignettes/`` and in ``R/`` folder
  you can find the reference documentation.

  The ``R/`` folder also includes tests for the R package
  in ``tests/`` and ``extra-tests/``.

Other subdirectories included in the arrow repository are:

- ``ci/`` contains code connected to the CI work.
- ``dev/`` contains scripts useful to developers when packaging,
  testing, or committing to Arrow.
- ``docs/`` contains most of the documentation. Read more on
  :ref:`documentation`.
- ``format/`` contains the Arrow Protocol files.

Other files included in Arrow are connected to either GitHub,
CI builds, docker or Archery.

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

**Binding**

Binding means that the function in the C++ is connected from 
other languages (C (Glib), MATLAB, Python, R or Ruby). Once a 
function is defined in the C++ we connect it from other languages
so that it can be used there also.

.. note::
	There is much you can learn with checking **Pull Requests**
	and **unit tests for similar issues**.  

.. tabs::

   .. tab:: Python

      **Adding a fix in Python**

      If you are doing a correction of an existing function, the
      easiest way is to run Python interactively or run Jupyter
      Notebook from the Python folder in Arrow and research
      the issue until you understand what needs to be done.

      After, you can search on GitHub for the function name, to
      see where the function is defined.

      Also, if there are errors produced, the errors will most
      likely point you towards the file you need to take a look at.

      **Python - Cython - C++**
       
      It is quite likely that you will bump into Cython code when
      working on Python issues. Less likely is that C++ code would
      need some correction, but it can happen.

      As mentioned before, the underlying code is written in C++.
      Python then connects to it via Cython. If you
      are not familiar with it you can ask for help and remember,
      **look for similar Pull Requests and JIRA issues!**

      **Adding tests**

      There are some issues where only tests are missing. Here you
      can search for similar functions and see how the unit tests for
      those functions are written and how they can apply in your case.

      This also hold true for adding a test for the issue you have solved.

   .. tab:: R package

      .. - **Philosophy behind R bindings**
      .. TODO

      .. #. New feature
      ..   If you are adding a new future in R or Python you can check out
      ..   our tutorials (link!) where we are adding a simple feature to Python and R.