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
.. This section should include architectural overview
.. of the Arrow project. If possible the content should be
.. written in words easy understandable to beginner not
.. necessary acquainted with the library and the technical
.. details.

.. _architectural_overview:

**********************
Architectural Overview
**********************

A general overview of Apache Arrow project can be found on the 
`front page <https://arrow.apache.org/>`_ and in the 
`Apache Arrow Overview <https://arrow.apache.org/overview/>`_.
You can also have a look at the
`Frequently Asked Questions <https://arrow.apache.org/faq/>`_.

For an Architectural Overview of Arrow's libraries please
refer to:

- :ref:`py_arch_overview`
- R package Architecture can be found on this page.


R package Architectural Overview
--------------------------------

.. figure:: /developers/images/R_architectural_overview.png
   :alt: Main parts of R package architecture: dplyr-*,
         dplyr-funcs*, tools, tests and src/.

* The ``r/R/dplyr-*`` files define the verbs used in a regular
  dplyr syntax on Arrow objects.
* The ``r/R/dplyr-funcs*`` files define bindings to Arrow C++
  functions that can be used with already defined dplyr verbs.
* All the C++ code connected to the R package lives in ``arrow/r/src``.
  It also includes C++ code which connects libarrow (the Arrow C++
  library) and the R code in package.
* If the libarrow source package is bundled with R package using
  ``make sync-cpp`` command then it will be included in the
  ``r/tools/cpp`` folder.

**Additionally**

* The ``r/man`` directory includes generated R documentation that
  shouldn't be updated directly but in the corresponding ``.R`` file.
* The vignettes are
  `"a long-form guide to the package" <https://r-pkgs.org/vignettes.html#introduction>`_
  and can be found in ``r/vignettes``.
