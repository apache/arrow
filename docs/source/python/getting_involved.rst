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

Getting Involved
================

Right now the primary audience for Apache Arrow are the developers of data
systems; most people will use Apache Arrow indirectly through systems that use
it for internal data handling and interoperating with other Arrow-enabled
systems.

Even if you do not plan to contribute to Apache Arrow itself or Arrow
integrations in other projects, we'd be happy to have you involved:

* Join the mailing list: send an email to
  `dev-subscribe@arrow.apache.org <mailto:dev-subscribe@arrow.apache.org>`_.
  Share your ideas and use cases for the project or read through the
  `Archive <http://mail-archives.apache.org/mod_mbox/arrow-dev/>`_.
* Follow our activity on `GitHub <https://github.com/apache/arrow/issues>`_
* Learn the `Format / Specification
  <https://github.com/apache/arrow/tree/main/format>`_


.. _py_arch_overview:

PyArrow Architecture
--------------------

PyArrow is for the major part a wrapper around the functionalities that
Arrow C++ implementation provides. The library tries to take what's available
in C++ and expose it through a user experience that is more pythonic and
less complex to use. So while in some cases it might be easy to map what's
in C++ to what's in Python, in many cases the C++ classes and methods are
used as foundations to build easier to use entities.

.. image:: /python/images/py_arch_overview.svg
   :alt: Four layers of PyArrow architecture: .py, .pyx, .pxd and low level C++ code.

* The ``*.py`` files in the pyarrow package are usually where the entities
  exposed to the user are declared. In some cases, those files might directly
  import the entities from inner implementation if they want to expose it
  as is without modification.
* The ``lib.pyx`` file is where the majority of the core C++ libarrow 
  capabilities are exposed to Python. Most of the implementation of this
  module relies on included ``*.pxi`` files where the specificic pieces
  are built. While being exposed to Python as ``pyarrow.lib`` its content
  should be considered internal. The public classes are then directly exposed
  in other modules (like ``pyarrow`` itself) by virtue of importing them from
  ``pyarrow.lib``
* The ``_*.pyx`` files are where the glue code is usually created, it puts
  together the C++ capabilities turning it into Python classes and methods.
  They can be considered the internal implementation of the capabilities
  exposed by the ``*.py`` files.
* The ``includes/*.pxd`` files are where the raw C++ library APIs are declared
  for usage in Cython. Here the C++ classes and methods are declared as they are
  so that in the other ``.pyx`` files they can be used to implement Python classes,
  functions and helpers.
* Apart from Arrow C++ library, which dependence is mentioned in the previous line,
  PyArrow is also based on PyArrow C++, dedicated pieces of code that live in
  ``python/pyarrow/src/arrow/python`` directory and provide the low level
  code for capabilities like converting to and from numpy or pandas and the classes
  that allow to use Python objects and callbacks in C++.