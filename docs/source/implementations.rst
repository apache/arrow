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

.. _implementations:

===============
Implementations
===============

Official Implementations
========================

The Apache Arrow project houses a collection of libraries for different
programming languages. Use the links in the table below to access the
documentation and source code for these libraries.

.. list-table::
   :header-rows: 1

   * - Language
     - Docs
     - Source
   * - C++
     - :doc:`C++ Docs<cpp/index>`
     - `C++ Source <https://github.com/apache/arrow/tree/main/cpp>`_
   * - C GLib
     - :doc:`C GLib Docs<c_glib/index>`
     - `C GLib Source <https://github.com/apache/arrow/tree/main/c_glib>`_
   * - C#
     - `C# Docs <https://github.com/apache/arrow/blob/main/csharp/README.md>`_ :fa:`external-link-alt`
     - `C# Source <https://github.com/apache/arrow/tree/main/csharp>`_
   * - Go
     - `Go Docs <https://godoc.org/github.com/apache/arrow/go/arrow>`_ :fa:`external-link-alt`
     - `Go Source <https://github.com/apache/arrow-go>`_
   * - Java
     - :doc:`Java Docs<java/index>`
     - `Java Source <https://github.com/apache/arrow-java>`_
   * - JavaScript
     - `JavaScript Docs <https://arrow.apache.org/docs/js>`_ :fa:`external-link-alt`
     - `JavaScript Source <https://github.com/apache/arrow-js>`_
   * - Julia
     - `Julia Docs <https://arrow.apache.org/julia/>`_ :fa:`external-link-alt`
     - `Julia Source <https://github.com/apache/arrow-julia>`_
   * - MATLAB
     - `MATLAB Docs <https://github.com/apache/arrow/blob/main/matlab/README.md>`_ :fa:`external-link-alt`
     - `MATLAB Source <https://github.com/apache/arrow/tree/main/matlab>`_
   * - Python
     - :doc:`Python Docs<python/index>`
     - `Python Source <https://github.com/apache/arrow/tree/main/python>`_
   * - R
     - `R Docs <r/index.html>`_ :fa:`external-link-alt`
     - `R Source <https://github.com/apache/arrow/tree/main/r>`_
   * - Ruby
     - `Ruby Docs <https://github.com/apache/arrow/blob/main/ruby/README.md>`_ :fa:`external-link-alt`
     - `Ruby Source <https://github.com/apache/arrow/tree/main/ruby>`_
   * - Rust
     - `Rust Docs <https://docs.rs/arrow/latest>`_ :fa:`external-link-alt`
     - `Rust Source <https://github.com/apache/arrow-rs>`_
   * - Swift
     - `Swift Docs <https://github.com/apache/arrow-swift/blob/main/Arrow/README.md>`_ :fa:`external-link-alt`
     - `Swift Source <https://github.com/apache/arrow-swift>`_

In addition to the libraries listed above, the Arrow project hosts the
**nanoarrow** subproject which provides a set of lightweight libraries
designed to help produce and consume Arrow data.

.. list-table::
   :header-rows: 0

   * - nanoarrow
     - `nanoarrow Docs <https://arrow.apache.org/nanoarrow>`_ :fa:`external-link-alt`
     - `nanoarrow Source <http://github.com/apache/arrow-nanoarrow>`_

Implementation Status
=====================

The :doc:`status` page provides an overview of the current capabilities of the
official Arrow libraries.

Cookbook
========

The Apache Arrow Cookbook is a collection of recipes for using the Arrow
libraries for different programming languages.

* `C++ Cookbook <https://arrow.apache.org/cookbook/cpp/>`_
* `Java Cookbook <https://arrow.apache.org/cookbook/java/>`_
* `Python Cookbook <https://arrow.apache.org/cookbook/py/>`_
* `R Cookbook <https://arrow.apache.org/cookbook/r/>`_

The source files for the Cookbook are maintained in the
`Apache Arrow Cookbooks repository <https://github.com/apache/arrow-cookbook>`_.

.. toctree::
   :maxdepth: 1
   :hidden:

   C++ <cpp/index>
   C GLib <c_glib/index>
   C# <https://github.com/apache/arrow/blob/main/csharp/README.md>
   Go <https://arrow.apache.org/go/>
   Java <java/index>
   JavaScript <https://arrow.apache.org/docs/js>
   Julia <https://arrow.apache.org/julia/>
   MATLAB <https://github.com/apache/arrow/blob/main/matlab/README.md>
   Python <python/index>
   R <https://arrow.apache.org/docs/r>
   Ruby <https://github.com/apache/arrow/blob/main/ruby/README.md>
   Rust <https://docs.rs/crate/arrow/>
   Swift <https://github.com/apache/arrow-swift/blob/main/Arrow/README.md>
   nanoarrow <https://arrow.apache.org/nanoarrow/>
   Implementation Status <status>
