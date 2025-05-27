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

:html_theme.sidebar_secondary.remove:

Apache Arrow
============

Apache Arrow is a universal columnar format and multi-language toolbox for fast
data interchange and in-memory analytics.

The project specifies a language-independent column-oriented memory format
for flat and hierarchical data, organized for efficient analytic operations on
modern hardware. The project houses an actively developed collection of
libraries in many languages for solving problems related to data transfer and
in-memory analytical processing. This includes such topics as:

* Zero-copy shared memory and RPC-based data movement
* Reading and writing file formats (like CSV, Apache ORC, and Apache Parquet)
* In-memory analytics and query processing

**To learn how to use Arrow refer to the documentation specific to your
target environment.**

.. grid:: 1 2 2 2
   :gutter: 4
   :padding: 2 2 0 0
   :class-container: sd-text-center

   .. grid-item-card:: Specifications
      :class-card: contrib-card
      :shadow: none

      Read about the Apache Arrow format and its related specifications and
      protocols.

      +++

      .. button-ref:: format
         :ref-type: ref
         :click-parent:
         :color: primary
         :expand:

         To Specifications

   .. grid-item-card:: Development
      :class-card: contrib-card
      :shadow: none

      Find documentation on building the libraries from source, building the
      documentation, contributing and code reviews, continuous integration,
      benchmarking, and the release process.

      +++

      .. button-ref:: developers
         :ref-type: ref
         :click-parent:
         :color: primary
         :expand:

         To Development

.. _toc.columnar:

.. toctree::
   :maxdepth: 2
   :hidden:

   format/index

.. _toc.development:

.. toctree::
   :maxdepth: 2
   :hidden:

   developers/index


Implementations
===========

Documentation and source code for Apache Arrow libraries

.. list-table::
   :header-rows: 1

   * - Language
     - Docs
     - Source
   * - C++
     - `C++ Docs <https://arrow.apache.org/docs/cpp>`_
     - `C++ Source <https://github.com/apache/arrow/tree/main/cpp>`_
   * - C GLib
     - `C GLib Docs <https://arrow.apache.org/docs/c_glib>`_
     - `C GLib Source <https://github.com/apache/arrow/tree/main/c_glib>`_
   * - C#
     - `C# Docs <https://github.com/apache/arrow/blob/main/csharp/README.md>`_
     - `C# Source <https://github.com/apache/arrow/tree/main/csharp>`_
   * - Go
     - `Go Docs <https://godoc.org/github.com/apache/arrow/go/arrow>`_
     - `Go Source <https://github.com/apache/arrow-go>`_
   * - Java
     - `Java Docs <https://arrow.apache.org/docs/java>`_
     - `Java Source <https://github.com/apache/arrow-java>`_
   * - JavaScript
     - `JavaScript Docs <https://arrow.apache.org/docs/js>`_
     - `JavaScript Source <https://github.com/apache/arrow-js>`_
   * - Julia
     - `Julia Docs <https://arrow.apache.org/julia/>`_
     - `Julia Source <https://github.com/apache/arrow-julia>`_
   * - MATLAB
     - `MATLAB Docs <https://github.com/apache/arrow/blob/main/matlab/README.md>`_
     - `MATLAB Source <https://github.com/apache/arrow/tree/main/matlab>`_
   * - Python
     - `Python Docs <https://arrow.apache.org/docs/python>`_
     - `Python Source <https://github.com/apache/arrow/tree/main/python>`_
   * - R
     - `R Docs <https://arrow.apache.org/docs/r>`_
     - `R Source <https://github.com/apache/arrow/tree/main/r>`_
   * - Ruby
     - `Ruby Docs <https://github.com/apache/arrow/blob/main/ruby/README.md>`_
     - `Ruby Source <https://github.com/apache/arrow/tree/main/ruby>`_
   * - Rust
     - `Rust Docs <https://docs.rs/arrow/latest>`_
     - `Rust Source <https://github.com/apache/arrow-rs>`_


Examples
--------

The Apache Arrow project provides example code in multiple languages.

- `C++ cookbook <https://arrow.apache.org/cookbook/cpp/>`__
- `Java cookbook <https://arrow.apache.org/cookbook/java/>`__
- `Python cookbook <https://arrow.apache.org/cookbook/py/>`__
- `R cookbook <https://arrow.apache.org/cookbook/r/>`__
