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

.. _toc.libraries:

.. toctree::
   :maxdepth: 2
   :hidden:

   libraries/index

Libraries
=========

The Apache Arrow project provides libraries in many languages. For more details, see the full
`Libraries documentation <libraries/index>`__.

- `C/GLib <c_glib/index>`__
- `C++ <cpp/index>`__
- `C# <https://github.com/apache/arrow/blob/main/csharp/README.md>`__
- `Go <https://arrow.apache.org/go/>`__
- `Java <java/index>`__
- `JavaScript <js/index>`__
- `Julia <https://arrow.apache.org/julia/>`__
- `MATLAB <https://github.com/apache/arrow/blob/main/matlab/README.md>`__
- `nanoarrow <https://arrow.apache.org/nanoarrow/>`__
- `Python <python/index>`__
- `R <r/index>`__
- `Ruby <https://github.com/apache/arrow/blob/main/ruby/README.md>`__
- `Rust <https://docs.rs/crate/arrow/>`__

Examples
--------

The Apache Arrow project provides example code in multiple languages. 

- `C++ cookbook <https://arrow.apache.org/cookbook/cpp/>`__
- `Java cookbook <https://arrow.apache.org/cookbook/java/>`__
- `Python cookbook <https://arrow.apache.org/cookbook/py/>`__
- `R cookbook <https://arrow.apache.org/cookbook/r/>`__
