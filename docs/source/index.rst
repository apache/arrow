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

Apache Arrow is a development platform for in-memory analytics. It contains a
set of technologies that enable big data systems to process and move data
fast. It specifies a standardized language-independent columnar memory format
for flat and hierarchical data, organized for efficient analytic operations on
modern hardware.

The project is developing a multi-language collection of libraries for solving
systems problems related to in-memory analytical data processing. This includes
such topics as:

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
---------------

.. _toc.usage:

.. toctree::
   :maxdepth: 1

   C/GLib <c_glib/index>
   C++ <cpp/index>
   C# <https://github.com/apache/arrow/blob/main/csharp/README.md>
   Go <https://pkg.go.dev/github.com/apache/arrow/go/v16>
   Java <java/index>
   JavaScript <js/index>
   Julia <https://arrow.apache.org/julia/>
   MATLAB <https://github.com/apache/arrow/blob/main/matlab/README.md>
   Python <python/index>
   R <r/index>
   Ruby <https://github.com/apache/arrow/blob/main/ruby/README.md>
   Rust <https://docs.rs/crate/arrow/>
   status

Examples
--------

.. _toc.cookbook:

.. toctree::
   :maxdepth: 1

   C++ cookbook <https://arrow.apache.org/cookbook/cpp/>
   Java cookbook <https://arrow.apache.org/cookbook/java/>
   Python cookbook <https://arrow.apache.org/cookbook/py/>
   R cookbook <https://arrow.apache.org/cookbook/r/>
