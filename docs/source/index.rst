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

.. grid:: 3
   :gutter: 4
   :padding: 2 2 0 0
   :class-container: sd-text-center

   .. grid-item-card:: Format
      :img-top: ./images/file-pen.svg
      :class-card: contrib-card
      :shadow: md

      Read about the Apache Arrow format
      specifications and protocols.

      +++

      .. button-ref:: format
         :ref-type: ref
         :click-parent:
         :color: secondary
         :expand:

         To the specifications

   .. grid-item-card:: Developing
      :class-card: contrib-card
      :shadow: md

      Find the documentation on the topic of
      contributions, reviews, building of the libraries
      from source, building of the documentation, 
      continuous integration, benchmarks and the
      release process.

      +++

      .. button-ref:: developers
         :ref-type: ref
         :click-parent:
         :color: secondary
         :expand:

         To the documentation 
   
   .. grid-item-card:: Supported Environments
      :img-top: ./developers/images/code-solid.svg
      :class-card: contrib-card
      :shadow: md

      Read the documentation for your target
      environment.

      +++

      .. button-ref:: env-index
         :ref-type: ref
         :click-parent:
         :color: secondary
         :expand:

         To the projects

.. _toc.columnar:

Apache Arrow Columnar Format
----------------------------

.. toctree::
   :maxdepth: 2

   format/index

Development Guide
-----------------

.. _toc.development:

.. toctree::
   :maxdepth: 2

   developers/index

Documentation for supported environments
----------------------------------------

.. _toc.usage:

.. toctree::
   :maxdepth: 2

   env-index
