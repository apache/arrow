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

.. _toc.usage:

.. toctree::
   :maxdepth: 1
   :caption: Supported Environments

   C/GLib <https://arrow.apache.org/docs/c_glib/>
   C++ <cpp/index>
   C# <https://github.com/apache/arrow/blob/master/csharp/README.md>
   Go <https://godoc.org/github.com/apache/arrow/go/arrow>
   Java <java/index>
   JavaScript <https://arrow.apache.org/docs/js/>
   Julia <https://github.com/apache/arrow/blob/master/julia/Arrow/README.md>
   MATLAB <https://github.com/apache/arrow/blob/master/matlab/README.md>
   Python <python/index>
   R <https://arrow.apache.org/docs/r/>
   Ruby <https://github.com/apache/arrow/blob/master/ruby/README.md>
   Rust <https://docs.rs/crate/arrow/>
   status

.. _toc.columnar:

.. toctree::
   :maxdepth: 2
   :caption: Specifications and Protocols

   format/Versioning
   format/Columnar
   format/Flight
   format/Integration
   format/CDataInterface
   format/CStreamInterface
   format/Other   

.. _toc.development:

.. toctree::
   :maxdepth: 2
   :caption: Development

   developers/contributing
   developers/cpp/index
   developers/python
   developers/archery
   developers/crossbow
   developers/docker
   developers/benchmarks
   developers/documentation
