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

Apache Arrow is a cross-language development platform for in-memory data. It
specifies a standardized language-independent columnar memory format for flat
and hierarchical data, organized for efficient analytic operations on modern
hardware. It also provides computational libraries and zero-copy streaming
messaging and interprocess communication.

.. toctree::
   :maxdepth: 1
   :caption: Memory Format

   format/README
   format/Guidelines
   format/Layout
   format/Metadata
   format/IPC

.. toctree::
   :maxdepth: 2
   :caption: Languages

   cpp/index
   python/index
