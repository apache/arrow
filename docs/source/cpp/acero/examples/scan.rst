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

.. default-domain:: cpp
.. highlight:: cpp
.. cpp:namespace:: arrow::compute

.. _stream_execution_scan_docs:

====
Scan
====

``scan`` is an operation used to load and process datasets.  It should be preferred over the
more generic ``source`` node when your input is a dataset.  The behavior is defined using 
:class:`arrow::dataset::ScanNodeOptions`.  More information on datasets and the various
scan options can be found in :doc:`../dataset`.

This node is capable of applying pushdown filters to the file readers which reduce
the amount of data that needs to be read.  This means you may supply the same
filter expression to the scan node that you also supply to the FilterNode because
the filtering is done in two different places.

Example:

.. literalinclude:: ../../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Scan Example)
  :end-before: (Doc section: Scan Example)
  :linenos:
  :lineno-match:
