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

.. _stream_execution_write_docs:

=====
Write
=====

The ``write`` node saves query results as a dataset of files in a
format like Parquet, Feather, CSV, etc. using the :doc:`../dataset`
functionality in Arrow. The write options are provided via the
:class:`arrow::dataset::WriteNodeOptions` which in turn contains
:class:`arrow::dataset::FileSystemDatasetWriteOptions`.
:class:`arrow::dataset::FileSystemDatasetWriteOptions` provides
control over the written dataset, including options like the output
directory, file naming scheme, and so on.

Example:

.. literalinclude:: ../../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Write Example)
  :end-before: (Doc section: Write Example)
  :linenos:
  :lineno-match:
