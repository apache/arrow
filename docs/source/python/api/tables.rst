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

.. currentmodule:: pyarrow

.. _api.table:

Tables and Tensors
==================

Factory Functions
-----------------

.. autosummary::
   :toctree: ../generated/

   chunked_array
   concat_arrays
   concat_tables
   record_batch
   concat_batches
   table

Classes
-------

.. autosummary::
   :toctree: ../generated/

   ChunkedArray
   RecordBatch
   Table
   TableGroupBy
   RecordBatchReader

Dataframe Interchange Protocol
------------------------------

.. autosummary::
   :toctree: ../generated/

   interchange.from_dataframe

.. _api.tensor:

Tensors
-------

.. autosummary::
   :toctree: ../generated/

   Tensor
