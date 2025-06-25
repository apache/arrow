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

Conversion of ``RecordBatch`` to ``Tensor`` instances
=====================================================

Arrow provides a method to convert ``RecordBatch`` objects to a ``Tensor``
with two dimensions:

.. code::

   std::shared_ptr<RecordBatch> batch;

   ASSERT_OK_AND_ASSIGN(auto tensor, batch->ToTensor());
   ASSERT_OK(tensor->Validate());

The conversion supports signed and unsigned integer types plus float types.
In case the ``RecordBatch`` has null values the conversion succeeds if
``null_to_nan`` parameter is set to ``true``. In this case all
types will be promoted to a floating-point data type.

.. code::

   std::shared_ptr<RecordBatch> batch;

   ASSERT_OK_AND_ASSIGN(auto tensor, batch->ToTensor(/*null_to_nan=*/true));
   ASSERT_OK(tensor->Validate());

Currently only column-major conversion is supported.
