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

.. _api.acero:
.. currentmodule:: pyarrow.acero

Acero - Streaming Execution Engine
==================================

.. warning::

    Acero is experimental and a stable API is not yet guaranteed.

Acero is a streaming query engine, which allows the computation to be expressed
as an "execution plan" (constructed using the :class:`Declaration` interface).
This enables to create a computation composed of ``pyarrow.compute`` functions
and to execute this efficiently in a batched manner.

.. autosummary::
   :toctree: ../generated/

   Declaration
   ExecNodeOptions
   TableSourceNodeOptions
   ScanNodeOptions
   FilterNodeOptions
   ProjectNodeOptions
   AggregateNodeOptions
   OrderByNodeOptions
   HashJoinNodeOptions


.. seealso::

   :doc:`Acero C++ user guide <../../cpp/streaming_execution>`

   :ref:`api.substrait`
      Alternative way to run Acero from a standardized Substrait plan.
