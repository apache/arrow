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
.. cpp:namespace:: gandiva

===============================
The Gandiva Expression Compiler
===============================

Gandiva is a runtime expression compiler that uses `LLVM`_ to generate
efficient native code for compute on Arrow record batches.
Gandiva only handles projections and filters; for other transformations, see
:ref:`Compute Functions <compute-cpp>`.

Gandiva was designed to take advantage of the Arrow memory format and modern
hardware. From the Arrow memory model, since Arrow arrays have separate buffers for values and 
validity bitmaps, values and their null status can often be processed 
independently, allowing for better instruction pipelining. On modern hardware,
compiling expressions using LLVM allows the execution to be optimized
to the local runtime environment and hardware, including available SIMD
instructions. To reduce optimization overhead, many Gandiva functions are
pre-compiled into LLVM IR (intermediate representation).

.. _LLVM: https://llvm.org/


Expression, Projector and Filter
================================
To effectively utilize Gandiva, you will construct expression trees with ``TreeExprBuilder``, 
including the creation of function nodes, if-else logic, and boolean expressions. 
Subsequently, leverage ``Projector`` or ``Filter`` execution kernels to efficiently evaluate these expressions.
See :doc:`./gandiva/expr_projector_filter` for more details. 


External Functions Development
==============================
Gandiva offers the capability of integrating external functions, encompassing 
both C functions and IR functions. This feature broadens the spectrum of 
functions that can be applied within Gandiva expressions. For developers 
looking to customize and enhance their computational solutions, 
Gandiva provides the opportunity to develop and register their own external 
functions, thus allowing for a more tailored and flexible use of the Gandiva 
environment.
See :doc:`./gandiva/external_func` for more details. 

.. toctree::
   :maxdepth: 2

   gandiva/expr_projector_filter
   gandiva/external_func