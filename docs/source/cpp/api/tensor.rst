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

=======
Tensors
=======

Dense Tensors
=============

.. doxygenclass:: arrow::Tensor
   :members:

.. doxygenclass:: arrow::NumericTensor
   :members:

Sparse Tensors
==============

.. doxygenenum:: arrow::SparseTensorFormat::type

.. doxygenclass:: arrow::SparseIndex
   :members:

.. doxygenclass:: arrow::SparseCOOIndex
   :members:

.. doxygenclass:: arrow::SparseCSRIndex
   :members:

.. doxygenclass:: arrow::SparseTensor
   :members:

.. doxygenclass:: arrow::SparseTensorImpl
   :members:

.. doxygentypedef:: arrow::SparseCOOTensor

.. doxygentypedef:: arrow::SparseCSCMatrix

.. doxygentypedef:: arrow::SparseCSFTensor

.. doxygentypedef:: arrow::SparseCSRMatrix
