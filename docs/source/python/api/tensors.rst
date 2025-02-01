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

.. _api.tensor:

Tensors
=======

PyArrow supports both dense and sparse tensors. Dense tensors store all data values explicitly, while sparse tensors represent only the non-zero elements and their locations, making them efficient for storage and computation.

Dense Tensors
-------------

.. autosummary::
   :toctree: ../generated/

   Tensor

Sparse Tensors
--------------

PyArrow supports the following sparse tensor formats:

.. autosummary::
   :toctree: ../generated/

   SparseCOOTensor
   SparseCSRMatrix
   SparseCSCMatrix
   SparseCSFTensor

SparseCOOTensor
^^^^^^^^^^^^^^^

The ``SparseCOOTensor`` represents a sparse tensor in Coordinate (COO) format, where non-zero elements are stored as tuples of row and column indices.

For detailed examples, see :ref:`data/SparseCOOTensor`.

SparseCSRMatrix
^^^^^^^^^^^^^^^

The ``SparseCSRMatrix`` represents a sparse matrix in Compressed Sparse Row (CSR) format. This format is useful for matrix-vector multiplication.

For detailed examples, see :ref:`data/SparseCSRMatrix`

SparseCSCMatrix
^^^^^^^^^^^^^^^

The ``SparseCSCMatrix`` represents a sparse matrix in Compressed Sparse Column (CSC) format, where data is stored by columns.

For detailed examples, see :ref:`data/SparseCSCMatrix`.

SparseCSFTensor
^^^^^^^^^^^^^^^

The ``SparseCSFTensor`` represents a sparse tensor in Compressed Sparse Fiber (CSF) format, which is a generalization of the CSR format for higher dimensions.

For detailed examples, see :ref:`data/SparseCSFTensor`.