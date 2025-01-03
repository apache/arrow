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

### SparseCOOTensor

The `SparseCOOTensor` represents a sparse tensor in Coordinate (COO) format, where non-zero elements are stored as tuples of row and column indices.

Example:
.. code-block:: python

   import pyarrow as pa

   indices = pa.array([[0, 0], [1, 2]])
   data = pa.array([1, 2])
   shape = (2, 3)

   tensor = pa.SparseCOOTensor(indices, data, shape)
   print(tensor.to_dense())

### SparseCSRMatrix

The `SparseCSRMatrix` represents a sparse matrix in Compressed Sparse Row (CSR) format. This format is useful for matrix-vector multiplication.

Example:
.. code-block:: python

   import pyarrow as pa

   data = pa.array([1, 2, 3])
   indptr = pa.array([0, 2, 3])
   indices = pa.array([0, 2, 1])
   shape = (2, 3)

   sparse_matrix = pa.SparseCSRMatrix.from_numpy(data, indptr, indices, shape)
   print(sparse_matrix)

### SparseCSCMatrix

The `SparseCSCMatrix` represents a sparse matrix in Compressed Sparse Column (CSC) format, where data is stored by columns.

Example:
.. code-block:: python

   import pyarrow as pa

   data = pa.array([1, 2, 3])
   indptr = pa.array([0, 1, 3])
   indices = pa.array([0, 1, 2])
   shape = (3, 2)

   sparse_matrix = pa.SparseCSCMatrix.from_numpy(data, indptr, indices, shape)
   print(sparse_matrix)

### SparseCSFTensor

The `SparseCSFTensor` represents a sparse tensor in Compressed Sparse Fiber (CSF) format, which is a generalization of the CSR format for higher dimensions.

Example:
.. code-block:: python

   import pyarrow as pa

   data = pa.array([1, 2, 3])
   indptr = [pa.array([0, 1, 3]), pa.array([0, 2, 3])]
   indices = [pa.array([0, 1]), pa.array([0, 1, 2])]
   shape = (2, 3, 2)

   sparse_tensor = pa.SparseCSFTensor.from_numpy(data, indptr, indices, shape)
   print(sparse_tensor)