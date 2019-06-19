# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest
import sys

import numpy as np
import pyarrow as pa


@pytest.mark.parametrize('sparse_tensor_type', [
    pa.SparseTensorCSR,
    pa.SparseTensorCOO,
])
def test_sparse_tensor_attrs(sparse_tensor_type):
    data = np.array([
        [0, 1, 0, 0, 1],
        [0, 0, 0, 0, 0],
        [0, 0, 0, 1, 0],
        [0, 0, 0, 0, 0],
        [0, 3, 0, 0, 0],
    ])
    sparse_tensor = sparse_tensor_type.from_dense_numpy(data)

    assert sparse_tensor.ndim == 2
    assert sparse_tensor.size == 25
    assert sparse_tensor.shape == data.shape
    assert sparse_tensor.is_mutable
    assert sparse_tensor.dim_name(0) == b''
    assert sparse_tensor.dim_names == []
    assert sparse_tensor.non_zero_length == 4


def test_sparse_tensor_coo_base_object():
    sparse_tensor = pa.SparseTensorCOO.from_dense_numpy(np.random.randn(10, 4))
    n = sys.getrefcount(sparse_tensor)
    data, coords = sparse_tensor.to_numpy()  # noqa
    assert sys.getrefcount(sparse_tensor) == n + 1


def test_sparse_tensor_csr_base_object():
    sparse_tensor = pa.SparseTensorCSR.from_dense_numpy(np.random.randn(10, 4))
    n = sys.getrefcount(sparse_tensor)
    data, indptr, indices = sparse_tensor.to_numpy()  # noqa
    assert sys.getrefcount(sparse_tensor) == n + 1


@pytest.mark.skip
@pytest.mark.parametrize('sparse_tensor_type', [
    pa.SparseTensorCSR,
    pa.SparseTensorCOO,
])
def test_sparse_tensor_equals(sparse_tensor_type):
    def eq(a, b):
        assert a.equals(b)
        assert a == b
        assert not (a != b)

    def ne(a, b):
        assert not a.equals(b)
        assert not (a == b)
        assert a != b

    data = np.random.randn(10, 6)[::, ::2]
    sparse_tensor1 = sparse_tensor_type.from_dense_numpy(data)
    sparse_tensor2 = sparse_tensor_type.from_dense_numpy(np.ascontiguousarray(data))
    eq(sparse_tensor1, sparse_tensor2)
    data = data.copy()
    data[9, 0] = 1.0
    sparse_tensor2 = sparse_tensor_type.from_dense_numpy(np.ascontiguousarray(data))
    ne(sparse_tensor1, sparse_tensor2)


@pytest.mark.parametrize('dtype_str,arrow_type', [
    ('i1', pa.int8()),
    ('i2', pa.int16()),
    ('i4', pa.int32()),
    ('i8', pa.int64()),
    ('u1', pa.uint8()),
    ('u2', pa.uint16()),
    ('u4', pa.uint32()),
    ('u8', pa.uint64()),
    ('f2', pa.float16()),
    ('f4', pa.float32()),
    ('f8', pa.float64())
])
def test_sparse_tensor_coo_from_dense(dtype_str, arrow_type):
    dtype = np.dtype(dtype_str)
    data_vector = np.array([4, 9, 7, 5]).astype(dtype)
    coords = np.array([[0, 0], [1, 3], [0, 2], [1, 3]])
    data = np.array([[4, 0, 9, 0],
                     [0, 7, 0, 0],
                     [0, 0, 0, 0],
                     [0, 0, 0, 5]]).astype(dtype)
    tensor = pa.Tensor.from_numpy(data)

    # Test from numpy array
    sparse_tensor = pa.SparseTensorCOO.from_dense_numpy(data)
    repr(sparse_tensor)
    assert sparse_tensor.type == arrow_type
    result_data, result_coords = sparse_tensor.to_numpy()
    assert (data_vector == result_data).all()
    assert (result_coords == coords).all()

    # Test from Tensor
    sparse_tensor = pa.SparseTensorCOO.from_tensor(tensor)
    repr(sparse_tensor)
    assert sparse_tensor.type == arrow_type
    result_data, result_coords = sparse_tensor.to_numpy()
    assert (data_vector == result_data).all()
    assert (result_coords == coords).all()


@pytest.mark.parametrize('dtype_str,arrow_type', [
    ('i1', pa.int8()),
    ('i2', pa.int16()),
    ('i4', pa.int32()),
    ('i8', pa.int64()),
    ('u1', pa.uint8()),
    ('u2', pa.uint16()),
    ('u4', pa.uint32()),
    ('u8', pa.uint64()),
    ('f2', pa.float16()),
    ('f4', pa.float32()),
    ('f8', pa.float64())
])
def test_sparse_tensor_csr_from_dense(dtype_str, arrow_type):
    dtype = np.dtype(dtype_str)
    dense_data = np.array([[1, 0, 2],
                           [0, 0, 3],
                           [4, 5, 6]]).astype(dtype)

    data = np.array([1, 2, 3, 4, 5, 6]).astype(dtype)
    indptr = np.array([0, 2, 3, 6])
    indices = np.array([0, 2, 2, 0, 1, 2])
    tensor = pa.Tensor.from_numpy(dense_data)

    # Test from numpy array
    sparse_tensor = pa.SparseTensorCSR.from_dense_numpy(dense_data)
    repr(sparse_tensor)
    result_data, result_indptr, result_indices = sparse_tensor.to_numpy()
    assert (data == result_data).all()
    assert (indptr == result_indptr).all()
    assert (indices == result_indices).all()

    # Test from Tensor
    sparse_tensor = pa.SparseTensorCSR.from_tensor(tensor)
    repr(sparse_tensor)
    assert sparse_tensor.type == arrow_type
    result_data, result_indptr, result_indices = sparse_tensor.to_numpy()
    assert (data == result_data).all()
    assert (indptr == result_indptr).all()
    assert (indices == result_indices).all()


@pytest.mark.skip
@pytest.mark.parametrize('dtype_str,arrow_type', [
    ('i1', pa.int8()),
    ('i2', pa.int16()),
    ('i4', pa.int32()),
    ('i8', pa.int64()),
    ('u1', pa.uint8()),
    ('u2', pa.uint16()),
    ('u4', pa.uint32()),
    ('u8', pa.uint64()),
    ('f2', pa.float16()),
    ('f4', pa.float32()),
    ('f8', pa.float64())
])
def test_sparse_tensor_coo_numpy_roundtrip(dtype_str, arrow_type):
    dtype = np.dtype(dtype_str)
    data = np.array([4, 5, 7, 9]).astype(dtype)
    coords = np.array([[0, 0], [3, 3], [1, 1], [0, 2]]).astype('i8')
    shape = (4, 4)
    dim_names = ["x", "y"]

    sparse_tensor = pa.SparseTensorCOO.from_numpy(data, coords, shape,
                                                  dim_names)
    repr(sparse_tensor)
    assert sparse_tensor.type == arrow_type
    result_data, result_coords = sparse_tensor.to_numpy()
    assert (data == result_data).all()
    assert (coords == result_coords).all()


@pytest.mark.parametrize('dtype_str,arrow_type', [
    ('i1', pa.int8()),
    ('i2', pa.int16()),
    ('i4', pa.int32()),
    ('i8', pa.int64()),
    ('u1', pa.uint8()),
    ('u2', pa.uint16()),
    ('u4', pa.uint32()),
    ('u8', pa.uint64()),
    ('f2', pa.float16()),
    ('f4', pa.float32()),
    ('f8', pa.float64())
])
def test_sparse_tensor_csr_numpy_roundtrip(dtype_str, arrow_type):
    dtype = np.dtype(dtype_str)
    data = np.array([1, 2, 3, 4, 5, 6]).astype(dtype)
    indptr = np.array([0, 2, 3, 6]).astype('i8')
    indices = np.array([0, 2, 2, 0, 1, 2]).astype('i8')
    shape = (3, 3)
    dim_names = ["x", "y"]

    sparse_tensor = pa.SparseTensorCSR.from_numpy(data, indptr, indices,
                                                  shape, dim_names)
    repr(sparse_tensor)
    assert sparse_tensor.type == arrow_type
    result_data, result_indptr, result_indices = sparse_tensor.to_numpy()
    assert (data == result_data).all()
    assert (indptr == result_indptr).all()
    assert (indices == result_indices).all()
