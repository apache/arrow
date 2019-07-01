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


tensor_type_pairs = [
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
]


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
    dim_names = ['x', 'y']
    sparse_tensor = sparse_tensor_type.from_dense_numpy(data, dim_names)

    assert sparse_tensor.ndim == 2
    assert sparse_tensor.size == 25
    assert sparse_tensor.shape == data.shape
    assert sparse_tensor.is_mutable
    assert sparse_tensor.dim_name(0) == dim_names[0]
    assert sparse_tensor.dim_names == dim_names
    assert sparse_tensor.non_zero_length == 4


def test_sparse_tensor_coo_base_object():
    data = np.array([[4], [9], [7], [5]])
    coords = np.array([[0, 0], [0, 2], [1, 1], [3, 3]])
    array = np.array([[4, 0, 9, 0],
                      [0, 7, 0, 0],
                      [0, 0, 0, 0],
                      [0, 0, 0, 5]])
    sparse_tensor = pa.SparseTensorCOO.from_dense_numpy(array)
    n = sys.getrefcount(sparse_tensor)
    result_data, result_coords = sparse_tensor.to_numpy()
    assert sys.getrefcount(sparse_tensor) == n + 2

    sparse_tensor = None
    assert np.array_equal(data, result_data)
    assert np.array_equal(coords, result_coords)
    assert result_coords.flags.f_contiguous  # column-major


def test_sparse_tensor_csr_base_object():
    data = np.array([[1], [2], [3], [4], [5], [6]])
    indptr = np.array([0, 2, 3, 6])
    indices = np.array([0, 2, 2, 0, 1, 2])
    array = np.array([[1, 0, 2],
                      [0, 0, 3],
                      [4, 5, 6]])

    sparse_tensor = pa.SparseTensorCSR.from_dense_numpy(array)
    n = sys.getrefcount(sparse_tensor)
    result_data, result_indptr, result_indices = sparse_tensor.to_numpy()
    assert sys.getrefcount(sparse_tensor) == n + 3

    sparse_tensor = None
    assert np.array_equal(data, result_data)
    assert np.array_equal(indptr, result_indptr)
    assert np.array_equal(indices, result_indices)


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
    sparse_tensor2 = sparse_tensor_type.from_dense_numpy(
        np.ascontiguousarray(data))
    eq(sparse_tensor1, sparse_tensor2)
    data = data.copy()
    data[9, 0] = 1.0
    sparse_tensor2 = sparse_tensor_type.from_dense_numpy(
        np.ascontiguousarray(data))
    ne(sparse_tensor1, sparse_tensor2)


@pytest.mark.parametrize('dtype_str,arrow_type', tensor_type_pairs)
def test_sparse_tensor_coo_from_dense(dtype_str, arrow_type):
    dtype = np.dtype(dtype_str)
    data = np.array([[4], [9], [7], [5]]).astype(dtype)
    coords = np.array([[0, 0], [0, 2], [1, 1], [3, 3]])
    array = np.array([[4, 0, 9, 0],
                      [0, 7, 0, 0],
                      [0, 0, 0, 0],
                      [0, 0, 0, 5]]).astype(dtype)
    tensor = pa.Tensor.from_numpy(array)

    # Test from numpy array
    sparse_tensor = pa.SparseTensorCOO.from_dense_numpy(array)
    repr(sparse_tensor)
    assert sparse_tensor.type == arrow_type
    result_data, result_coords = sparse_tensor.to_numpy()
    assert np.array_equal(data, result_data)
    assert np.array_equal(coords, result_coords)

    # Test from Tensor
    sparse_tensor = pa.SparseTensorCOO.from_tensor(tensor)
    repr(sparse_tensor)
    assert sparse_tensor.type == arrow_type
    result_data, result_coords = sparse_tensor.to_numpy()
    assert np.array_equal(data, result_data)
    assert np.array_equal(coords, result_coords)


@pytest.mark.parametrize('dtype_str,arrow_type', tensor_type_pairs)
def test_sparse_tensor_csr_from_dense(dtype_str, arrow_type):
    dtype = np.dtype(dtype_str)
    dense_data = np.array([[1, 0, 2],
                           [0, 0, 3],
                           [4, 5, 6]]).astype(dtype)

    data = np.array([[1], [2], [3], [4], [5], [6]])
    indptr = np.array([0, 2, 3, 6])
    indices = np.array([0, 2, 2, 0, 1, 2])
    tensor = pa.Tensor.from_numpy(dense_data)

    # Test from numpy array
    sparse_tensor = pa.SparseTensorCSR.from_dense_numpy(dense_data)
    repr(sparse_tensor)
    result_data, result_indptr, result_indices = sparse_tensor.to_numpy()
    assert np.array_equal(data, result_data)
    assert np.array_equal(indptr, result_indptr)
    assert np.array_equal(indices, result_indices)

    # Test from Tensor
    sparse_tensor = pa.SparseTensorCSR.from_tensor(tensor)
    repr(sparse_tensor)
    assert sparse_tensor.type == arrow_type
    result_data, result_indptr, result_indices = sparse_tensor.to_numpy()
    assert np.array_equal(data, result_data)
    assert np.array_equal(indptr, result_indptr)
    assert np.array_equal(indices, result_indices)


@pytest.mark.parametrize('dtype_str,arrow_type', tensor_type_pairs)
def test_sparse_tensor_coo_numpy_roundtrip(dtype_str, arrow_type):
    dtype = np.dtype(dtype_str)
    data = np.array([[4], [9], [7], [5]]).astype(dtype)
    coords = np.array([[0, 0], [3, 3], [1, 1], [0, 2]])
    shape = (4, 4)
    dim_names = ["x", "y"]

    sparse_tensor = pa.SparseTensorCOO.from_numpy(data, coords, shape,
                                                  dim_names)
    repr(sparse_tensor)
    assert sparse_tensor.type == arrow_type
    result_data, result_coords = sparse_tensor.to_numpy()
    assert np.array_equal(data, result_data)
    assert np.array_equal(coords, result_coords)
    assert sparse_tensor.dim_names == dim_names


@pytest.mark.parametrize('dtype_str,arrow_type', tensor_type_pairs)
def test_sparse_tensor_csr_numpy_roundtrip(dtype_str, arrow_type):
    dtype = np.dtype(dtype_str)
    data = np.array([[1], [2], [3], [4], [5], [6]]).astype(dtype)
    indptr = np.array([0, 2, 3, 6])
    indices = np.array([0, 2, 2, 0, 1, 2])
    shape = (3, 3)
    dim_names = ["x", "y"]

    sparse_tensor = pa.SparseTensorCSR.from_numpy(data, indptr, indices,
                                                  shape, dim_names)
    repr(sparse_tensor)
    assert sparse_tensor.type == arrow_type
    result_data, result_indptr, result_indices = sparse_tensor.to_numpy()
    assert np.array_equal(data, result_data)
    assert np.array_equal(indptr, result_indptr)
    assert np.array_equal(indices, result_indices)
    assert sparse_tensor.dim_names == dim_names
