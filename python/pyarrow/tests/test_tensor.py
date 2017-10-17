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

import os
import sys
import pytest

import numpy as np
import pyarrow as pa


def test_tensor_attrs():
    data = np.random.randn(10, 4)

    tensor = pa.Tensor.from_numpy(data)

    assert tensor.ndim == 2
    assert tensor.size == 40
    assert tensor.shape == list(data.shape)
    assert tensor.strides == list(data.strides)

    assert tensor.is_contiguous
    assert tensor.is_mutable

    # not writeable
    data2 = data.copy()
    data2.flags.writeable = False
    tensor = pa.Tensor.from_numpy(data2)
    assert not tensor.is_mutable


def test_tensor_base_object():
    tensor = pa.Tensor.from_numpy(np.random.randn(10, 4))
    n = sys.getrefcount(tensor)
    array = tensor.to_numpy()  # noqa
    assert sys.getrefcount(tensor) == n + 1


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
def test_tensor_numpy_roundtrip(dtype_str, arrow_type):
    dtype = np.dtype(dtype_str)
    data = (100 * np.random.randn(10, 4)).astype(dtype)

    tensor = pa.Tensor.from_numpy(data)
    assert tensor.type == arrow_type

    repr(tensor)

    result = tensor.to_numpy()
    assert (data == result).all()


def _try_delete(path):
    import gc
    gc.collect()
    try:
        os.remove(path)
    except os.error:
        pass


def test_tensor_ipc_roundtrip(tmpdir):
    data = np.random.randn(10, 4)
    tensor = pa.Tensor.from_numpy(data)

    path = os.path.join(str(tmpdir), 'pyarrow-tensor-ipc-roundtrip')
    mmap = pa.create_memory_map(path, 1024)

    pa.write_tensor(tensor, mmap)

    mmap.seek(0)
    result = pa.read_tensor(mmap)

    assert result.equals(tensor)


def test_tensor_ipc_strided(tmpdir):
    data1 = np.random.randn(10, 4)
    tensor1 = pa.Tensor.from_numpy(data1[::2])

    data2 = np.random.randn(10, 6, 4)
    tensor2 = pa.Tensor.from_numpy(data2[::, ::2, ::])

    path = os.path.join(str(tmpdir), 'pyarrow-tensor-ipc-strided')
    mmap = pa.create_memory_map(path, 2048)

    for tensor in [tensor1, tensor2]:
        mmap.seek(0)
        pa.write_tensor(tensor, mmap)

        mmap.seek(0)
        result = pa.read_tensor(mmap)

        assert result.equals(tensor)


def test_tensor_equals():
    data = np.random.randn(10, 6, 4)[::, ::2, ::]
    tensor1 = pa.Tensor.from_numpy(data)
    tensor2 = pa.Tensor.from_numpy(np.ascontiguousarray(data))
    assert tensor1.equals(tensor2)
    data = data.copy()
    data[9, 0, 0] = 1.0
    tensor2 = pa.Tensor.from_numpy(np.ascontiguousarray(data))
    assert not tensor1.equals(tensor2)


def test_tensor_size():
    data = np.random.randn(10, 4)
    tensor = pa.Tensor.from_numpy(data)
    assert pa.get_tensor_size(tensor) > (data.size * 8)


def test_read_tensor(tmpdir):
    # Create and write tensor tensor
    data = np.random.randn(10, 4)
    tensor = pa.Tensor.from_numpy(data)
    data_size = pa.get_tensor_size(tensor)
    path = os.path.join(str(tmpdir), 'pyarrow-tensor-ipc-read-tensor')
    write_mmap = pa.create_memory_map(path, data_size)
    pa.write_tensor(tensor, write_mmap)
    # Try to read tensor
    read_mmap = pa.memory_map(path, mode='r')
    array = pa.read_tensor(read_mmap).to_numpy()
    np.testing.assert_equal(data, array)
