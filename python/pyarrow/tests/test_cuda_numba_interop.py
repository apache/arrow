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
import pyarrow as pa
import numpy as np

cuda = pytest.importorskip("pyarrow.cuda")
nb_cuda = pytest.importorskip("numba.cuda")


def make_random_buffer(size, target='host', ctx=None):
    """Return a host or device buffer with random data.
    """
    if target == 'host':
        assert size >= 0
        buf = pa.allocate_buffer(size)
        arr = np.frombuffer(buf, dtype=np.uint8)
        arr[:] = np.random.randint(low=0, high=255, size=size, dtype=np.uint8)
        return arr, buf
    elif target == 'device':
        arr, buf = make_random_buffer(size, target='host')
        dbuf = ctx.new_buffer(size)
        dbuf.copy_from_host(buf, position=0, nbytes=size)
        return arr, dbuf
    raise ValueError('invalid target value')


def test_numba_memalloc():
    from numba.cuda.cudadrv.devicearray import DeviceNDArray

    # Create context instances
    ctx = cuda.Context()
    nb_ctx = ctx.to_numba()
    assert ctx.handle == nb_ctx.handle.value
    assert ctx.handle == nb_cuda.cudadrv.driver.driver.get_context().value

    # Dummy data
    size = 10
    arr, buf = make_random_buffer(size, target='host')

    # Allocate memory using numba context
    # Warning: this will not be reflected in pyarrow context manager
    # (e.g bytes_allocated does not change)
    mem = nb_ctx.memalloc(size)
    darr = DeviceNDArray(arr.shape, arr.strides, arr.dtype, gpu_data=mem)
    darr[:5] = 99
    darr[5:] = 88
    np.testing.assert_equal(darr.copy_to_host()[:5], 99)
    np.testing.assert_equal(darr.copy_to_host()[5:], 88)

    # wrap numba allocated memory with CudaBuffer
    cbuf = cuda.CudaBuffer.from_numba(mem)
    arr2 = np.frombuffer(cbuf.copy_to_host(), dtype=arr.dtype)
    np.testing.assert_equal(arr2, darr.copy_to_host())


def test_pyarrow_memalloc():
    from numba.cuda.cudadrv.devicearray import DeviceNDArray

    ctx = cuda.Context()

    size = 10
    arr, cbuf = make_random_buffer(size, target='device', ctx=ctx)

    # wrap CudaBuffer with numba device array
    mem = cbuf.to_numba()
    darr = DeviceNDArray(arr.shape, arr.strides, arr.dtype, gpu_data=mem)
    np.testing.assert_equal(darr.copy_to_host(), arr)


def test_numba_context():
    from numba.cuda.cudadrv.devicearray import DeviceNDArray

    size = 10
    with nb_cuda.gpus[0]:
        # context is managed by numba
        nb_ctx = nb_cuda.cudadrv.devices.get_context()
        ctx = cuda.Context.from_numba(nb_ctx)
        arr, cbuf = make_random_buffer(size, target='device', ctx=ctx)
        assert cbuf.context.handle == nb_ctx.handle.value
        mem = cbuf.to_numba()
        darr = DeviceNDArray(arr.shape, arr.strides, arr.dtype, gpu_data=mem)
        np.testing.assert_equal(darr.copy_to_host(), arr)
        darr[0] = 99
        arr2 = np.frombuffer(cbuf.copy_to_host(), dtype=np.uint8)
        assert arr2[0] == 99


def test_pyarrow_jit():
    from numba.cuda.cudadrv.devicearray import DeviceNDArray

    # applying numba.cuda kernel to memory hold by CudaBuffer
    ctx = cuda.Context()
    size = 10
    arr, cbuf = make_random_buffer(size, target='device', ctx=ctx)

    @nb_cuda.jit
    def increment_by_one(an_array):
        pos = nb_cuda.grid(1)
        if pos < an_array.size:
            an_array[pos] += 1
    threadsperblock = 32
    blockspergrid = (arr.size + (threadsperblock - 1)) // threadsperblock
    mem = cbuf.to_numba()
    darr = DeviceNDArray(arr.shape, arr.strides, arr.dtype, gpu_data=mem)
    increment_by_one[blockspergrid, threadsperblock](darr)
    np.testing.assert_equal(np.frombuffer(cbuf.copy_to_host(),
                                          dtype=arr.dtype),
                            arr + 1)
