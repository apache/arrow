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

dtypes = ['uint8', 'int16', 'float32']
cuda = pytest.importorskip("pyarrow.cuda")
nb_cuda = pytest.importorskip("numba.cuda")

from numba.cuda.cudadrv.devicearray import DeviceNDArray  # noqa: E402


ctx = None
nb_ctx = None


def setup_module(module):
    # pyarrow.cuda numba.cuda interoperability requires using
    # numba.cuda context manager
    cuda.Context.use_numba_context(True)

    module.ctx = cuda.Context()
    module.nb_ctx = ctx.to_numba()


def teardown_module(module):
    del module.ctx
    del module.nb_ctx


def test_context():
    assert ctx.handle == nb_ctx.handle.value
    assert ctx.handle == ctx.to_numba().handle.value
    ctx2 = cuda.Context.from_numba(nb_ctx)
    assert ctx.handle == ctx2.handle

    size = 10
    buf = ctx.new_buffer(size)
    assert ctx.handle == buf.context.handle


def make_random_buffer(size, target='host', dtype='uint8', ctx=None):
    """Return a host or device buffer with random data.
    """
    dtype = np.dtype(dtype)
    if target == 'host':
        assert size >= 0
        buf = pa.allocate_buffer(size*dtype.itemsize)
        arr = np.frombuffer(buf, dtype=dtype)
        arr[:] = np.random.randint(low=0, high=255, size=size*dtype.itemsize,
                                   dtype=np.uint8).view(dtype=dtype)
        return arr, buf
    elif target == 'device':
        arr, buf = make_random_buffer(size, target='host', dtype=dtype)
        dbuf = ctx.new_buffer(size * dtype.itemsize)
        dbuf.copy_from_host(buf, position=0, nbytes=buf.size)
        return arr, dbuf
    raise ValueError('invalid target value')


@pytest.mark.parametrize("dtype", dtypes, ids=dtypes)
def test_numba_memalloc(dtype):
    dtype = np.dtype(dtype)
    # Allocate memory using numba context
    # Warning: this will not be reflected in pyarrow context manager
    # (e.g bytes_allocated does not change)
    size = 10
    mem = nb_ctx.memalloc(size * dtype.itemsize)
    darr = DeviceNDArray((size,), (dtype.itemsize,), dtype, gpu_data=mem)
    darr[:5] = 99
    darr[5:] = 88
    np.testing.assert_equal(darr.copy_to_host()[:5], 99)
    np.testing.assert_equal(darr.copy_to_host()[5:], 88)

    # wrap numba allocated memory with CudaBuffer
    cbuf = cuda.CudaBuffer.from_numba(mem)
    arr2 = np.frombuffer(cbuf.copy_to_host(), dtype=dtype)
    np.testing.assert_equal(arr2, darr.copy_to_host())


@pytest.mark.parametrize("dtype", dtypes, ids=dtypes)
def test_pyarrow_memalloc(dtype):
    size = 10
    arr, cbuf = make_random_buffer(size, target='device', dtype=dtype, ctx=ctx)

    # wrap CudaBuffer with numba device array
    mem = cbuf.to_numba()
    darr = DeviceNDArray(arr.shape, arr.strides, arr.dtype, gpu_data=mem)
    np.testing.assert_equal(darr.copy_to_host(), arr)


@pytest.mark.parametrize("dtype", dtypes, ids=dtypes)
def test_numba_context(dtype):
    size = 10
    with nb_cuda.gpus[0]:
        arr, cbuf = make_random_buffer(size, target='device',
                                       dtype=dtype, ctx=ctx)
        assert cbuf.context.handle == nb_ctx.handle.value
        mem = cbuf.to_numba()
        darr = DeviceNDArray(arr.shape, arr.strides, arr.dtype, gpu_data=mem)
        np.testing.assert_equal(darr.copy_to_host(), arr)
        darr[0] = 99
        arr2 = np.frombuffer(cbuf.copy_to_host(), dtype=dtype)
        assert arr2[0] == 99


@pytest.mark.parametrize("dtype", dtypes, ids=dtypes)
def test_pyarrow_jit(dtype):

    @nb_cuda.jit
    def increment_by_one(an_array):
        pos = nb_cuda.grid(1)
        if pos < an_array.size:
            an_array[pos] += 1

    # applying numba.cuda kernel to memory hold by CudaBuffer
    size = 10
    arr, cbuf = make_random_buffer(size, target='device', dtype=dtype, ctx=ctx)
    threadsperblock = 32
    blockspergrid = (arr.size + (threadsperblock - 1)) // threadsperblock
    mem = cbuf.to_numba()
    darr = DeviceNDArray(arr.shape, arr.strides, arr.dtype, gpu_data=mem)
    increment_by_one[blockspergrid, threadsperblock](darr)
    np.testing.assert_equal(np.frombuffer(cbuf.copy_to_host(),
                                          dtype=arr.dtype),
                            arr + 1)
