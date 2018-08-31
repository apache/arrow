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

gpu_support = pytest.mark.skipif(not pa.has_gpu_support, reason='Arrow not built with GPU support')

def setup_module(module):
    if pa.has_gpu_support:
        module.manager = pa.CudaDeviceManager()
        module.context = module.manager.get_context()
        
def teardown_module(module):
    if pa.has_gpu_support:
        module.context.close()

@gpu_support
def test_manager_num_devices():
    assert manager.num_devices > 0

    # expected to fail, but fails only occasionally:
    #manager.get_context(manager.num_devices+1) 

@gpu_support
def test_context_allocate():
    bytes_allocated = context.bytes_allocated
    cudabuf = context.allocate(128)
    assert context.bytes_allocated == bytes_allocated + 128

def make_random_buffer(size, target='host'):
    if target == 'host':
        assert size >= 0
        buf = pa.allocate_buffer(size, resizable=True)
        assert buf.size == size
        arr = np.frombuffer(buf, dtype=np.uint8)
        assert arr.size == size
        arr[:] = np.random.randint(low=0, high=255, size=size, dtype=np.uint8)
        assert arr.sum()>0
        arr_ = np.frombuffer(buf, dtype=np.uint8)
        assert (arr == arr_).all()
        return arr, buf
    if target == 'device':
        arr, buf = make_random_buffer(size, target='host')
        dbuf = context.allocate(size)
        assert dbuf.size == size
        dbuf.copy_from_host(buf, position=0, nbytes=size)
        return arr, dbuf
    raise ValueError('invalid target value')
    
@gpu_support
def test_copy_from_to_host():
    size = 1024

    # Create a buffer in host containing range(size)
    buf = pa.allocate_buffer(size, resizable=True) # in host
    assert isinstance(buf, pa.Buffer)
    assert not isinstance(buf, pa.CudaBuffer)
    arr = np.frombuffer(buf, dtype=np.uint8)
    assert arr.size == size
    arr[:] = range(size)
    arr_ = np.frombuffer(buf, dtype=np.uint8)
    assert (arr==arr_).all()
    
    device_buffer = context.allocate(size)
    assert isinstance(device_buffer, pa.CudaBuffer)
    assert isinstance(device_buffer, pa.Buffer)
    assert device_buffer.size == size
    
    device_buffer.copy_from_host(buf, position=0, nbytes=size)
    
    buf2 = device_buffer.copy_to_host(position=0, nbytes=size)
    arr2 = np.frombuffer(buf2, dtype=np.uint8)
    assert (arr==arr2).all()
    
@gpu_support
def test_copy_to_host():
    size = 1024
    arr, dbuf = make_random_buffer(size, target='device')

    buf = dbuf.copy_to_host()
    assert (arr == np.frombuffer(buf, dtype=np.uint8)).all()

    buf = dbuf.copy_to_host(position=size//4)
    assert (arr[size//4:] == np.frombuffer(buf, dtype=np.uint8)).all()

    buf = dbuf.copy_to_host(position=size//4, nbytes=size//8)
    assert (arr[size//4:size//4+size//8] == np.frombuffer(buf, dtype=np.uint8)).all()

    buf = dbuf.copy_to_host(position=size//4, nbytes=0)
    assert buf.size == 0
    x
    for (position, nbytes) in [
            (size+2, -1), (-2, -1), (0, size + 10), (10, size-5),
    ]:
        with pytest.raises(ValueError) as e_info:
            dbuf.copy_to_host(position=position, nbytes=nbytes)
        assert str(e_info.value).startswith('inconsistent input parameters')

    buf = pa.allocate_buffer(size//4)
    dbuf.copy_to_host(buf=buf)
    assert (arr[:size//4] == np.frombuffer(buf, dtype=np.uint8)).all()

    dbuf.copy_to_host(buf=buf, position=12)
    assert (arr[12:12+size//4] == np.frombuffer(buf, dtype=np.uint8)).all()
    
    dbuf.copy_to_host(buf=buf, nbytes=12)
    assert (arr[:12] == np.frombuffer(buf, dtype=np.uint8)[:12]).all()

    dbuf.copy_to_host(buf=buf, nbytes=12, position=6)
    assert (arr[6:6+12] == np.frombuffer(buf, dtype=np.uint8)[:12]).all()

    for (position, nbytes) in [
            (size+2, -1), (-2, -1), (0, size + 10), (10, size-5),
            (0, size//2), (size//4, size//4+1)
    ]:
        with pytest.raises(ValueError) as e_info:
            dbuf.copy_to_host(buf=buf, position=position, nbytes=nbytes)
            print('dbuf.size={}, buf.size={}, position={}, nbytes={}'
                  .format(dbuf.size, buf.size, position, nbytes))
        assert str(e_info.value).startswith('inconsistent input parameters')


        
@gpu_support
def _test_buffer_bytes():
    val = b'some data'
    buf = pa.py_cudabuffer(val) # TODO: fix impl
    assert isinstance(buf, pa.CudaBuffer)
    
