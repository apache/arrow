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
    
    device_buffer.copy_from_host(0, buf, size)
    
    buf2 = device_buffer.copy_to_host(0, size)
    arr2 = np.frombuffer(buf2, dtype=np.uint8)
    assert (arr==arr2).all()
    
    
@gpu_support
def _test_buffer_bytes():
    val = b'some data'
    buf = pa.py_cudabuffer(val) # TODO: fix impl
    assert isinstance(buf, pa.CudaBuffer)
    
