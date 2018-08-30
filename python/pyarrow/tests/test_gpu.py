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

gpu_support = pytest.mark.skipif(not pa.has_gpu_support, reason='Arrow not built with GPU support')

@gpu_support
def test_CudaDeviceManager():
    manager = pa.CudaDeviceManager()
    assert manager.num_devices > 0

    # expected to fail, but fails only occasionally:
    #manager.get_context(manager.num_devices+1) 

@gpu_support
def test_CudaContext():
    manager = pa.CudaDeviceManager()
    context = manager.get_context()
    assert context.bytes_allocated == 0
    cudabuf = context.allocate(128)
    assert context.bytes_allocated == 128
    context.close()

@gpu_support
def _test_buffer_bytes():
    val = b'some data'
    buf = pa.py_cudabuffer(val) # TODO: fix impl
    assert isinstance(buf, pa.CudaBuffer)
    
