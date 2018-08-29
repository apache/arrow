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

# distutils: language = c++

from pyarrow.includes.libarrow cimport *

cdef extern from "arrow/gpu/cuda_api.h" namespace "arrow" nogil:

    cdef cppclass CCudaContext" arrow::gpu::CudaContext":
        shared_ptr[CCudaContext]  shared_from_this()

    cdef cppclass CCudaBuffer" arrow::gpu::CudaBuffer"(CBuffer):
        CCudaBuffer(uint8_t* data, int64_t size, const shared_ptr[CCudaContext]& context, c_bool own_data = false, c_bool is_ipc = false);
