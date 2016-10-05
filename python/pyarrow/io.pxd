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

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_io cimport (ReadableFileInterface,
                                           OutputStream)

cdef class Buffer:
    cdef:
        shared_ptr[CBuffer] buffer

    cdef init(self, const shared_ptr[CBuffer]& buffer)

cdef class NativeFile:
    cdef:
        shared_ptr[ReadableFileInterface] rd_file
        shared_ptr[OutputStream] wr_file
        bint is_readonly
        bint is_open
        bint own_file

    # By implementing these "virtual" functions (all functions in Cython
    # extension classes are technically virtual in the C++ sense) we can expose
    # the arrow::io abstract file interfaces to other components throughout the
    # suite of Arrow C++ libraries
    cdef read_handle(self, shared_ptr[ReadableFileInterface]* file)
    cdef write_handle(self, shared_ptr[OutputStream]* file)
