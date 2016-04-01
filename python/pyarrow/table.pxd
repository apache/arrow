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

from pyarrow.includes.common cimport shared_ptr
from pyarrow.includes.libarrow cimport CChunkedArray, CColumn, CTable


cdef class ChunkedArray:
    cdef:
        shared_ptr[CChunkedArray] sp_chunked_array
        CChunkedArray* chunked_array

    cdef init(self, const shared_ptr[CChunkedArray]& chunked_array)
    cdef _check_nullptr(self)


cdef class Column:
    cdef:
        shared_ptr[CColumn] sp_column
        CColumn* column

    cdef init(self, const shared_ptr[CColumn]& column)
    cdef _check_nullptr(self)


cdef class Table:
    cdef:
        shared_ptr[CTable] sp_table
        CTable* table
    
    cdef init(self, const shared_ptr[CTable]& table)
    cdef _check_nullptr(self)
