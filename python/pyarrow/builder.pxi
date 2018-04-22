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

import six
from pyarrow.compat import tobytes

cdef class StringBuilder:
    cdef:
        unique_ptr[CStringBuilder] builder

    def __cinit__(self, MemoryPool memory_pool=None):
        cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        self.builder.reset(new CStringBuilder(pool))

    def append(self, value):
        if value is None or value is np.nan:
            self.builder.get().AppendNull()
        elif isinstance(value, (six.string_types, six.binary_type)):
            self.builder.get().Append(tobytes(value))
        else:
            raise TypeError('StringBuilder only accepts string objects')

    def append_values(self, values):
        for value in values:
            self.append(value)

    def length(self):
        return self.builder.get().length()

    def null_count(self):
        return self.builder.get().null_count()

    def finish(self):
        return pyarrow_wrap_array(self._finish())

    cdef shared_ptr[CArray] _finish(self) nogil:
        cdef shared_ptr[CArray] out
        self.builder.get().Finish(&out)
        return out
