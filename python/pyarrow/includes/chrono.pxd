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

from libc.stdint cimport *


cdef extern from "<chrono>" namespace "std::chrono":
    cdef cppclass duration:
        duration(int64_t count)
        const int64_t count()

    cdef cppclass nanoseconds(duration):
        nanoseconds(int64_t count)

    T duration_cast[T](duration d)


cdef extern from "<chrono>" namespace "std::chrono::system_clock":
    cdef cppclass time_point:
        time_point(const duration& d)
        const duration time_since_epoch()
        ctypedef duration duration
