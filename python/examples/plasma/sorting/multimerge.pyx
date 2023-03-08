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

# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

from libcpp.vector cimport vector
from libcpp.pair cimport pair

import numpy as np

cimport numpy as np

cdef extern from "<queue>" namespace "std" nogil:
    cdef cppclass priority_queue[T]:
        priority_queue() except +
        priority_queue(priority_queue&) except +
        bint empty()
        void pop()
        void push(T&)
        size_t size()
        T& top()


def multimerge2d(*arrays):
    """Merge a list of sorted 2d arrays into a sorted 2d array.

    This assumes C style ordering for both input and output arrays. For
    each input array we have array[i,0] <= array[i+1,0] and for the output
    array the same will hold.

    Ideally this code would be simpler and also support both C style
    and Fortran style ordering.
    """
    cdef int num_arrays = len(arrays)
    assert num_arrays > 0

    cdef int num_cols = arrays[0].shape[1]

    for i in range(num_arrays):
        assert arrays[i].ndim == 2
        assert arrays[i].dtype == np.float64
        assert arrays[i].shape[1] == num_cols
        assert not np.isfortran(arrays[i])

    cdef vector[double*] data

    # The indices vector keeps track of the index of the next row to process in
    # each array.
    cdef vector[int] indices = num_arrays * [0]

    # The sizes vector stores the total number of elements that each array has.
    cdef vector[int] sizes

    cdef priority_queue[pair[double, int]] queue
    cdef pair[double, int] top
    cdef int num_rows = sum([array.shape[0] for array in arrays])
    cdef np.ndarray[np.float64_t, ndim=2] result = np.zeros(
        (num_rows, num_cols), dtype=np.float64)
    cdef double* result_ptr = <double*> np.PyArray_DATA(result)
    for i in range(num_arrays):
        if arrays[i].size > 0:
            sizes.push_back(arrays[i].size)
            data.push_back(<double*> np.PyArray_DATA(arrays[i]))
            queue.push(pair[double, int](-data[i][0], i))

    cdef int curr_idx = 0
    cdef int j
    cdef int col = 0

    for j in range(num_rows):
        top = queue.top()
        for col in range(num_cols):
            result_ptr[curr_idx + col] = (
                data[top.second][indices[top.second] + col])

        indices[top.second] += num_cols
        curr_idx += num_cols

        queue.pop()
        if indices[top.second] < sizes[top.second]:
            queue.push(
                pair[double, int](-data[top.second][indices[top.second]],
                                  top.second))

    return result
