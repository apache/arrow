# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

from libc.stdint cimport uintptr_t
from libcpp.vector cimport vector
from libcpp.pair cimport pair

cimport numpy as np
import numpy as np

cdef extern from "<queue>" namespace "std" nogil:
    cdef cppclass priority_queue[T]:
        priority_queue() except +
        priority_queue(priority_queue&) except +
        #priority_queue(Container&)
        bint empty()
        void pop()
        void push(T&)
        size_t size()
        T& top()

def multimerge(*arrays):
  cdef int num_arrays = len(arrays)
  cdef vector[double*] data
  cdef vector[int] indices = num_arrays * [0]
  cdef vector[int] sizes
  cdef priority_queue[pair[double, int]] queue
  cdef pair[double, int] top
  cdef int num_elts = sum([len(array) for array in arrays])
  cdef np.ndarray[np.float64_t, ndim=1] result = np.zeros(num_elts, dtype=np.float64)
  cdef double* result_ptr = <double*> np.PyArray_DATA(result)
  cdef int curr_idx = 0
  for i in range(num_arrays):
    sizes.push_back(len(arrays[i]))
    data.push_back(<double*> np.PyArray_DATA(arrays[i]))
    queue.push(pair[double, int](-data[i][0], i))
  while curr_idx < num_elts:
    top = queue.top()
    result_ptr[curr_idx] = data[top.second][indices[top.second]]
    indices[top.second] += 1
    queue.pop()
    if indices[top.second] < sizes[top.second]:
      queue.push(pair[double, int](-data[top.second][indices[top.second]], top.second))
    curr_idx += 1
  return result

def multimerge2d(*arrays):
  cdef int num_arrays = len(arrays)

  assert num_arrays > 0

  num_cols = arrays[0].shape[1]

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
  cdef np.ndarray[np.float64_t, ndim=2] result = np.zeros((num_rows, num_cols), dtype=np.float64)
  cdef double* result_ptr = <double*> np.PyArray_DATA(result)
  for i in range(num_arrays):
    if arrays[i].size > 0:
      sizes.push_back(arrays[i].size)
      data.push_back(<double*> np.PyArray_DATA(arrays[i]))
      queue.push(pair[double, int](-data[i][0], i))

  cdef int curr_idx = 0

  for j in range(num_rows):
    top = queue.top()
    for col in range(num_cols):
      result_ptr[curr_idx + col] = data[top.second][indices[top.second] + col]

    indices[top.second] += num_cols
    curr_idx += num_cols

    queue.pop()
    if indices[top.second] < sizes[top.second]:
      queue.push(pair[double, int](-data[top.second][indices[top.second]], top.second))

  return result
