
from pyarrow.includes.libarrow_gpu cimport *

cdef class CudaBuffer:
    cdef:
        shared_ptr[CCudaBuffer] buffer
        Py_ssize_t shape[1]
        Py_ssize_t strides[1]

    #cdef void init(self, const shared_ptr[CudaBuffer]& buffer)
