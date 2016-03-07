# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

cdef extern from 'pyarrow/init.h' namespace 'arrow::py':
    void pyarrow_init()

pyarrow_init()
