# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

cdef extern from 'pyarrow/do_import_numpy.h':
    pass

cdef extern from 'pyarrow/numpy_interop.h' namespace 'pyarrow':
    int import_numpy()

cdef extern from 'pyarrow/config.h' namespace 'pyarrow':
    void pyarrow_init()
    void pyarrow_set_numpy_nan(object o)

import_numpy()
pyarrow_init()

import numpy as np
pyarrow_set_numpy_nan(np.nan)
