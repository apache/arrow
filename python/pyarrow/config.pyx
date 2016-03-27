# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

cdef extern from 'pyarrow/config.h' namespace 'pyarrow':
    void pyarrow_init()
    void pyarrow_set_numpy_nan(object o)

pyarrow_init()

import numpy as np
pyarrow_set_numpy_nan(np.nan)
