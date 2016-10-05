#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.

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
