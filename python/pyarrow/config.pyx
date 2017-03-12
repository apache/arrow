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

cdef extern from 'pyarrow/numpy_interop.h' namespace 'arrow::py':
    int import_numpy()

cdef extern from 'pyarrow/config.h' namespace 'arrow::py':
    void pyarrow_init()
    void pyarrow_set_numpy_nan(object o)

import_numpy()
pyarrow_init()

import numpy as np
pyarrow_set_numpy_nan(np.nan)

import multiprocessing
import os
cdef int CPU_COUNT = int(
    os.environ.get('OMP_NUM_THREADS',
                   max(multiprocessing.cpu_count() // 2, 1)))

def cpu_count():
    """
    Returns
    -------
    count : Number of CPUs to use by default in parallel operations. Default is
      max(1, multiprocessing.cpu_count() / 2), but can be overridden by the
      OMP_NUM_THREADS environment variable. For the default, we divide the CPU
      count by 2 because most modern computers have hyperthreading turned on,
      so doubling the CPU count beyond the number of physical cores does not
      help.
    """
    return CPU_COUNT

def set_cpu_count(count):
    global CPU_COUNT
    CPU_COUNT = max(int(count), 1)
