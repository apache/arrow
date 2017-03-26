// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef PYARROW_NUMPY_INTEROP_H
#define PYARROW_NUMPY_INTEROP_H

#include <Python.h>

#include <numpy/numpyconfig.h>

// Don't use the deprecated Numpy functions
#ifdef NPY_1_7_API_VERSION
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#else
#define NPY_ARRAY_NOTSWAPPED NPY_NOTSWAPPED
#define NPY_ARRAY_ALIGNED NPY_ALIGNED
#define NPY_ARRAY_WRITEABLE NPY_WRITEABLE
#define NPY_ARRAY_UPDATEIFCOPY NPY_UPDATEIFCOPY
#endif

// This is required to be able to access the NumPy C API properly in C++ files
// other than this main one
#define PY_ARRAY_UNIQUE_SYMBOL arrow_ARRAY_API
#ifndef NUMPY_IMPORT_ARRAY
#define NO_IMPORT_ARRAY
#endif

#include <numpy/arrayobject.h>
#include <numpy/ufuncobject.h>

namespace arrow {
namespace py {

inline int import_numpy() {
#ifdef NUMPY_IMPORT_ARRAY
  import_array1(-1);
  import_umath1(-1);
#endif

  return 0;
}

}  // namespace py
}  // namespace arrow

#endif  // PYARROW_NUMPY_INTEROP_H
