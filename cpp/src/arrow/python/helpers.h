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

#ifndef ARROW_PYTHON_HELPERS_H
#define ARROW_PYTHON_HELPERS_H

#include "arrow/python/platform.h"

#include <limits>
#include <memory>
#include <string>
#include <utility>

#include <numpy/halffloat.h>

#include "arrow/type.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

namespace py {

class OwnedRef;

// \brief Get an arrow DataType instance from Arrow's Type::type enum
// \param[in] type One of the values of Arrow's Type::type enum
// \return A shared pointer to DataType
ARROW_EXPORT std::shared_ptr<DataType> GetPrimitiveType(Type::type type);

// \brief Construct a np.float16 object from a npy_half value.
ARROW_EXPORT PyObject* PyHalf_FromHalf(npy_half value);

// \brief Convert a Python object to a npy_half value.
ARROW_EXPORT Status PyFloat_AsHalf(PyObject* obj, npy_half* out);

namespace internal {

// \brief Import a Python module
// \param[in] module_name The name of the module
// \param[out] ref The OwnedRef containing the module PyObject*
Status ImportModule(const std::string& module_name, OwnedRef* ref);

// \brief Import an object from a Python module
// \param[in] module A Python module
// \param[in] name The name of the object to import
// \param[out] ref The OwnedRef containing the \c name attribute of the Python module \c
// module
Status ImportFromModule(const OwnedRef& module, const std::string& name, OwnedRef* ref);

// \brief Check whether obj is an integer, independent of Python versions.
inline bool IsPyInteger(PyObject* obj) {
#if PYARROW_IS_PY2
  return PyLong_Check(obj) || PyInt_Check(obj);
#else
  return PyLong_Check(obj);
#endif
}

// \brief Use pandas missing value semantics to check if a value is null
bool PandasObjectIsNull(PyObject* obj);

// \brief Check whether obj is nan
bool PyFloat_IsNaN(PyObject* obj);

inline bool IsPyBinary(PyObject* obj) {
  return PyBytes_Check(obj) || PyByteArray_Check(obj);
}

// \brief Convert a Python integer into a C integer
// \param[in] obj A Python integer
// \param[out] out A pointer to a C integer to hold the result of the conversion
// \return The status of the operation
template <typename Int>
Status CIntFromPython(PyObject* obj, Int* out, const std::string& overflow_message = "");

// \brief Convert a Python unicode string to a std::string
Status PyUnicode_AsStdString(PyObject* obj, std::string* out);

// \brief Convert a Python bytes object to a std::string
std::string PyBytes_AsStdString(PyObject* obj);

// \brief Call str() on the given object and return the result as a std::string
Status PyObject_StdStringStr(PyObject* obj, std::string* out);

// \brief Return the repr() of the given object (always succeeds)
std::string PyObject_StdStringRepr(PyObject* obj);

// \brief Cast the given size to int32_t, with error checking
inline Status CastSize(Py_ssize_t size, int32_t* out,
                       const char* error_msg = "Maximum size exceeded (2GB)") {
  // size is assumed to be positive
  if (size > std::numeric_limits<int32_t>::max()) {
    return Status::Invalid(error_msg);
  }
  *out = static_cast<int32_t>(size);
  return Status::OK();
}

Status BuilderAppend(StringBuilder* builder, PyObject* obj, bool check_valid = false,
                     bool* is_full = nullptr);
Status BuilderAppend(BinaryBuilder* builder, PyObject* obj, bool* is_full = nullptr);
Status BuilderAppend(FixedSizeBinaryBuilder* builder, PyObject* obj,
                     bool* is_full = nullptr);

}  // namespace internal
}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_HELPERS_H
