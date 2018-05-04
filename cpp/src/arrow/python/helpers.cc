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

#include <limits>
#include <sstream>
#include <type_traits>
#include <typeinfo>

#include "arrow/python/common.h"
#include "arrow/python/decimal.h"
#include "arrow/python/helpers.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

#include <arrow/api.h>

namespace arrow {
namespace py {

#define GET_PRIMITIVE_TYPE(NAME, FACTORY) \
  case Type::NAME:                        \
    return FACTORY()

std::shared_ptr<DataType> GetPrimitiveType(Type::type type) {
  switch (type) {
    case Type::NA:
      return null();
      GET_PRIMITIVE_TYPE(UINT8, uint8);
      GET_PRIMITIVE_TYPE(INT8, int8);
      GET_PRIMITIVE_TYPE(UINT16, uint16);
      GET_PRIMITIVE_TYPE(INT16, int16);
      GET_PRIMITIVE_TYPE(UINT32, uint32);
      GET_PRIMITIVE_TYPE(INT32, int32);
      GET_PRIMITIVE_TYPE(UINT64, uint64);
      GET_PRIMITIVE_TYPE(INT64, int64);
      GET_PRIMITIVE_TYPE(DATE32, date32);
      GET_PRIMITIVE_TYPE(DATE64, date64);
      GET_PRIMITIVE_TYPE(BOOL, boolean);
      GET_PRIMITIVE_TYPE(HALF_FLOAT, float16);
      GET_PRIMITIVE_TYPE(FLOAT, float32);
      GET_PRIMITIVE_TYPE(DOUBLE, float64);
      GET_PRIMITIVE_TYPE(BINARY, binary);
      GET_PRIMITIVE_TYPE(STRING, utf8);
    default:
      return nullptr;
  }
}

PyObject* PyHalf_FromHalf(npy_half value) {
  PyObject* result = PyArrayScalar_New(Half);
  if (result != NULL) {
    PyArrayScalar_ASSIGN(result, Half, value);
  }
  return result;
}

Status PyFloat_AsHalf(PyObject* obj, npy_half* out) {
  if (PyArray_IsScalar(obj, Half)) {
    *out = PyArrayScalar_VAL(obj, Half);
    return Status::OK();
  } else {
    // XXX: cannot use npy_double_to_half() without linking with Numpy
    return Status::TypeError("Expected np.float16 instance");
  }
}

namespace internal {

std::string PyBytes_AsStdString(PyObject* obj) {
  DCHECK(PyBytes_Check(obj));
  return std::string(PyBytes_AS_STRING(obj), PyBytes_GET_SIZE(obj));
}

Status PyUnicode_AsStdString(PyObject* obj, std::string* out) {
  DCHECK(PyUnicode_Check(obj));
#if PY_MAJOR_VERSION >= 3
  Py_ssize_t size;
  // The utf-8 representation is cached on the unicode object
  const char* data = PyUnicode_AsUTF8AndSize(obj, &size);
  RETURN_IF_PYERROR();
  *out = std::string(data, size);
  return Status::OK();
#else
  OwnedRef bytes_ref(PyUnicode_AsUTF8String(obj));
  RETURN_IF_PYERROR();
  *out = PyBytes_AsStdString(bytes_ref.obj());
  return Status::OK();
#endif
}

std::string PyObject_StdStringRepr(PyObject* obj) {
#if PY_MAJOR_VERSION >= 3
  OwnedRef unicode_ref(PyObject_Repr(obj));
  OwnedRef bytes_ref;

  if (unicode_ref) {
    bytes_ref.reset(
        PyUnicode_AsEncodedString(unicode_ref.obj(), "utf8", "backslashreplace"));
  }
#else
  OwnedRef bytes_ref(PyObject_Repr(obj));
  if (!bytes_ref) {
    PyErr_Clear();
    std::stringstream ss;
    ss << "<object of type '" << Py_TYPE(obj)->tp_name << "' repr() failed>";
    return ss.str();
  }
#endif
  return PyBytes_AsStdString(bytes_ref.obj());
}

Status PyObject_StdStringStr(PyObject* obj, std::string* out) {
  OwnedRef string_ref(PyObject_Str(obj));
  RETURN_IF_PYERROR();
#if PY_MAJOR_VERSION >= 3
  return PyUnicode_AsStdString(string_ref.obj(), out);
#else
  *out = PyBytes_AsStdString(string_ref.obj());
  return Status::OK();
#endif
}

Status ImportModule(const std::string& module_name, OwnedRef* ref) {
  PyObject* module = PyImport_ImportModule(module_name.c_str());
  RETURN_IF_PYERROR();
  DCHECK_NE(module, nullptr) << "unable to import the " << module_name << " module";
  ref->reset(module);
  return Status::OK();
}

Status ImportFromModule(const OwnedRef& module, const std::string& name, OwnedRef* ref) {
  /// Assumes that ImportModule was called first
  DCHECK_NE(module.obj(), nullptr) << "Cannot import from nullptr Python module";

  PyObject* attr = PyObject_GetAttrString(module.obj(), name.c_str());
  RETURN_IF_PYERROR();
  DCHECK_NE(attr, nullptr) << "unable to import the " << name << " object";
  ref->reset(attr);
  return Status::OK();
}

Status BuilderAppend(BinaryBuilder* builder, PyObject* obj, bool* is_full) {
  PyBytesView view;
  // XXX For some reason, we must accept unicode objects here
  RETURN_NOT_OK(view.FromString(obj));
  int32_t length;
  RETURN_NOT_OK(CastSize(view.size, &length));
  // Did we reach the builder size limit?
  if (ARROW_PREDICT_FALSE(builder->value_data_length() + length > kBinaryMemoryLimit)) {
    if (is_full) {
      *is_full = true;
      return Status::OK();
    } else {
      return Status::Invalid("Maximum array size reached (2GB)");
    }
  }
  RETURN_NOT_OK(builder->Append(view.bytes, length));
  if (is_full) {
    *is_full = false;
  }
  return Status::OK();
}

Status BuilderAppend(FixedSizeBinaryBuilder* builder, PyObject* obj, bool* is_full) {
  PyBytesView view;
  // XXX For some reason, we must accept unicode objects here
  RETURN_NOT_OK(view.FromString(obj));
  const auto expected_length =
      checked_cast<const FixedSizeBinaryType&>(*builder->type()).byte_width();
  if (ARROW_PREDICT_FALSE(view.size != expected_length)) {
    std::stringstream ss;
    ss << "Got bytestring of length " << view.size << " (expected " << expected_length
       << ")";
    return Status::Invalid(ss.str());
  }
  // Did we reach the builder size limit?
  if (ARROW_PREDICT_FALSE(builder->value_data_length() + view.size >
                          kBinaryMemoryLimit)) {
    if (is_full) {
      *is_full = true;
      return Status::OK();
    } else {
      return Status::Invalid("Maximum array size reached (2GB)");
    }
  }
  RETURN_NOT_OK(builder->Append(view.bytes));
  if (is_full) {
    *is_full = false;
  }
  return Status::OK();
}

Status BuilderAppend(StringBuilder* builder, PyObject* obj, bool check_valid,
                     bool* is_full) {
  PyBytesView view;
  RETURN_NOT_OK(view.FromString(obj, check_valid));
  int32_t length;
  RETURN_NOT_OK(CastSize(view.size, &length));
  // Did we reach the builder size limit?
  if (ARROW_PREDICT_FALSE(builder->value_data_length() + length > kBinaryMemoryLimit)) {
    if (is_full) {
      *is_full = true;
      return Status::OK();
    } else {
      return Status::Invalid("Maximum array size reached (2GB)");
    }
  }
  RETURN_NOT_OK(builder->Append(view.bytes, length));
  if (is_full) {
    *is_full = false;
  }
  return Status::OK();
}

namespace {

Status IntegerOverflowStatus(const std::string& overflow_message) {
  if (overflow_message.empty()) {
    return Status::Invalid("Value too large to fit in C integer type");
  } else {
    return Status::Invalid(overflow_message);
  }
}

// Extract C signed int from Python object
template <typename Int,
          typename std::enable_if<std::is_signed<Int>::value, Int>::type = 0>
Status CIntFromPythonImpl(PyObject* obj, Int* out, const std::string& overflow_message) {
  static_assert(sizeof(Int) <= sizeof(long long),  // NOLINT
                "integer type larger than long long");

  if (sizeof(Int) > sizeof(long)) {  // NOLINT
    const auto value = PyLong_AsLongLong(obj);
    if (ARROW_PREDICT_FALSE(value == -1)) {
      RETURN_IF_PYERROR();
    }
    if (ARROW_PREDICT_FALSE(value < std::numeric_limits<Int>::min() ||
                            value > std::numeric_limits<Int>::max())) {
      return IntegerOverflowStatus(overflow_message);
    }
    *out = static_cast<Int>(value);
  } else {
    const auto value = PyLong_AsLong(obj);
    if (ARROW_PREDICT_FALSE(value == -1)) {
      RETURN_IF_PYERROR();
    }
    if (ARROW_PREDICT_FALSE(value < std::numeric_limits<Int>::min() ||
                            value > std::numeric_limits<Int>::max())) {
      return IntegerOverflowStatus(overflow_message);
    }
    *out = static_cast<Int>(value);
  }
  return Status::OK();
}

// Extract C unsigned int from Python object
template <typename Int,
          typename std::enable_if<std::is_unsigned<Int>::value, Int>::type = 0>
Status CIntFromPythonImpl(PyObject* obj, Int* out, const std::string& overflow_message) {
  static_assert(sizeof(Int) <= sizeof(unsigned long long),  // NOLINT
                "integer type larger than unsigned long long");

  OwnedRef ref;
  // PyLong_AsUnsignedLong() and PyLong_AsUnsignedLongLong() don't handle
  // conversion from non-ints (e.g. np.uint64), so do it ourselves
  if (!PyLong_Check(obj)) {
    ref.reset(PyNumber_Long(obj));
    if (!ref) {
      RETURN_IF_PYERROR();
    }
    obj = ref.obj();
  }
  if (sizeof(Int) > sizeof(unsigned long)) {  // NOLINT
    const auto value = PyLong_AsUnsignedLongLong(obj);
    if (ARROW_PREDICT_FALSE(value == static_cast<decltype(value)>(-1))) {
      RETURN_IF_PYERROR();
    }
    if (ARROW_PREDICT_FALSE(value > std::numeric_limits<Int>::max())) {
      return IntegerOverflowStatus(overflow_message);
    }
    *out = static_cast<Int>(value);
  } else {
    const auto value = PyLong_AsUnsignedLong(obj);
    if (ARROW_PREDICT_FALSE(value == static_cast<decltype(value)>(-1))) {
      RETURN_IF_PYERROR();
    }
    if (ARROW_PREDICT_FALSE(value > std::numeric_limits<Int>::max())) {
      return IntegerOverflowStatus(overflow_message);
    }
    *out = static_cast<Int>(value);
  }
  return Status::OK();
}

}  // namespace

template <typename Int>
Status CIntFromPython(PyObject* obj, Int* out, const std::string& overflow_message) {
  if (PyBool_Check(obj)) {
    return Status::TypeError("Expected integer, got bool");
  }
  return CIntFromPythonImpl(obj, out, overflow_message);
}

template Status CIntFromPython(PyObject*, int8_t*, const std::string&);
template Status CIntFromPython(PyObject*, int16_t*, const std::string&);
template Status CIntFromPython(PyObject*, int32_t*, const std::string&);
template Status CIntFromPython(PyObject*, int64_t*, const std::string&);
template Status CIntFromPython(PyObject*, uint8_t*, const std::string&);
template Status CIntFromPython(PyObject*, uint16_t*, const std::string&);
template Status CIntFromPython(PyObject*, uint32_t*, const std::string&);
template Status CIntFromPython(PyObject*, uint64_t*, const std::string&);

bool PyFloat_IsNaN(PyObject* obj) {
  return PyFloat_Check(obj) && std::isnan(PyFloat_AsDouble(obj));
}

bool PandasObjectIsNull(PyObject* obj) {
  return obj == Py_None || obj == numpy_nan || PyFloat_IsNaN(obj) ||
         (internal::PyDecimal_Check(obj) && internal::PyDecimal_ISNAN(obj));
}

}  // namespace internal
}  // namespace py
}  // namespace arrow
