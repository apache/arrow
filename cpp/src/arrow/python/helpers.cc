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

#include <algorithm>
#include <limits>
#include <sstream>

#include "arrow/python/common.h"
#include "arrow/python/helpers.h"
#include "arrow/util/decimal.h"
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
  OwnedRef unicode_ref(PyObject_Repr(obj));
  OwnedRef bytes_ref;

  if (unicode_ref) {
    bytes_ref.reset(
        PyUnicode_AsEncodedString(unicode_ref.obj(), "utf8", "backslashreplace"));
  }
  if (!bytes_ref) {
    PyErr_Clear();
    std::stringstream ss;
    ss << "<object of type '" << Py_TYPE(obj)->tp_name << "' repr() failed>";
    return ss.str();
  }
  return PyBytes_AsStdString(bytes_ref.obj());
}

Status PyObject_StdStringStr(PyObject* obj, std::string* out) {
  OwnedRef unicode_ref(PyObject_Str(obj));
  return PyUnicode_AsStdString(unicode_ref.obj(), out);
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
  RETURN_NOT_OK(PyBytesView::FromString(obj, &view));
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
  RETURN_NOT_OK(PyBytesView::FromString(obj, &view));
  const auto expected_length =
      static_cast<const FixedSizeBinaryType&>(*builder->type()).byte_width();
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
  RETURN_NOT_OK(PyBytesView::FromString(obj, &view, check_valid));
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

template <typename UInt>
Status UnsignedIntFromPython(PyObject* obj, UInt* out) {
  static_assert(std::is_unsigned<UInt>::value, "requires unsigned integer type");
  static_assert(sizeof(UInt) <= sizeof(unsigned long long),  // NOLINT
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
  if (sizeof(UInt) > sizeof(unsigned long)) {  // NOLINT
    const auto value = PyLong_AsUnsignedLongLong(obj);
    if (ARROW_PREDICT_FALSE(value == static_cast<decltype(value)>(-1))) {
      RETURN_IF_PYERROR();
    }
    if (ARROW_PREDICT_FALSE(value > std::numeric_limits<UInt>::max())) {
      std::stringstream ss;
      ss << "value too large to fit in " << typeid(UInt).name();
      return Status::Invalid(ss.str());
    }
    *out = static_cast<UInt>(value);
  } else {
    const auto value = PyLong_AsUnsignedLong(obj);
    if (ARROW_PREDICT_FALSE(value == static_cast<decltype(value)>(-1))) {
      RETURN_IF_PYERROR();
    }
    if (ARROW_PREDICT_FALSE(value > std::numeric_limits<UInt>::max())) {
      std::stringstream ss;
      ss << "value too large to fit in " << typeid(UInt).name();
      return Status::Invalid(ss.str());
    }
    *out = static_cast<UInt>(value);
  }
  return Status::OK();
}

template <typename Int>
Status SignedIntFromPython(PyObject* obj, Int* out) {
  static_assert(std::is_signed<Int>::value, "requires signed integer type");
  static_assert(sizeof(Int) <= sizeof(long long),  // NOLINT
                "integer type larger than long long");

  if (sizeof(Int) > sizeof(long)) {  // NOLINT
    const auto value = PyLong_AsLongLong(obj);
    if (ARROW_PREDICT_FALSE(value == -1)) {
      RETURN_IF_PYERROR();
    }
    if (ARROW_PREDICT_FALSE(value < std::numeric_limits<Int>::min() ||
                            value > std::numeric_limits<Int>::max())) {
      std::stringstream ss;
      ss << "value too large to fit in " << typeid(Int).name();
      return Status::Invalid(ss.str());
    }
    *out = static_cast<Int>(value);
  } else {
    const auto value = PyLong_AsLong(obj);
    if (ARROW_PREDICT_FALSE(value == -1)) {
      RETURN_IF_PYERROR();
    }
    if (ARROW_PREDICT_FALSE(value < std::numeric_limits<Int>::min() ||
                            value > std::numeric_limits<Int>::max())) {
      std::stringstream ss;
      ss << "value too large to fit in " << typeid(Int).name();
      return Status::Invalid(ss.str());
    }
    *out = static_cast<Int>(value);
  }
  return Status::OK();
}

}  // namespace

Status UInt8FromPythonInt(PyObject* obj, uint8_t* out) {
  return UnsignedIntFromPython(obj, out);
}

Status UInt16FromPythonInt(PyObject* obj, uint16_t* out) {
  return UnsignedIntFromPython(obj, out);
}

Status UInt32FromPythonInt(PyObject* obj, uint32_t* out) {
  return UnsignedIntFromPython(obj, out);
}

Status UInt64FromPythonInt(PyObject* obj, uint64_t* out) {
  return UnsignedIntFromPython(obj, out);
}

Status Int8FromPythonInt(PyObject* obj, int8_t* out) {
  return SignedIntFromPython(obj, out);
}

Status Int16FromPythonInt(PyObject* obj, int16_t* out) {
  return SignedIntFromPython(obj, out);
}

Status Int32FromPythonInt(PyObject* obj, int32_t* out) {
  return SignedIntFromPython(obj, out);
}

Status Int64FromPythonInt(PyObject* obj, int64_t* out) {
  return SignedIntFromPython(obj, out);
}

//
// Python Decimal support
//

Status ImportDecimalType(OwnedRef* decimal_type) {
  OwnedRef decimal_module;
  RETURN_NOT_OK(ImportModule("decimal", &decimal_module));
  RETURN_NOT_OK(ImportFromModule(decimal_module, "Decimal", decimal_type));
  return Status::OK();
}

Status PythonDecimalToString(PyObject* python_decimal, std::string* out) {
  // Call Python's str(decimal_object)
  return PyObject_StdStringStr(python_decimal, out);
}

// \brief Infer the precision and scale of a Python decimal.Decimal instance
// \param python_decimal[in] An instance of decimal.Decimal
// \param precision[out] The value of the inferred precision
// \param scale[out] The value of the inferred scale
// \return The status of the operation
static Status InferDecimalPrecisionAndScale(PyObject* python_decimal, int32_t* precision,
                                            int32_t* scale) {
  DCHECK_NE(python_decimal, NULLPTR);
  DCHECK_NE(precision, NULLPTR);
  DCHECK_NE(scale, NULLPTR);

  // TODO(phillipc): Make sure we perform PyDecimal_Check(python_decimal) as a DCHECK
  OwnedRef as_tuple(PyObject_CallMethod(python_decimal, const_cast<char*>("as_tuple"),
                                        const_cast<char*>("")));
  RETURN_IF_PYERROR();
  DCHECK(PyTuple_Check(as_tuple.obj()));

  OwnedRef digits(PyObject_GetAttrString(as_tuple.obj(), "digits"));
  RETURN_IF_PYERROR();
  DCHECK(PyTuple_Check(digits.obj()));

  const auto num_digits = static_cast<int32_t>(PyTuple_Size(digits.obj()));
  RETURN_IF_PYERROR();

  OwnedRef py_exponent(PyObject_GetAttrString(as_tuple.obj(), "exponent"));
  RETURN_IF_PYERROR();
  DCHECK(IsPyInteger(py_exponent.obj()));

  const auto exponent = static_cast<int32_t>(PyLong_AsLong(py_exponent.obj()));
  RETURN_IF_PYERROR();

  const int32_t abs_exponent = std::abs(exponent);

  int32_t num_additional_zeros;

  if (num_digits <= abs_exponent) {
    DCHECK_NE(exponent, 0) << "exponent should never be zero here";

    // we have leading/trailing zeros, leading if exponent is negative
    num_additional_zeros = exponent < 0 ? abs_exponent - num_digits : exponent;
    *scale = static_cast<int32_t>(exponent < 0) * -exponent;
  } else {
    // we can use the number of digits as the precision
    num_additional_zeros = 0;
    *scale = -exponent;
  }

  *precision = num_digits + num_additional_zeros;
  return Status::OK();
}

PyObject* DecimalFromString(PyObject* decimal_constructor,
                            const std::string& decimal_string) {
  DCHECK_NE(decimal_constructor, nullptr);

  auto string_size = decimal_string.size();
  DCHECK_GT(string_size, 0);

  auto string_bytes = decimal_string.c_str();
  DCHECK_NE(string_bytes, nullptr);

  return PyObject_CallFunction(decimal_constructor, const_cast<char*>("s#"), string_bytes,
                               string_size);
}

Status DecimalFromPythonDecimal(PyObject* python_decimal, const DecimalType& arrow_type,
                                Decimal128* out) {
  DCHECK_NE(python_decimal, NULLPTR);
  DCHECK_NE(out, NULLPTR);

  std::string string;
  RETURN_NOT_OK(PythonDecimalToString(python_decimal, &string));

  int32_t inferred_precision;
  int32_t inferred_scale;

  RETURN_NOT_OK(
      Decimal128::FromString(string, out, &inferred_precision, &inferred_scale));

  const int32_t precision = arrow_type.precision();
  const int32_t scale = arrow_type.scale();

  if (ARROW_PREDICT_FALSE(inferred_precision > precision)) {
    std::stringstream buf;
    buf << "Decimal type with precision " << inferred_precision
        << " does not fit into precision inferred from first array element: "
        << precision;
    return Status::Invalid(buf.str());
  }

  if (scale != inferred_scale) {
    DCHECK_NE(out, NULLPTR);
    RETURN_NOT_OK(out->Rescale(inferred_scale, scale, out));
  }
  return Status::OK();
}

bool PyDecimal_Check(PyObject* obj) {
  static OwnedRef decimal_type;
  if (!decimal_type.obj()) {
    Status status = ImportDecimalType(&decimal_type);
    DCHECK_OK(status);
    DCHECK(PyType_Check(decimal_type.obj()));
  }
  // PyObject_IsInstance() is slower as it has to check for virtual subclasses
  const int result =
      PyType_IsSubtype(Py_TYPE(obj), reinterpret_cast<PyTypeObject*>(decimal_type.obj()));
  DCHECK_NE(result, -1) << " error during PyType_IsSubtype check";
  return result == 1;
}

bool PyDecimal_ISNAN(PyObject* obj) {
  DCHECK(PyDecimal_Check(obj)) << "obj is not an instance of decimal.Decimal";
  OwnedRef is_nan(
      PyObject_CallMethod(obj, const_cast<char*>("is_nan"), const_cast<char*>("")));
  return PyObject_IsTrue(is_nan.obj()) == 1;
}

DecimalMetadata::DecimalMetadata()
    : DecimalMetadata(std::numeric_limits<int32_t>::min(),
                      std::numeric_limits<int32_t>::min()) {}

DecimalMetadata::DecimalMetadata(int32_t precision, int32_t scale)
    : precision_(precision), scale_(scale) {}

Status DecimalMetadata::Update(int32_t suggested_precision, int32_t suggested_scale) {
  const int32_t current_precision = precision_;
  precision_ = std::max(current_precision, suggested_precision);

  const int32_t current_scale = scale_;
  scale_ = std::max(current_scale, suggested_scale);

  // if our suggested scale is zero and we don't yet have enough precision then we need to
  // add whatever the current scale is to the precision
  if (suggested_scale == 0 && suggested_precision > current_precision) {
    precision_ += scale_;
  }

  return Status::OK();
}

Status DecimalMetadata::Update(PyObject* object) {
  DCHECK(PyDecimal_Check(object)) << "Object is not a Python Decimal";

  if (ARROW_PREDICT_FALSE(PyDecimal_ISNAN(object))) {
    return Status::OK();
  }

  int32_t precision;
  int32_t scale;
  RETURN_NOT_OK(InferDecimalPrecisionAndScale(object, &precision, &scale));
  return Update(precision, scale);
}

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
