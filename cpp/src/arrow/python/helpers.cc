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

namespace internal {

Status ImportModule(const std::string& module_name, OwnedRef* ref) {
  PyObject* module = PyImport_ImportModule(module_name.c_str());
  RETURN_IF_PYERROR();
  ref->reset(module);
  return Status::OK();
}

Status ImportFromModule(const OwnedRef& module, const std::string& name, OwnedRef* ref) {
  /// Assumes that ImportModule was called first
  DCHECK_NE(module.obj(), nullptr) << "Cannot import from nullptr Python module";

  PyObject* attr = PyObject_GetAttrString(module.obj(), name.c_str());
  RETURN_IF_PYERROR();
  ref->reset(attr);
  return Status::OK();
}

Status PythonDecimalToString(PyObject* python_decimal, std::string* out) {
  // Call Python's str(decimal_object)
  OwnedRef str_obj(PyObject_Str(python_decimal));
  RETURN_IF_PYERROR();

  PyObjectStringify str(str_obj.obj());
  RETURN_IF_PYERROR();

  const char* bytes = str.bytes;
  DCHECK_NE(bytes, nullptr);

  Py_ssize_t size = str.size;

  std::string c_string(bytes, size);
  *out = c_string;
  return Status::OK();
}

Status InferDecimalPrecisionAndScale(PyObject* python_decimal, int32_t* precision,
                                     int32_t* scale) {
  DCHECK_NE(python_decimal, NULLPTR);
  DCHECK_NE(precision, NULLPTR);
  DCHECK_NE(scale, NULLPTR);

  OwnedRef as_tuple(PyObject_CallMethod(python_decimal, "as_tuple", "()"));
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

  *precision = num_digits;
  *scale = -exponent;
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

bool IsPyInteger(PyObject* obj) {
#if PYARROW_IS_PY2
  return PyLong_Check(obj) || PyInt_Check(obj);
#else
  return PyLong_Check(obj);
#endif
}

}  // namespace internal
}  // namespace py
}  // namespace arrow
