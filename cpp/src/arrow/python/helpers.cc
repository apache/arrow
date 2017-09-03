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

#include "arrow/python/helpers.h"
#include "arrow/python/common.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"

#include <arrow/api.h>

namespace arrow {
namespace py {

#define GET_PRIMITIVE_TYPE(NAME, FACTORY) \
  case Type::NAME:                        \
    return FACTORY();                     \
    break;

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

Status InferDecimalPrecisionAndScale(PyObject* python_decimal, int* precision,
                                     int* scale) {
  // Call Python's str(decimal_object)
  OwnedRef str_obj(PyObject_Str(python_decimal));
  RETURN_IF_PYERROR();
  PyObjectStringify str(str_obj.obj());

  const char* bytes = str.bytes;
  DCHECK_NE(bytes, nullptr);

  auto size = str.size;

  std::string c_string(bytes, size);
  return decimal::FromString(c_string, nullptr, precision, scale);
}

Status DecimalFromString(PyObject* decimal_constructor, const std::string& decimal_string,
                         PyObject** out) {
  DCHECK_NE(decimal_constructor, nullptr);
  DCHECK_NE(out, nullptr);

  auto string_size = decimal_string.size();
  DCHECK_GT(string_size, 0);

  auto string_bytes = decimal_string.c_str();
  DCHECK_NE(string_bytes, nullptr);

  *out = PyObject_CallFunction(decimal_constructor, const_cast<char*>("s#"), string_bytes,
                               string_size);
  RETURN_IF_PYERROR();
  return Status::OK();
}

}  // namespace py
}  // namespace arrow
