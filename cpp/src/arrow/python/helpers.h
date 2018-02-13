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

#ifndef PYARROW_HELPERS_H
#define PYARROW_HELPERS_H

#include "arrow/python/platform.h"

#include <memory>
#include <string>
#include <utility>

#include "arrow/type.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Decimal128;

namespace py {

class OwnedRef;

ARROW_EXPORT
std::shared_ptr<DataType> GetPrimitiveType(Type::type type);

namespace internal {

Status ImportModule(const std::string& module_name, OwnedRef* ref);
Status ImportFromModule(const OwnedRef& module, const std::string& module_name,
                        OwnedRef* ref);

Status PythonDecimalToString(PyObject* python_decimal, std::string* out);

Status InferDecimalPrecisionAndScale(PyObject* python_decimal,
                                     int32_t* precision = NULLPTR,
                                     int32_t* scale = NULLPTR);

PyObject* DecimalFromString(PyObject* decimal_constructor,
                            const std::string& decimal_string);
Status DecimalFromPythonDecimal(PyObject* python_decimal, const DecimalType& arrow_type,
                                Decimal128* out);
bool IsPyInteger(PyObject* obj);

Status UInt64FromPythonInt(PyObject* obj, uint64_t* out);

}  // namespace internal
}  // namespace py
}  // namespace arrow

#endif  // PYARROW_HELPERS_H
