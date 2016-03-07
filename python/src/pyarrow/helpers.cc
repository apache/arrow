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

#include "pyarrow/helpers.h"

#include <arrow/api.h>

using namespace arrow;

namespace pyarrow {

#define GET_PRIMITIVE_TYPE(NAME, Type)          \
  case LogicalType::NAME:                       \
    if (nullable) {                             \
      return NAME;                              \
    } else {                                    \
      return std::make_shared<Type>(nullable);  \
    }                                           \
    break;

std::shared_ptr<DataType> GetPrimitiveType(LogicalType::type type,
    bool nullable) {
  switch (type) {
    case LogicalType::NA:
      return NA;
    GET_PRIMITIVE_TYPE(UINT8, UInt8Type);
    GET_PRIMITIVE_TYPE(INT8, Int8Type);
    GET_PRIMITIVE_TYPE(UINT16, UInt16Type);
    GET_PRIMITIVE_TYPE(INT16, Int16Type);
    GET_PRIMITIVE_TYPE(UINT32, UInt32Type);
    GET_PRIMITIVE_TYPE(INT32, Int32Type);
    GET_PRIMITIVE_TYPE(UINT64, UInt64Type);
    GET_PRIMITIVE_TYPE(INT64, Int64Type);
    GET_PRIMITIVE_TYPE(BOOL, BooleanType);
    GET_PRIMITIVE_TYPE(FLOAT, FloatType);
    GET_PRIMITIVE_TYPE(DOUBLE, DoubleType);
    GET_PRIMITIVE_TYPE(STRING, StringType);
    default:
      return nullptr;
  }
}

} // namespace pyarrow
