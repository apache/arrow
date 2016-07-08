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

#include <arrow/api.h>
#include <memory>

#include "pyarrow/visibility.h"

namespace pyarrow {

using arrow::DataType;
using arrow::Type;

extern const std::shared_ptr<arrow::NullType> NA;
extern const std::shared_ptr<arrow::BooleanType> BOOL;
extern const std::shared_ptr<arrow::UInt8Type> UINT8;
extern const std::shared_ptr<arrow::UInt16Type> UINT16;
extern const std::shared_ptr<arrow::UInt32Type> UINT32;
extern const std::shared_ptr<arrow::UInt64Type> UINT64;
extern const std::shared_ptr<arrow::Int8Type> INT8;
extern const std::shared_ptr<arrow::Int16Type> INT16;
extern const std::shared_ptr<arrow::Int32Type> INT32;
extern const std::shared_ptr<arrow::Int64Type> INT64;
extern const std::shared_ptr<arrow::FloatType> FLOAT;
extern const std::shared_ptr<arrow::DoubleType> DOUBLE;
extern const std::shared_ptr<arrow::StringType> STRING;

PYARROW_EXPORT
std::shared_ptr<DataType> GetPrimitiveType(Type::type type);

} // namespace pyarrow

#endif // PYARROW_HELPERS_H
