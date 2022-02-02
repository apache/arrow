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

#pragma once

#include <memory>
#include <vector>

#include "arrow/array.h"         // IWYU pragma: export
#include "arrow/builder.h"       // IWYU pragma: export
#include "arrow/pretty_print.h"  // IWYU pragma: export
#include "arrow/record_batch.h"  // IWYU pragma: export
#include "arrow/status.h"        // IWYU pragma: export
#include "arrow/type.h"          // IWYU pragma: export

namespace gandiva {

using arrow::ArrayDataVector;
using arrow::DataTypeVector;
using arrow::FieldVector;
using arrow::Result;
using arrow::Status;
using arrow::StatusCode;

using ArrayPtr = std::shared_ptr<arrow::Array>;
using ArrayDataPtr = std::shared_ptr<arrow::ArrayData>;
using DataTypePtr = std::shared_ptr<arrow::DataType>;
using FieldPtr = std::shared_ptr<arrow::Field>;
using RecordBatchPtr = std::shared_ptr<arrow::RecordBatch>;
using SchemaPtr = std::shared_ptr<arrow::Schema>;

using Decimal128TypePtr = std::shared_ptr<arrow::Decimal128Type>;
using Decimal128TypeVector = std::vector<Decimal128TypePtr>;

static inline bool is_decimal_128(DataTypePtr type) {
  if (type->id() == arrow::Type::DECIMAL) {
    auto decimal_type = arrow::internal::checked_cast<arrow::DecimalType*>(type.get());
    return decimal_type->byte_width() == 16;
  } else {
    return false;
  }
}

static inline bool IsArrowStringLiteral(arrow::Type::type type) {
  return type == arrow::Type::STRING || type == arrow::Type::BINARY;
}

}  // namespace gandiva
