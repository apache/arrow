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

#include "arrow/result.h"
#include "parquet/schema.h"

namespace arrow {
class DataType;
}

namespace parquet::arrow {

using ::arrow::Result;

Result<std::shared_ptr<::arrow::DataType>> FromByteArray(const LogicalType& logical_type);
Result<std::shared_ptr<::arrow::DataType>> FromFLBA(const LogicalType& logical_type,
                                                    int32_t physical_length);
Result<std::shared_ptr<::arrow::DataType>> FromInt32(const LogicalType& logical_type);
Result<std::shared_ptr<::arrow::DataType>> FromInt64(const LogicalType& logical_type);

Result<std::shared_ptr<::arrow::DataType>> GetArrowType(
    Type::type physical_type, const LogicalType& logical_type, int type_length,
    ::arrow::TimeUnit::type int96_arrow_time_unit = ::arrow::TimeUnit::NANO);

Result<std::shared_ptr<::arrow::DataType>> GetArrowType(
    const schema::PrimitiveNode& primitive,
    ::arrow::TimeUnit::type int96_arrow_time_unit = ::arrow::TimeUnit::NANO);

}  // namespace parquet::arrow
