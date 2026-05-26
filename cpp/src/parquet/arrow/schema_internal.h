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
#include "arrow/type_fwd.h"
#include "parquet/schema.h"

namespace parquet::arrow {

using ::arrow::Result;

Result<std::shared_ptr<::arrow::DataType>> FromInt32(
    const LogicalType& logical_type, const ArrowReaderProperties& reader_properties);
Result<std::shared_ptr<::arrow::DataType>> FromInt64(
    const LogicalType& logical_type, const ArrowReaderProperties& reader_properties);

Result<std::shared_ptr<::arrow::DataType>> GetArrowType(
    Type::type physical_type, const LogicalType& logical_type, int type_length,
    const ArrowReaderProperties& reader_properties,
    const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata = nullptr);

Result<std::shared_ptr<::arrow::DataType>> GetArrowType(
    const schema::PrimitiveNode& primitive,
    const ArrowReaderProperties& reader_properties,
    const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata = nullptr);

}  // namespace parquet::arrow
