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

#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "parquet/properties.h"
#include "parquet/variant/builder.h"

namespace arrow::extension {
class VariantArray;
}  // namespace arrow::extension

namespace parquet::variant::internal {

using ::arrow::Array;
using ::arrow::Buffer;
using ::arrow::DataType;
using ::arrow::FieldVector;
using ::arrow::Result;
using ::arrow::Status;

std::shared_ptr<::arrow::Table> VariantTable(
    const std::shared_ptr<DataType>& variant_type,
    const std::vector<std::shared_ptr<Array>>& storage_children,
    const FieldVector& storage_fields);

Result<std::shared_ptr<Buffer>> WriteVariantTable(
    const std::shared_ptr<::arrow::Table>& table,
    std::shared_ptr<WriterProperties> writer_properties = default_writer_properties(),
    std::shared_ptr<ArrowWriterProperties> arrow_properties =
        default_arrow_writer_properties());

Status WriteVariantRecordBatch(const std::shared_ptr<::arrow::Table>& table,
                               std::shared_ptr<ArrowWriterProperties> arrow_properties =
                                   default_arrow_writer_properties());

Result<std::shared_ptr<::arrow::extension::VariantArray>> RoundTripVariantArray(
    const std::shared_ptr<::arrow::extension::VariantArray>& array);

std::shared_ptr<Buffer> EmptyVariantMetadata();

EncodedVariantValue Int8Variant(int8_t value);

std::shared_ptr<Array> BinaryArrayFromValues(
    const std::vector<std::optional<std::string_view>>& values);

std::shared_ptr<::arrow::StructArray> MakeInt64FieldGroup(
    const std::vector<std::optional<std::string_view>>& values,
    std::string_view typed_values, const std::vector<bool>& is_valid = {});

void AssertEncodedRow(const ::arrow::extension::VariantArray& array, int64_t row,
                      const EncodedVariantValue& expected);

void AssertUnshreddedValue(const ::arrow::extension::VariantArray& array, int64_t row,
                           const EncodedVariantValue& expected);

}  // namespace parquet::variant::internal
