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

#ifndef PARQUET_ARROW_SCHEMA_H
#define PARQUET_ARROW_SCHEMA_H

#include <memory>
#include <vector>

#include "arrow/api.h"

#include "parquet/api/schema.h"
#include "parquet/api/writer.h"
#include "parquet/arrow/writer.h"

namespace arrow {

class Status;

}  // namespace arrow

namespace parquet {

namespace arrow {

PARQUET_EXPORT
::arrow::Status NodeToField(const schema::Node& node,
                            std::shared_ptr<::arrow::Field>* out);

/// Convert parquet schema to arrow schema with selected indices
/// \param parquet_schema to be converted
/// \param column_indices indices of leaf nodes in parquet schema tree. Appearing ordering
///                       matters for the converted schema. Repeated indices are ignored
///                       except for the first one
/// \param key_value_metadata optional metadata, can be nullptr
/// \param out the corresponding arrow schema
/// \return Status::OK() on a successful conversion.
PARQUET_EXPORT
::arrow::Status FromParquetSchema(
    const SchemaDescriptor* parquet_schema, const std::vector<int>& column_indices,
    const std::shared_ptr<const KeyValueMetadata>& key_value_metadata,
    std::shared_ptr<::arrow::Schema>* out);

// Without indices
PARQUET_EXPORT
::arrow::Status FromParquetSchema(
    const SchemaDescriptor* parquet_schema,
    const std::shared_ptr<const KeyValueMetadata>& key_value_metadata,
    std::shared_ptr<::arrow::Schema>* out);

// Without metadata
::arrow::Status PARQUET_EXPORT FromParquetSchema(const SchemaDescriptor* parquet_schema,
                                                 const std::vector<int>& column_indices,
                                                 std::shared_ptr<::arrow::Schema>* out);

// Without metadata or indices
::arrow::Status PARQUET_EXPORT FromParquetSchema(const SchemaDescriptor* parquet_schema,
                                                 std::shared_ptr<::arrow::Schema>* out);

::arrow::Status PARQUET_EXPORT FieldToNode(const std::shared_ptr<::arrow::Field>& field,
                                           const WriterProperties& properties,
                                           const ArrowWriterProperties& arrow_properties,
                                           schema::NodePtr* out);

::arrow::Status PARQUET_EXPORT
ToParquetSchema(const ::arrow::Schema* arrow_schema, const WriterProperties& properties,
                const ArrowWriterProperties& arrow_properties,
                std::shared_ptr<SchemaDescriptor>* out);

::arrow::Status PARQUET_EXPORT ToParquetSchema(const ::arrow::Schema* arrow_schema,
                                               const WriterProperties& properties,
                                               std::shared_ptr<SchemaDescriptor>* out);

int32_t DecimalSize(int32_t precision);

}  // namespace arrow

}  // namespace parquet

#endif  // PARQUET_ARROW_SCHEMA_H
