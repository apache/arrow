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

#include <cstdint>
#include <memory>
#include <vector>

#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/schema.h"

namespace arrow {

class Field;
class Schema;

}  // namespace arrow

namespace parquet {

class WriterProperties;

namespace arrow {

class ArrowWriterProperties;

PARQUET_EXPORT
::arrow::Status FieldToNode(const std::shared_ptr<::arrow::Field>& field,
                            const WriterProperties& properties,
                            const ArrowWriterProperties& arrow_properties,
                            schema::NodePtr* out);

PARQUET_EXPORT
::arrow::Status ToParquetSchema(const ::arrow::Schema* arrow_schema,
                                const WriterProperties& properties,
                                const ArrowWriterProperties& arrow_properties,
                                std::shared_ptr<SchemaDescriptor>* out);

PARQUET_EXPORT
::arrow::Status ToParquetSchema(const ::arrow::Schema* arrow_schema,
                                const WriterProperties& properties,
                                std::shared_ptr<SchemaDescriptor>* out);

PARQUET_EXPORT
int32_t DecimalSize(int32_t precision);

}  // namespace arrow

}  // namespace parquet

#endif  // PARQUET_ARROW_SCHEMA_H
