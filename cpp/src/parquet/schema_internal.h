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

// Non-public Thrift schema serialization utilities

#pragma once

#include <memory>
#include <vector>

#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace parquet {

namespace format {
class SchemaElement;
}

namespace schema {

// ----------------------------------------------------------------------
// Conversion from Parquet Thrift metadata

PARQUET_EXPORT
std::shared_ptr<SchemaDescriptor> FromParquet(
    const std::vector<format::SchemaElement>& schema);

PARQUET_EXPORT
std::unique_ptr<Node> Unflatten(const format::SchemaElement* elements, int length);

// ----------------------------------------------------------------------
// Conversion to Parquet Thrift metadata

PARQUET_EXPORT
void ToParquet(const GroupNode* schema, std::vector<format::SchemaElement>* out);

}  // namespace schema
}  // namespace parquet
