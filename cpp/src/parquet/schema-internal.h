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

// This module contains the logical parquet-cpp types (independent of Thrift
// structures), schema nodes, and related type tools

#ifndef PARQUET_SCHEMA_INTERNAL_H
#define PARQUET_SCHEMA_INTERNAL_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
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

std::shared_ptr<SchemaDescriptor> FromParquet(
    const std::vector<format::SchemaElement>& schema);

class FlatSchemaConverter {
 public:
  FlatSchemaConverter(const format::SchemaElement* elements, int length)
      : elements_(elements), length_(length), pos_(0), current_id_(0) {}

  std::unique_ptr<Node> Convert();

 private:
  const format::SchemaElement* elements_;
  int length_;
  int pos_;
  int current_id_;

  int next_id() { return current_id_++; }

  const format::SchemaElement& Next();

  std::unique_ptr<Node> NextNode();
};

// ----------------------------------------------------------------------
// Conversion to Parquet Thrift metadata

void ToParquet(const GroupNode* schema, std::vector<format::SchemaElement>* out);

// Converts nested parquet schema back to a flat vector of Thrift structs
class SchemaFlattener {
 public:
  SchemaFlattener(const GroupNode* schema, std::vector<format::SchemaElement>* out);

  void Flatten();

 private:
  const GroupNode* root_;
  std::vector<format::SchemaElement>* elements_;
};

}  // namespace schema
}  // namespace parquet

#endif  // PARQUET_SCHEMA_INTERNAL_H
