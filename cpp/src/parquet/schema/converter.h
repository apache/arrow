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

// Conversion routines for converting to and from flat Parquet metadata. Among
// other things, this limits the exposure of the internals of the Thrift
// metadata structs to the rest of the library.

// NB: This file is not part of the schema public API and only used internally
// for converting to and from Parquet Thrift metadata

#ifndef PARQUET_SCHEMA_CONVERTER_H
#define PARQUET_SCHEMA_CONVERTER_H

#include <memory>
#include <vector>

namespace parquet { class SchemaElement;}

namespace parquet_cpp {

class SchemaDescriptor;

namespace schema {

class GroupNode;
class Node;

// ----------------------------------------------------------------------
// Conversion from Parquet Thrift metadata

std::shared_ptr<SchemaDescriptor> FromParquet(
    const std::vector<parquet::SchemaElement>& schema);

class FlatSchemaConverter {
 public:
  FlatSchemaConverter(const parquet::SchemaElement* elements, int length) :
      elements_(elements),
      length_(length),
      pos_(0),
      current_id_(0) {}

  std::unique_ptr<Node> Convert();

 private:
  const parquet::SchemaElement* elements_;
  int length_;
  int pos_;
  int current_id_;

  int next_id() {
    return current_id_++;
  }

  const parquet::SchemaElement& Next();

  std::unique_ptr<Node> NextNode();
};

// ----------------------------------------------------------------------
// Conversion to Parquet Thrift metadata

void ToParquet(const GroupNode* schema, std::vector<parquet::SchemaElement>* out);

// Converts nested parquet_cpp schema back to a flat vector of Thrift structs
class SchemaFlattener {
 public:
  SchemaFlattener(const GroupNode* schema, std::vector<parquet::SchemaElement>* out);

 private:
  const GroupNode* root_;
  std::vector<parquet::SchemaElement>* schema_;
};

} // namespace schema

} // namespace parquet_cpp

#endif // PARQUET_SCHEMA_CONVERTER_H
