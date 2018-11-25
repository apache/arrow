// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/io/file.h>
#include <arrow/util/logging.h>

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

using parquet::LogicalType;
using parquet::Repetition;
using parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

constexpr int FIXED_LENGTH = 10;

static std::shared_ptr<GroupNode> SetupSchema() {
  parquet::schema::NodeVector fields;
  // Create a primitive node named 'boolean_field' with type:BOOLEAN,
  // repetition:REQUIRED
  fields.push_back(PrimitiveNode::Make("boolean_field", Repetition::REQUIRED,
                                       Type::BOOLEAN, LogicalType::NONE));

  // Create a primitive node named 'int32_field' with type:INT32, repetition:REQUIRED,
  // logical type:TIME_MILLIS
  fields.push_back(PrimitiveNode::Make("int32_field", Repetition::REQUIRED, Type::INT32,
                                       LogicalType::TIME_MILLIS));

  // Create a primitive node named 'int64_field' with type:INT64, repetition:REPEATED
  fields.push_back(PrimitiveNode::Make("int64_field", Repetition::REPEATED, Type::INT64,
                                       LogicalType::NONE));

  fields.push_back(PrimitiveNode::Make("int96_field", Repetition::REQUIRED, Type::INT96,
                                       LogicalType::NONE));

  fields.push_back(PrimitiveNode::Make("float_field", Repetition::REQUIRED, Type::FLOAT,
                                       LogicalType::NONE));

  fields.push_back(PrimitiveNode::Make("double_field", Repetition::REQUIRED, Type::DOUBLE,
                                       LogicalType::NONE));

  // Create a primitive node named 'ba_field' with type:BYTE_ARRAY, repetition:OPTIONAL
  fields.push_back(PrimitiveNode::Make("ba_field", Repetition::OPTIONAL, Type::BYTE_ARRAY,
                                       LogicalType::NONE));

  // Create a primitive node named 'flba_field' with type:FIXED_LEN_BYTE_ARRAY,
  // repetition:REQUIRED, field_length = FIXED_LENGTH
  fields.push_back(PrimitiveNode::Make("flba_field", Repetition::REQUIRED,
                                       Type::FIXED_LEN_BYTE_ARRAY, LogicalType::NONE,
                                       FIXED_LENGTH));

  // Create a GroupNode named 'schema' using the primitive nodes defined above
  // This GroupNode is the root node of the schema tree
  return std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));
}
