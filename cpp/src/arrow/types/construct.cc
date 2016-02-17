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

#include "arrow/types/construct.h"

#include <memory>

#include "arrow/types/floating.h"
#include "arrow/types/integer.h"
#include "arrow/types/list.h"
#include "arrow/types/string.h"
#include "arrow/util/status.h"

namespace arrow {

class ArrayBuilder;

// Initially looked at doing this with vtables, but shared pointers makes it
// difficult

#define BUILDER_CASE(ENUM, BuilderType)                         \
    case TypeEnum::ENUM:                                        \
      *out = static_cast<ArrayBuilder*>(new BuilderType(type)); \
      return Status::OK();

Status make_builder(const TypePtr& type, ArrayBuilder** out) {
  switch (type->type) {
    BUILDER_CASE(UINT8, UInt8Builder);
    BUILDER_CASE(INT8, Int8Builder);
    BUILDER_CASE(UINT16, UInt16Builder);
    BUILDER_CASE(INT16, Int16Builder);
    BUILDER_CASE(UINT32, UInt32Builder);
    BUILDER_CASE(INT32, Int32Builder);
    BUILDER_CASE(UINT64, UInt64Builder);
    BUILDER_CASE(INT64, Int64Builder);

    // BUILDER_CASE(BOOL, BooleanBuilder);

    BUILDER_CASE(FLOAT, FloatBuilder);
    BUILDER_CASE(DOUBLE, DoubleBuilder);

    BUILDER_CASE(STRING, StringBuilder);

    case TypeEnum::LIST:
      {
        ListType* list_type = static_cast<ListType*>(type.get());
        ArrayBuilder* value_builder;
        RETURN_NOT_OK(make_builder(list_type->value_type, &value_builder));

        // The ListBuilder takes ownership of the value_builder
        ListBuilder* builder = new ListBuilder(type, value_builder);
        *out = static_cast<ArrayBuilder*>(builder);
        return Status::OK();
      }
    // BUILDER_CASE(CHAR, CharBuilder);

    // BUILDER_CASE(VARCHAR, VarcharBuilder);
    // BUILDER_CASE(BINARY, BinaryBuilder);

    // BUILDER_CASE(DATE, DateBuilder);
    // BUILDER_CASE(TIMESTAMP, TimestampBuilder);
    // BUILDER_CASE(TIME, TimeBuilder);

    // BUILDER_CASE(LIST, ListBuilder);
    // BUILDER_CASE(STRUCT, StructBuilder);
    // BUILDER_CASE(DENSE_UNION, DenseUnionBuilder);
    // BUILDER_CASE(SPARSE_UNION, SparseUnionBuilder);

    default:
      return Status::NotImplemented(type->ToString());
  }
}

} // namespace arrow
