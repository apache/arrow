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

#include "arrow/type.h"
#include "arrow/types/list.h"
#include "arrow/types/primitive.h"
#include "arrow/types/string.h"
#include "arrow/types/struct.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

namespace arrow {

class ArrayBuilder;

#define BUILDER_CASE(ENUM, BuilderType)      \
  case Type::ENUM:                           \
    out->reset(new BuilderType(pool, type)); \
    return Status::OK();

// Initially looked at doing this with vtables, but shared pointers makes it
// difficult
//
// TODO(wesm): come up with a less monolithic strategy
Status MakeBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
    std::shared_ptr<ArrayBuilder>* out) {
  switch (type->type) {
    BUILDER_CASE(UINT8, UInt8Builder);
    BUILDER_CASE(INT8, Int8Builder);
    BUILDER_CASE(UINT16, UInt16Builder);
    BUILDER_CASE(INT16, Int16Builder);
    BUILDER_CASE(UINT32, UInt32Builder);
    BUILDER_CASE(INT32, Int32Builder);
    BUILDER_CASE(UINT64, UInt64Builder);
    BUILDER_CASE(INT64, Int64Builder);
    BUILDER_CASE(TIMESTAMP, TimestampBuilder);

    BUILDER_CASE(BOOL, BooleanBuilder);

    BUILDER_CASE(FLOAT, FloatBuilder);
    BUILDER_CASE(DOUBLE, DoubleBuilder);

    BUILDER_CASE(STRING, StringBuilder);
    BUILDER_CASE(BINARY, BinaryBuilder);

    case Type::LIST: {
      std::shared_ptr<ArrayBuilder> value_builder;
      const std::shared_ptr<DataType>& value_type =
          static_cast<ListType*>(type.get())->value_type();
      RETURN_NOT_OK(MakeBuilder(pool, value_type, &value_builder));
      out->reset(new ListBuilder(pool, value_builder));
      return Status::OK();
    }

    case Type::STRUCT: {
      std::vector<FieldPtr>& fields = type->children_;
      std::vector<std::shared_ptr<ArrayBuilder>> values_builder;

      for (auto it : fields) {
        std::shared_ptr<ArrayBuilder> builder;
        RETURN_NOT_OK(MakeBuilder(pool, it->type, &builder));
        values_builder.push_back(builder);
      }
      out->reset(new StructBuilder(pool, type, values_builder));
      return Status::OK();
    }

    default:
      return Status::NotImplemented(type->ToString());
  }
}

#define MAKE_PRIMITIVE_ARRAY_CASE(ENUM, ArrayType)                          \
  case Type::ENUM:                                                          \
    out->reset(new ArrayType(type, length, data, null_count, null_bitmap)); \
    break;

Status MakePrimitiveArray(const TypePtr& type, int32_t length,
    const std::shared_ptr<Buffer>& data, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap, ArrayPtr* out) {
  switch (type->type) {
    MAKE_PRIMITIVE_ARRAY_CASE(BOOL, BooleanArray);
    MAKE_PRIMITIVE_ARRAY_CASE(UINT8, UInt8Array);
    MAKE_PRIMITIVE_ARRAY_CASE(INT8, Int8Array);
    MAKE_PRIMITIVE_ARRAY_CASE(UINT16, UInt16Array);
    MAKE_PRIMITIVE_ARRAY_CASE(INT16, Int16Array);
    MAKE_PRIMITIVE_ARRAY_CASE(UINT32, UInt32Array);
    MAKE_PRIMITIVE_ARRAY_CASE(INT32, Int32Array);
    MAKE_PRIMITIVE_ARRAY_CASE(UINT64, UInt64Array);
    MAKE_PRIMITIVE_ARRAY_CASE(INT64, Int64Array);
    MAKE_PRIMITIVE_ARRAY_CASE(FLOAT, FloatArray);
    MAKE_PRIMITIVE_ARRAY_CASE(DOUBLE, DoubleArray);
    MAKE_PRIMITIVE_ARRAY_CASE(TIME, Int64Array);
    MAKE_PRIMITIVE_ARRAY_CASE(TIMESTAMP, TimestampArray);
    MAKE_PRIMITIVE_ARRAY_CASE(TIMESTAMP_DOUBLE, DoubleArray);
    default:
      return Status::NotImplemented(type->ToString());
  }
#ifdef NDEBUG
  return Status::OK();
#else
  return (*out)->Validate();
#endif
}

}  // namespace arrow
