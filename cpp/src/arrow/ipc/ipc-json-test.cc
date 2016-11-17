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

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/ipc/json-internal.h"
#include "arrow/ipc/json.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/types/primitive.h"
#include "arrow/types/string.h"
#include "arrow/types/struct.h"
#include "arrow/util/memory-pool.h"
#include "arrow/util/status.h"

namespace arrow {
namespace ipc {

void TestSchemaRoundTrip(const Schema& schema) {
  rj::StringBuffer sb;
  rj::Writer<rj::StringBuffer> writer(sb);

  ASSERT_OK(WriteJsonSchema(schema, &writer));

  rj::Document d;
  d.Parse(sb.GetString());

  std::shared_ptr<Schema> out;
  ASSERT_OK(ReadJsonSchema(d, &out));

  ASSERT_TRUE(schema.Equals(out));
}

void TestArrayRoundTrip(const Array& array) {
  static std::string name = "dummy";

  rj::StringBuffer sb;
  rj::Writer<rj::StringBuffer> writer(sb);

  ASSERT_OK(WriteJsonArray(name, array, &writer));

  std::string array_as_json = sb.GetString();

  rj::Document d;
  d.Parse(array_as_json);

  if (d.HasParseError()) { FAIL() << "JSON parsing failed"; }

  std::shared_ptr<Array> out;
  ASSERT_OK(ReadJsonArray(default_memory_pool(), d, array.type(), &out));

  ASSERT_TRUE(array.Equals(out)) << array_as_json;
}

template <typename T, typename ValueType>
void CheckPrimitive(const std::shared_ptr<DataType>& type,
    const std::vector<bool>& is_valid, const std::vector<ValueType>& values) {
  MemoryPool* pool = default_memory_pool();
  typename TypeTraits<T>::BuilderType builder(pool, type);

  for (size_t i = 0; i < values.size(); ++i) {
    if (is_valid[i]) {
      ASSERT_OK(builder.Append(values[i]));
    } else {
      ASSERT_OK(builder.AppendNull());
    }
  }

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));
  TestArrayRoundTrip(*array.get());
}

template <typename TYPE, typename C_TYPE>
void MakeArray(const std::shared_ptr<DataType>& type,
    const std::vector<bool>& is_valid, const std::vector<C_TYPE>& values,
    std::shared_ptr<Array>* out) {
  std::shared_ptr<Buffer> values_buffer = test::GetBufferFromVector(values);
  std::shared_ptr<Buffer> values_bitmap;
  ASSERT_OK(test::GetBitmapFromBoolVector(is_valid, &values_bitmap));

  using ArrayType = typename TypeTraits<TYPE>::ArrayType;

  int32_t null_count = 0;
  for (bool val : is_valid) {
    if (!val) { ++null_count; }
  }

  *out = std::make_shared<ArrayType>(type, static_cast<int32_t>(values.size()),
      values_buffer, null_count, values_bitmap);
}

TEST(TestJsonSchemaWriter, FlatTypes) {
  std::vector<std::shared_ptr<Field>> fields = {field("f0", int8()),
      field("f1", int16(), false), field("f2", int32()), field("f3", int64(), false),
      field("f4", uint8()), field("f5", uint16()), field("f6", uint32()),
      field("f7", uint64()), field("f8", float32()), field("f9", float64()),
      field("f10", utf8()), field("f11", binary()), field("f12", list(int32())),
      field("f13", struct_({field("s1", int32()), field("s2", utf8())})),
      field("f14", date()), field("f15", timestamp(TimeUnit::NANO)),
      field("f16", time(TimeUnit::MICRO)),
      field("f17", union_({field("u1", int8()), field("u2", time(TimeUnit::MILLI))},
                       {0, 1}, UnionMode::DENSE))};

  Schema schema(fields);
  TestSchemaRoundTrip(schema);
}

template <typename T>
void PrimitiveTypesCheckOne() {
  using c_type = typename T::c_type;

  std::vector<bool> is_valid = {true, false, true, true, true, false, true, true};
  std::vector<c_type> values = {0, 1, 2, 3, 4, 5, 6, 7};
  CheckPrimitive<T, c_type>(std::make_shared<T>(), is_valid, values);
}

TEST(TestJsonArrayWriter, PrimitiveTypes) {
  PrimitiveTypesCheckOne<Int8Type>();
  PrimitiveTypesCheckOne<Int16Type>();
  PrimitiveTypesCheckOne<Int32Type>();
  PrimitiveTypesCheckOne<Int64Type>();
  PrimitiveTypesCheckOne<UInt8Type>();
  PrimitiveTypesCheckOne<UInt16Type>();
  PrimitiveTypesCheckOne<UInt32Type>();
  PrimitiveTypesCheckOne<UInt64Type>();
  PrimitiveTypesCheckOne<FloatType>();
  PrimitiveTypesCheckOne<DoubleType>();

  std::vector<bool> is_valid = {true, false, true, true, true, false, true, true};
  std::vector<std::string> values = {"foo", "bar", "", "baz", "qux", "foo", "a", "1"};

  CheckPrimitive<StringType, std::string>(utf8(), is_valid, values);
  CheckPrimitive<BinaryType, std::string>(binary(), is_valid, values);
}

TEST(TestJsonArrayWriter, NestedTypes) {
  auto value_type = int32();

  std::vector<bool> values_is_valid = {true, false, true, true, false, true, true};
  std::vector<int32_t> values = {0, 1, 2, 3, 4, 5, 6};

  std::shared_ptr<Array> values_array;
  MakeArray<Int32Type, int32_t>(int32(), values_is_valid, values, &values_array);

  // List
  std::vector<bool> list_is_valid = {true, false, true, true, true};
  std::vector<int32_t> offsets = {0, 0, 0, 1, 4, 7};

  std::shared_ptr<Buffer> list_bitmap;
  ASSERT_OK(test::GetBitmapFromBoolVector(list_is_valid, &list_bitmap));
  std::shared_ptr<Buffer> offsets_buffer = test::GetBufferFromVector(offsets);

  ListArray list_array(list(value_type), 5, offsets_buffer, values_array, 1, list_bitmap);

  TestArrayRoundTrip(list_array);

  // Struct
  std::vector<bool> struct_is_valid = {true, false, true, true, true, false, true};
  std::shared_ptr<Buffer> struct_bitmap;
  ASSERT_OK(test::GetBitmapFromBoolVector(struct_is_valid, &struct_bitmap));

  auto struct_type =
      struct_({field("f1", int32()), field("f2", int32()), field("f3", int32())});

  std::vector<std::shared_ptr<Array>> fields = {values_array, values_array, values_array};
  StructArray struct_array(
      struct_type, static_cast<int>(struct_is_valid.size()), fields, 2, struct_bitmap);
  TestArrayRoundTrip(struct_array);
}

TEST(TestJsonFileReadWrite, BasicRoundTrip) {
  auto v1_type = int8();
  auto v2_type = int32();
  auto v3_type = utf8();

  std::vector<bool> is_valid = {true, false, true, true, false, true, true};

  std::vector<int8_t> v1_values = {0, 1, 2, 3, 4, 5, 6};
  std::shared_ptr<Array> v1;
  MakeArray<Int8Type, int8_t>(v1_type, is_valid, v1_values, &v1);

  std::vector<int32_t> v2_values = {0, 1, 2, 3, 4, 5, 6};
  std::shared_ptr<Array> v2;
  MakeArray<Int32Type, int32_t>(v2_type, is_valid, v2_values, &v2);

  std::vector<std::string> v3_values = {"foo", "bar", "", "", "", "baz", "qux"};
  std::shared_ptr<Array> v3;
  MakeArray<StringType, std::string>(v3_type, is_valid, v3_values, &v3);

  std::shared_ptr<Schema> schema({field("f1", v1_type), field("f2", v2_type),
          field("f3", v3_type)});

  std::vector<std::shared_ptr<Array>> arrays = {v1, v2, v3}

  std::unique_ptr<JsonWriter> writer;
  ASSERT_OK(JsonWriter::Open(schema, &writer));

  const int nbatches = 3;
  const int32_t num_rows = static_cast<int32_t>(v1_values.size());

  for (int i = 0; i < nbatches; ++i) {
    ASSERT_OK(writer_->WriteRecordBatch(arrays, num_rows));
  }

  std::shared_ptr<Buffer> data;
  ASSERT_OK(writer->Finish(&data));
}

}  // namespace ipc
}  // namespace arrow
