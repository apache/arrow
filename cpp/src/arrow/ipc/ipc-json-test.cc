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

#include "arrow/ipc/json-internal.h"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "rapidjson/document.h"
#include "rapidjson/filewritestream.h"
#include "rapidjson/prettywriter.h"
#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/types/primitive.h"
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

  rj::Document d;
  d.Parse(sb.GetString());

  std::shared_ptr<Array> out;
  ASSERT_OK(ReadJsonArray(default_memory_pool(), d, array.type(), &out));

  ASSERT_TRUE(array.Equals(out));
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


TEST(TestJsonArrayWriter, PrimitiveTypes) {
  std::vector<bool> is_valid = {true, false, true, true, true, false, true, true};

  std::vector<uint8_t> u1 = {0, 1, 2, 3, 4, 5, 6, 7};
  CheckPrimitive<UInt8Type, uint8_t>(uint8(), is_valid, u1);
}

}  // namespace ipc
}  // namespace arrow
