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

#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/util/memory-pool.h"
#include "arrow/util/status.h"

namespace arrow {
namespace ipc {

class TestJsonSchemaWriter : public ::testing::Test {
 public:
  void SetUp() {}
  void TearDown() {}

  void TestRoundTrip(const Schema& schema) {
    rj::StringBuffer sb;
    rj::Writer<rj::StringBuffer> writer(sb);

    ASSERT_OK(WriteJsonSchema(schema, &writer));

    rj::Document d;
    d.Parse(sb.GetString());

    std::shared_ptr<Schema> out;
    ASSERT_OK(ReadJsonSchema(d, &out));

    ASSERT_TRUE(schema.Equals(out));
  }
};

TEST_F(TestJsonSchemaWriter, FlatTypes) {
  std::vector<std::shared_ptr<Field>> fields = {
      field("f0", int8()), field("f1", int16(), false), field("f2", int32()),
      field("f3", int64(), false), field("f4", uint8()), field("f5", uint16()),
      field("f6", uint32()), field("f7", uint64()), field("f8", float32()),
      field("f9", float64()), field("f10", utf8()), field("f11", binary()),
      field("f12", list(int32())), field("f13", struct_({field("s1", int32()),
                field("s2", utf8())})),
      field("f14", date()), field("f15", timestamp(TimeUnit::NANO)),
      field("f16", timestamp(TimeUnit::MICRO)),
  };

  Schema schema(fields);
  TestRoundTrip(schema);
}

}  // namespace ipc
}  // namespace arrow
