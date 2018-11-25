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
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/stl.h"
#include "arrow/type.h"

namespace arrow {
namespace stl {

TEST(TestSchemaFromTuple, PrimitiveTypesVector) {
  Schema expected_schema(
      {field("column1", int8(), false), field("column2", int16(), false),
       field("column3", int32(), false), field("column4", int64(), false),
       field("column5", uint8(), false), field("column6", uint16(), false),
       field("column7", uint32(), false), field("column8", uint64(), false),
       field("column9", boolean(), false), field("column10", utf8(), false)});

  std::shared_ptr<Schema> schema =
      SchemaFromTuple<std::tuple<int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t,
                                 uint32_t, uint64_t, bool, std::string>>::
          MakeSchema(std::vector<std::string>({"column1", "column2", "column3", "column4",
                                               "column5", "column6", "column7", "column8",
                                               "column9", "column10"}));
  ASSERT_TRUE(expected_schema.Equals(*schema));
}

TEST(TestSchemaFromTuple, PrimitiveTypesTuple) {
  Schema expected_schema(
      {field("column1", int8(), false), field("column2", int16(), false),
       field("column3", int32(), false), field("column4", int64(), false),
       field("column5", uint8(), false), field("column6", uint16(), false),
       field("column7", uint32(), false), field("column8", uint64(), false),
       field("column9", boolean(), false), field("column10", utf8(), false)});

  std::shared_ptr<Schema> schema = SchemaFromTuple<
      std::tuple<int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t,
                 bool, std::string>>::MakeSchema(std::make_tuple("column1", "column2",
                                                                 "column3", "column4",
                                                                 "column5", "column6",
                                                                 "column7", "column8",
                                                                 "column9", "column10"));
  ASSERT_TRUE(expected_schema.Equals(*schema));
}

TEST(TestSchemaFromTuple, SimpleList) {
  Schema expected_schema({field("column1", list(utf8()), false)});
  std::shared_ptr<Schema> schema =
      SchemaFromTuple<std::tuple<std::vector<std::string>>>::MakeSchema({"column1"});

  ASSERT_TRUE(expected_schema.Equals(*schema));
}

TEST(TestSchemaFromTuple, NestedList) {
  Schema expected_schema({field("column1", list(list(boolean())), false)});
  std::shared_ptr<Schema> schema =
      SchemaFromTuple<std::tuple<std::vector<std::vector<bool>>>>::MakeSchema(
          {"column1"});

  ASSERT_TRUE(expected_schema.Equals(*schema));
}

}  // namespace stl
}  // namespace arrow
