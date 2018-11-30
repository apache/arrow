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
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <rapidjson/error/en.h>
#include <rapidjson/reader.h>

#include "arrow/json/options.h"
#include "arrow/json/parser.h"
#include "arrow/status.h"
#include "arrow/test-util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace json {

auto boring_json = R"(
    { "hello": 3.5, "world": false, "yo": "thing" }
    { "hello": 3.2, "world": true, "yo": "thingie" }
    { "hello": 3.4, "world": false, "yo": "thingy" }
    { "hello": 0.0, "world": true, "yo": "thingumy" }
  )";
auto boring_json_with_nulls = R"(
    { "hello": 3.5, "world": null }
    { "hello": null, "world": true }
    { "hello": 3.4, "world": false }
    { "hello": null, "world": true }
  )";

TEST(BlockParser, FailOnChangedColumnType) {
  auto options = ParseOptions::Defaults();
  options.explicit_schema = schema({field("a", int32())});
  BlockParser parser(options);
  uint32_t out_size;
  auto a_changes_type = "{\"a\":0}\n{\"a\":true}";
  ASSERT_RAISES(Invalid, parser.Parse(a_changes_type, sizeof(a_changes_type), &out_size));
}

TEST(BasicJson, ParserUsage) {
  auto options = ParseOptions::Defaults();
  options.explicit_schema =
      schema({field("hello", float64()), field("world", boolean()), field("yo", utf8())});
  BlockParser parser(options);
  uint32_t out_size;
  ASSERT_OK(parser.Parse(boring_json, sizeof(boring_json), &out_size));
  std::shared_ptr<RecordBatch> parsed;
  ASSERT_OK(parser.Finish(&parsed));

  std::shared_ptr<Array> hello_expected;
  ArrayFromVector<DoubleType>({3.5, 3.2, 3.4, 0.0}, &hello_expected);
  auto hello = parsed->column(parsed->schema()->GetFieldIndex("hello"));
  AssertArraysEqual(*hello, *hello_expected);

  std::shared_ptr<Array> world_expected;
  ArrayFromVector<BooleanType, bool>({0, 1, 0, 1}, &world_expected);
  auto world = parsed->column(parsed->schema()->GetFieldIndex("world"));
  AssertArraysEqual(*world, *world_expected);

  std::shared_ptr<Array> yo_expected;
  ArrayFromVector<StringType, std::string>({"thing", "thingie", "thingy", "thingumy"},
                                           &yo_expected);
  auto yo = parsed->column(parsed->schema()->GetFieldIndex("yo"));
  AssertArraysEqual(*yo, *yo_expected);
}

}  // namespace json
}  // namespace arrow
