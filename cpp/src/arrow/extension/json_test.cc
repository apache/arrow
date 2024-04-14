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

#include "arrow/extension/json.h"

#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {

using extension::json;

class TestJsonExtensionType : public ::testing::Test {};

std::shared_ptr<Array> ExampleJson() {
  std::shared_ptr<Array> arr = ArrayFromJSON(utf8(), R"([
    "null",
    "1234",
    "3.14159",
    "true",
    "false",
    "\"a json string\"",
    "[\"a\", \"json\", \"array\"]",
    "{\"obj\": \"a simple json object\"}"
   ])");
  return ExtensionType::WrapArray(arrow::extension::json(), arr);
}

TEST_F(TestJsonExtensionType, JsonRoundtrip) {
  auto ext_arr = ExampleJson();

  auto batch = RecordBatch::Make(schema({field("f0", json())}), 8, {ext_arr});
  std::shared_ptr<RecordBatch> read_batch;
  RoundtripBatch(batch, &read_batch);
  CompareBatch(*batch, *read_batch, false /* compare_metadata */);
}

}  // namespace arrow
