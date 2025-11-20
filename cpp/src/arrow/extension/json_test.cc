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

#include "arrow/array/validate.h"
#include "arrow/ipc/test_common.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "parquet/exception.h"

namespace arrow {

using arrow::ipc::test::RoundtripBatch;
using extension::json;

class TestJsonExtensionType : public ::testing::Test {};

std::shared_ptr<Array> ExampleJson(const std::shared_ptr<DataType>& storage_type) {
  std::shared_ptr<Array> arr = ArrayFromJSON(storage_type, R"([
    "null",
    "1234",
    "3.14159",
    "true",
    "false",
    "\"a json string\"",
    "[\"a\", \"json\", \"array\"]",
    "{\"obj\": \"a simple json object\"}"
   ])");
  return ExtensionType::WrapArray(arrow::extension::json(storage_type), arr);
}

TEST_F(TestJsonExtensionType, JsonRoundtrip) {
  for (const auto& storage_type : {utf8(), large_utf8(), utf8_view()}) {
    std::shared_ptr<Array> ext_arr = ExampleJson(storage_type);
    auto batch =
        RecordBatch::Make(schema({field("f0", json(storage_type))}), 8, {ext_arr});

    std::shared_ptr<RecordBatch> read_batch;
    ASSERT_OK(RoundtripBatch(batch, &read_batch));
    ASSERT_OK(read_batch->ValidateFull());
    CompareBatch(*batch, *read_batch, /*compare_metadata*/ true);

    auto read_ext_arr = read_batch->column(0);
    ASSERT_OK(internal::ValidateUTF8(*read_ext_arr));
    ASSERT_OK(read_ext_arr->ValidateFull());
  }
}

TEST_F(TestJsonExtensionType, InvalidUTF8) {
  for (const auto& storage_type : {utf8(), large_utf8(), utf8_view()}) {
    auto json_type = json(storage_type);
    auto invalid_input = ArrayFromJSON(storage_type, "[\"Ⱥa\xFFⱭ\", \"Ɽ\xe1\xbdⱤaA\"]");
    auto ext_arr = ExtensionType::WrapArray(json_type, invalid_input);

    ASSERT_RAISES_WITH_MESSAGE(Invalid,
                               "Invalid: Invalid UTF8 sequence at string index 0",
                               ext_arr->ValidateFull());
    ASSERT_RAISES_WITH_MESSAGE(Invalid,
                               "Invalid: Invalid UTF8 sequence at string index 0",
                               arrow::internal::ValidateUTF8(*ext_arr));

    auto batch = RecordBatch::Make(schema({field("f0", json_type)}), 2, {ext_arr});
    std::shared_ptr<RecordBatch> read_batch;
    ASSERT_OK(RoundtripBatch(batch, &read_batch));
  }
}

TEST_F(TestJsonExtensionType, StorageTypeValidation) {
  ASSERT_TRUE(json(utf8())->Equals(json(utf8())));
  ASSERT_FALSE(json(large_utf8())->Equals(json(utf8())));
  ASSERT_FALSE(json(utf8_view())->Equals(json(utf8())));
  ASSERT_FALSE(json(utf8_view())->Equals(json(large_utf8())));

  for (const auto& storage_type : {int16(), binary(), float64(), null()}) {
    ASSERT_RAISES_WITH_MESSAGE(Invalid,
                               "Invalid: Invalid storage type for JsonExtensionType: " +
                                   storage_type->ToString(),
                               extension::JsonExtensionType::Make(storage_type));
  }
}

}  // namespace arrow
