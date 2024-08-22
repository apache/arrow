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

#include "arrow/extension/uuid.h"

#include "arrow/testing/matchers.h"

#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/test_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/key_value_metadata.h"

#include "arrow/testing/extension_type.h"

namespace arrow {

using arrow::ipc::test::RoundtripBatch;

TEST(TestUuuidExtensionType, ExtensionTypeTest) {
  auto type = uuid();
  ASSERT_EQ(type->id(), Type::EXTENSION);

  const auto& ext_type = static_cast<const ExtensionType&>(*type);
  std::string serialized = ext_type.Serialize();

  ASSERT_OK_AND_ASSIGN(auto deserialized,
                       ext_type.Deserialize(fixed_size_binary(16), serialized));
  ASSERT_TRUE(deserialized->Equals(*type));
  ASSERT_FALSE(deserialized->Equals(*fixed_size_binary(16)));
}

TEST(TestUuuidExtensionType, RoundtripBatch) {
  auto ext_type = extension::uuid();
  auto exact_ext_type = internal::checked_pointer_cast<extension::UuidType>(ext_type);
  auto arr = ArrayFromJSON(fixed_size_binary(16), R"(["abcdefghijklmnop", null])");
  auto ext_arr = ExtensionType::WrapArray(ext_type, arr);

  // Pass extension array, expect getting back extension array
  std::shared_ptr<RecordBatch> read_batch;
  auto ext_field = field(/*name=*/"f0", /*type=*/ext_type);
  auto batch = RecordBatch::Make(schema({ext_field}), ext_arr->length(), {ext_arr});
  RoundtripBatch(batch, &read_batch);
  CompareBatch(*batch, *read_batch, /*compare_metadata=*/true);

  // Pass extension metadata and storage array, expect getting back extension array
  std::shared_ptr<RecordBatch> read_batch2;
  auto ext_metadata =
      key_value_metadata({{"ARROW:extension:name", exact_ext_type->extension_name()},
                          {"ARROW:extension:metadata", ""}});
  ext_field = field(/*name=*/"f0", /*type=*/exact_ext_type->storage_type(),
                    /*nullable=*/true, /*metadata=*/ext_metadata);
  auto batch2 = RecordBatch::Make(schema({ext_field}), arr->length(), {arr});
  RoundtripBatch(batch2, &read_batch2);
  CompareBatch(*batch, *read_batch2, /*compare_metadata=*/true);
}

}  // namespace arrow
