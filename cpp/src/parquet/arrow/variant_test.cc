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

#include "parquet/arrow/variant.h"

#include "arrow/array/validate.h"
#include "arrow/ipc/test_common.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "parquet/exception.h"

namespace parquet::arrow {

using ::arrow::binary;
using ::arrow::struct_;

class TestVariantExtensionType : public ::testing::Test {};

TEST(TestVariantExtensionType, StorageTypeValidation) {
  auto variant1 =
      variant(struct_({field("metadata", binary()), field("value", binary())}));
  auto variant2 =
      variant(struct_({field("metadata", binary()), field("value", binary())}));

  ASSERT_TRUE(variant1->Equals(variant2));

  auto missing_value = struct_({field("metadata", binary())});
  auto missing_metadata = struct_({field("value", binary())});
  auto bad_value_type =
      struct_({field("metadata", binary()), field("value", ::arrow::int32())});
  auto extra_field = struct_(
      {field("metadata", binary()), field("value", binary()), field("extra", binary())});

  for (const auto& storage_type :
       {missing_value, missing_metadata, bad_value_type, extra_field}) {
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid,
        "Invalid: Invalid storage type for VariantExtensionType: " +
            storage_type->ToString(),
        VariantExtensionType::Make(storage_type));
  }
}

}  // namespace parquet::arrow
