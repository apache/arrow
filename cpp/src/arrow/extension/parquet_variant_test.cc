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

#include "arrow/extension/parquet_variant.h"

#include <memory>

#include <gtest/gtest.h>

#include "arrow/array/array_nested.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

TEST(TestVariantArray, Accessors) {
  auto metadata = ArrayFromJSON(binary(), R"(["metadata"])");
  auto value = ArrayFromJSON(binary(), R"(["value"])");
  auto typed_value = ArrayFromJSON(int64(), "[1]");
  auto storage_type = struct_({field("typed_value", int64()),
                               field("metadata", binary(), /*nullable=*/false),
                               field("value", binary())});
  ASSERT_OK_AND_ASSIGN(auto storage, StructArray::Make({typed_value, metadata, value},
                                                       storage_type->fields()));
  ASSERT_OK_AND_ASSIGN(auto type, extension::VariantExtensionType::Make(storage_type));
  auto array = std::make_shared<extension::VariantArray>(type, storage);

  ASSERT_EQ(metadata->data(), array->metadata()->data());
  ASSERT_EQ(value->data(), array->value()->data());
  ASSERT_EQ(typed_value->data(), array->typed_value()->data());
  ASSERT_TRUE(array->is_shredded());

  auto unshredded_type = struct_({field("value", binary(), /*nullable=*/false),
                                  field("metadata", binary(), /*nullable=*/false)});
  ASSERT_OK_AND_ASSIGN(auto unshredded_storage,
                       StructArray::Make({value, metadata}, unshredded_type->fields()));
  ASSERT_OK_AND_ASSIGN(auto unshredded_extension_type,
                       extension::VariantExtensionType::Make(unshredded_type));
  auto unshredded = std::make_shared<extension::VariantArray>(unshredded_extension_type,
                                                              unshredded_storage);

  ASSERT_EQ(metadata->data(), unshredded->metadata()->data());
  ASSERT_EQ(value->data(), unshredded->value()->data());
  ASSERT_EQ(nullptr, unshredded->typed_value());
  ASSERT_FALSE(unshredded->is_shredded());
}

TEST(TestVariantArray, CompatibleStorage) {
  auto metadata = ArrayFromJSON(binary(), R"(["metadata"])");
  auto typed_value = ArrayFromJSON(int64(), "[1]");
  auto storage_type = struct_(
      {field("typed_value", int64()), field("metadata", binary(), /*nullable=*/false)});
  ASSERT_OK_AND_ASSIGN(
      auto storage, StructArray::Make({typed_value, metadata}, storage_type->fields()));
  auto prototype_storage_type = struct_({field("metadata", binary(), /*nullable=*/false),
                                         field("value", binary(), /*nullable=*/false)});
  ASSERT_OK_AND_ASSIGN(auto prototype_type,
                       extension::VariantExtensionType::Make(prototype_storage_type));
  const auto& prototype =
      internal::checked_cast<const extension::VariantExtensionType&>(*prototype_type);
  ASSERT_OK_AND_ASSIGN(auto type, prototype.Deserialize(storage_type, ""));
  auto array = std::make_shared<extension::VariantArray>(type, storage);

  ASSERT_EQ(metadata->data(), array->metadata()->data());
  ASSERT_EQ(nullptr, array->value());
  ASSERT_EQ(typed_value->data(), array->typed_value()->data());
  ASSERT_TRUE(array->is_shredded());
}

}  // namespace arrow
