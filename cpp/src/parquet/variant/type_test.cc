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
#include "arrow/extension/uuid.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/checked_cast.h"

#include <array>
#include <memory>

namespace parquet::variant {

using ::arrow::binary;
using ::arrow::binary_view;
using ::arrow::decimal128;
using ::arrow::decimal256;
using ::arrow::decimal32;
using ::arrow::decimal64;
using ::arrow::dictionary;
using ::arrow::duration;
using ::arrow::ExtensionType;
using ::arrow::field;
using ::arrow::fixed_size_binary;
using ::arrow::fixed_size_list;
using ::arrow::int32;
using ::arrow::int64;
using ::arrow::large_list_view;
using ::arrow::list;
using ::arrow::list_view;
using ::arrow::struct_;
using ::arrow::timestamp;
using ::arrow::TimeUnit;
using ::arrow::uint64;
using ::arrow::utf8;
using ::arrow::utf8_view;
using ::arrow::extension::kVariantExtensionName;
using ::arrow::extension::VariantExtensionType;

TEST(TestVariantType, Storage) {
  {
    auto unshredded = struct_({field("metadata", binary(), /*nullable=*/false),
                               field("value", binary(), /*nullable=*/false)});
    ASSERT_OK_AND_ASSIGN(auto type, VariantExtensionType::Make(unshredded));
    ASSERT_EQ(
        kVariantExtensionName,
        ::arrow::internal::checked_cast<const ExtensionType&>(*type).extension_name());
  }

  {
    auto shredded = struct_({field("metadata", binary(), /*nullable=*/false),
                             field("value", binary()), field("typed_value", int64())});
    ASSERT_OK_AND_ASSIGN(auto shredded_type, VariantExtensionType::Make(shredded));
    auto variant_type =
        ::arrow::internal::checked_pointer_cast<VariantExtensionType>(shredded_type);
    ASSERT_EQ("metadata", variant_type->metadata()->name());
    ASSERT_EQ("value", variant_type->value()->name());
    ASSERT_EQ("typed_value", variant_type->typed_value()->name());
  }

  {
    auto typed_only = struct_(
        {field("metadata", binary(), /*nullable=*/false), field("typed_value", int64())});
    ASSERT_RAISES(Invalid, VariantExtensionType::Make(typed_only));
  }

  {
    auto flipped =
        std::dynamic_pointer_cast<VariantExtensionType>(::arrow::extension::variant(
            struct_({field("value", binary(), /*nullable=*/false),
                     field("metadata", binary(), /*nullable=*/false)})));
    ASSERT_EQ("metadata", flipped->metadata()->name());
    ASSERT_EQ("value", flipped->value()->name());
  }

  ASSERT_OK(VariantExtensionType::Make(
      struct_({field("metadata", binary_view(), /*nullable=*/false),
               field("value", binary_view(), /*nullable=*/false)})));

  auto shredded_field_group =
      struct_({field("value", binary()), field("typed_value", int64())});
  auto shredded_object =
      struct_({field("metadata", binary(), /*nullable=*/false), field("value", binary()),
               field("typed_value",
                     struct_({field("a", shredded_field_group, /*nullable=*/false)}))});
  auto shredded_list =
      struct_({field("metadata", binary(), /*nullable=*/false), field("value", binary()),
               field("typed_value",
                     list(field("element", shredded_field_group, /*nullable=*/false)))});
  ASSERT_OK(VariantExtensionType::Make(shredded_object));
  ASSERT_OK(VariantExtensionType::Make(shredded_list));

  for (const auto& typed_value_type :
       {binary_view(), utf8_view(), ::arrow::extension::uuid(),
        list_view(field("element", shredded_field_group, /*nullable=*/false)),
        large_list_view(field("element", shredded_field_group, /*nullable=*/false)),
        fixed_size_list(field("element", shredded_field_group, /*nullable=*/false),
                        /*list_size=*/2)}) {
    auto valid_shredded_type =
        struct_({field("metadata", binary(), /*nullable=*/false),
                 field("value", binary()), field("typed_value", typed_value_type)});
    ASSERT_OK(VariantExtensionType::Make(valid_shredded_type));
  }
}

TEST(TestVariantType, InvalidStorage) {
  auto missing_value = struct_({field("metadata", binary(), /*nullable=*/false)});
  auto missing_metadata = struct_({field("value", binary(), /*nullable=*/false)});
  auto nullable_metadata =
      struct_({field("metadata", binary()), field("value", binary(), false)});
  auto nullable_unshredded =
      struct_({field("metadata", binary(), false), field("value", binary())});
  auto bad_value_type =
      struct_({field("metadata", binary(), false), field("value", int32(), false)});
  auto extra = struct_({field("metadata", binary(), false),
                        field("value", binary(), false), field("extra", binary())});
  auto dictionary_typed_value =
      struct_({field("metadata", binary(), false), field("value", binary()),
               field("typed_value", dictionary(int32(), utf8()))});
  auto dictionary_value = struct_({field("metadata", binary(), false),
                                   field("value", dictionary(int32(), binary()), false)});
  auto dictionary_metadata =
      struct_({field("metadata", dictionary(int32(), binary()), false),
               field("value", binary(), false)});
  auto required_nested_value =
      struct_({field("metadata", binary(), false), field("value", binary()),
               field("typed_value",
                     struct_({field("a", struct_({field("value", binary(), false)}),
                                    /*nullable=*/false)}))});

  ASSERT_RAISES(Invalid, VariantExtensionType::Make(missing_value));
  ASSERT_RAISES(Invalid, VariantExtensionType::Make(missing_metadata));
  ASSERT_RAISES(Invalid, VariantExtensionType::Make(nullable_metadata));
  ASSERT_RAISES(Invalid, VariantExtensionType::Make(nullable_unshredded));
  ASSERT_RAISES(Invalid, VariantExtensionType::Make(bad_value_type));
  ASSERT_RAISES(Invalid, VariantExtensionType::Make(extra));
  ASSERT_RAISES(Invalid, VariantExtensionType::Make(dictionary_typed_value));
  ASSERT_RAISES(Invalid, VariantExtensionType::Make(dictionary_value));
  ASSERT_RAISES(Invalid, VariantExtensionType::Make(dictionary_metadata));
  ASSERT_RAISES(Invalid, VariantExtensionType::Make(required_nested_value));

  std::array invalid_typed_value_types{
      uint64(),
      duration(TimeUnit::MICRO),
      timestamp(TimeUnit::MILLI),
      struct_({}),
      fixed_size_binary(/*byte_width=*/8),
      decimal32(/*precision=*/8, /*scale=*/9),
      decimal64(/*precision=*/16, /*scale=*/17),
      decimal128(/*precision=*/32, /*scale=*/-1),
      decimal256(/*precision=*/39, /*scale=*/0),
  };
  for (const auto& typed_value_type : invalid_typed_value_types) {
    auto invalid_shredded_type =
        struct_({field("metadata", binary(), /*nullable=*/false),
                 field("value", binary()), field("typed_value", typed_value_type)});
    ASSERT_RAISES(Invalid, VariantExtensionType::Make(std::move(invalid_shredded_type)));
  }
}

TEST(TestVariantType, ReadStorage) {
  auto base_storage = struct_({field("metadata", binary(), /*nullable=*/false),
                               field("value", binary(), /*nullable=*/false)});
  ASSERT_OK_AND_ASSIGN(auto base_type, VariantExtensionType::Make(base_storage));
  auto variant_type =
      ::arrow::internal::checked_pointer_cast<VariantExtensionType>(base_type);

  auto typed_only = struct_(
      {field("metadata", binary(), /*nullable=*/false), field("typed_value", int64())});
  ASSERT_OK(variant_type->Deserialize(typed_only, /*serialized_data=*/""));

  auto typed_only_field_group = struct_({field("typed_value", ::arrow::utf8())});
  auto shredded_object =
      struct_({field("metadata", binary(), /*nullable=*/false), field("value", binary()),
               field("typed_value",
                     struct_({field("a", typed_only_field_group, /*nullable=*/false)}))});
  ASSERT_OK(variant_type->Deserialize(shredded_object, /*serialized_data=*/""));
}

}  // namespace parquet::variant
