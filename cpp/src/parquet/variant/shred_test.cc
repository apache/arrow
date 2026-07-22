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

#include "parquet/variant/shred.h"

#include <cstdint>
#include <functional>
#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "arrow/array.h"  // IWYU pragma: keep
#include "arrow/chunked_array.h"
#include "arrow/extension/parquet_variant.h"
#include "arrow/extension/uuid.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/key_value_metadata.h"
#include "parquet/exception.h"
#include "parquet/variant/array_internal.h"
#include "parquet/variant/builder.h"
#include "parquet/variant/decoding.h"
#include "parquet/variant/test_util_internal.h"
#include "parquet/variant/validate.h"

namespace parquet::variant {

namespace {

using ::arrow::Array;
using ::arrow::BaseListType;
using ::arrow::BinaryViewArray;
using ::arrow::ChunkedArray;
using ::arrow::DataType;
using ::arrow::Field;
using ::arrow::StructArray;
using ::arrow::Type;
using ::arrow::extension::VariantArray;
using ::arrow::internal::checked_cast;
using ::arrow::internal::checked_pointer_cast;
using internal::AssertUnshreddedValue;
using internal::BinaryArrayFromValues;

std::shared_ptr<VariantArray> SingleValue(const EncodedVariantValue& encoded) {
  VariantArrayBuilder builder;
  builder.AppendEncoded(encoded);
  return builder.Finish();
}

template <std::invocable<VariantBuilder&> AppendInput,
          std::invocable<VariantBuilder&> AppendExpected>
void AssertConverted(const std::shared_ptr<DataType>& target, AppendInput append_input,
                     AppendExpected append_expected) {
  VariantBuilder input_builder;
  std::invoke(append_input, input_builder);
  auto input = input_builder.Finish();
  auto shredded = ShredVariantArray(*SingleValue(input), target);

  ASSERT_TRUE(shredded->value()->IsNull(0));
  ASSERT_FALSE(shredded->typed_value()->IsNull(0));
  ASSERT_TRUE(shredded->is_shredded());
  ValidateVariants<true>(ChunkedArray{shredded});

  VariantBuilder expected_builder;
  std::invoke(append_expected, expected_builder);
  AssertUnshreddedValue(*shredded, 0, expected_builder.Finish());
}

void AssertFallback(const EncodedVariantValue& input,
                    const std::shared_ptr<DataType>& target) {
  auto shredded = ShredVariantArray(*SingleValue(input), target);
  const auto& residual = checked_cast<const BinaryViewArray&>(*shredded->value());
  ASSERT_EQ(std::string_view{*input.value}, residual.GetView(0));
  ASSERT_TRUE(shredded->typed_value()->IsNull(0));
  ValidateVariants<true>(ChunkedArray{shredded});
  AssertUnshreddedValue(*shredded, 0, input);
}

template <typename TypeClass>
  requires ::arrow::is_var_length_list_type<TypeClass>::value
void AssertListOffsets(
    const Array& array,
    std::initializer_list<typename TypeClass::offset_type> expected_offsets) {
  using ArrayType = typename ::arrow::TypeTraits<TypeClass>::ArrayType;
  const auto& list = checked_cast<const ArrayType&>(array);
  ASSERT_EQ(2, list.data()->buffers.size());
  ASSERT_EQ(expected_offsets.size(), static_cast<size_t>(list.length() + 1));
  int64_t index = 0;
  for (const auto expected : expected_offsets) {
    ASSERT_EQ(expected, list.raw_value_offsets()[index++]);
  }
}

template <typename TypeClass>
  requires ::arrow::is_list_view_type<TypeClass>::value
void AssertListViewDimensions(
    const Array& array,
    std::initializer_list<typename TypeClass::offset_type> expected_offsets,
    std::initializer_list<typename TypeClass::offset_type> expected_sizes) {
  using ArrayType = typename ::arrow::TypeTraits<TypeClass>::ArrayType;
  const auto& list = checked_cast<const ArrayType&>(array);
  ASSERT_EQ(3, list.data()->buffers.size());
  ASSERT_EQ(expected_offsets.size(), static_cast<size_t>(list.length()));
  ASSERT_EQ(expected_sizes.size(), static_cast<size_t>(list.length()));
  int64_t index = 0;
  for (const auto expected : expected_offsets) {
    ASSERT_EQ(expected, list.raw_value_offsets()[index++]);
  }
  index = 0;
  for (const auto expected : expected_sizes) {
    ASSERT_EQ(expected, list.raw_value_sizes()[index++]);
  }
}

std::vector<std::shared_ptr<DataType>> ListTargets(
    const std::shared_ptr<Field>& element) {
  return {::arrow::list(element), ::arrow::large_list(element),
          ::arrow::list_view(element), ::arrow::large_list_view(element),
          ::arrow::fixed_size_list(element, 3)};
}

}  // namespace

TEST(TestVariantShred, TargetTypes) {
  VariantArrayBuilder input_builder;
  input_builder.AppendNull();
  auto input = input_builder.Finish();

  std::vector<std::shared_ptr<DataType>> targets = {
      ::arrow::boolean(),
      ::arrow::int8(),
      ::arrow::int16(),
      ::arrow::int32(),
      ::arrow::int64(),
      ::arrow::float32(),
      ::arrow::float64(),
      ::arrow::decimal32(9, 2),
      ::arrow::decimal64(18, 2),
      ::arrow::decimal128(38, 2),
      ::arrow::date32(),
      ::arrow::time64(::arrow::TimeUnit::MICRO),
      ::arrow::timestamp(::arrow::TimeUnit::MICRO),
      ::arrow::timestamp(::arrow::TimeUnit::NANO, "UTC"),
      ::arrow::binary(),
      ::arrow::large_binary(),
      ::arrow::binary_view(),
      ::arrow::utf8(),
      ::arrow::large_utf8(),
      ::arrow::utf8_view(),
      ::arrow::fixed_size_binary(16),
      ::arrow::extension::uuid(),
      ::arrow::struct_({::arrow::field("a", ::arrow::int64())}),
  };
  auto element = ::arrow::field("item", ::arrow::int64());
  auto lists = ListTargets(element);
  targets.insert(targets.end(), lists.begin(), lists.end());

  for (const auto& target : targets) {
    auto shredded = ShredVariantArray(*input, target);
    ASSERT_EQ(1, shredded->length()) << target->ToString();
    ASSERT_TRUE(shredded->IsNull(0)) << target->ToString();
    ASSERT_TRUE(shredded->is_shredded()) << target->ToString();
    ASSERT_TRUE(shredded->metadata()->type()->Equals(input->metadata()->type()));
    ValidateVariants<true>(ChunkedArray{shredded});
  }
}

TEST(TestVariantShred, InvalidTargets) {
  VariantArrayBuilder builder;
  auto input = builder.Finish();
  std::vector<std::shared_ptr<DataType>> targets = {
      nullptr,
      ::arrow::null(),
      ::arrow::uint8(),
      ::arrow::uint64(),
      ::arrow::decimal32(4, -1),
      ::arrow::decimal32(4, 5),
      ::arrow::time32(::arrow::TimeUnit::MILLI),
      ::arrow::time64(::arrow::TimeUnit::NANO),
      ::arrow::timestamp(::arrow::TimeUnit::MILLI),
      ::arrow::fixed_size_binary(15),
      ::arrow::struct_({}),
      ::arrow::struct_(
          {::arrow::field("a", ::arrow::int64()), ::arrow::field("a", ::arrow::utf8())}),
      ::arrow::fixed_size_list(::arrow::int64(), -1),
      ::arrow::map(::arrow::utf8(), ::arrow::int64()),
      ::arrow::duration(::arrow::TimeUnit::MICRO),
      input->type(),
  };

  for (const auto& target : targets) {
    ASSERT_THROW(ShredVariantArray(*input, target), ParquetException)
        << (target == nullptr ? "null" : target->ToString());
  }
}

TEST(TestVariantShred, AlreadyShredded) {
  VariantArrayBuilder builder;
  builder.AppendInt8(1);
  auto shredded = ShredVariantArray(*builder.Finish(), ::arrow::int64());
  ASSERT_THROW(ShredVariantArray(*shredded, ::arrow::int64()), ParquetException);
}

TEST(TestVariantShred, Primitive) {
  AssertConverted(
      ::arrow::boolean(), [](VariantBuilder& builder) { builder.AppendBoolean(true); },
      [](VariantBuilder& builder) { builder.AppendBoolean(true); });
  AssertConverted(
      ::arrow::int32(), [](VariantBuilder& builder) { builder.AppendInt32(7); },
      [](VariantBuilder& builder) { builder.AppendInt32(7); });
  AssertConverted(
      ::arrow::float32(), [](VariantBuilder& builder) { builder.AppendFloat(9); },
      [](VariantBuilder& builder) { builder.AppendFloat(9); });
  AssertConverted(
      ::arrow::boolean(), [](VariantBuilder& builder) { builder.AppendInt16(1); },
      [](VariantBuilder& builder) { builder.AppendBoolean(true); });
  AssertConverted(
      ::arrow::int8(), [](VariantBuilder& builder) { builder.AppendBoolean(true); },
      [](VariantBuilder& builder) { builder.AppendInt8(1); });
  AssertConverted(
      ::arrow::int16(), [](VariantBuilder& builder) { builder.AppendInt8(7); },
      [](VariantBuilder& builder) { builder.AppendInt16(7); });
  AssertConverted(
      ::arrow::int32(), [](VariantBuilder& builder) { builder.AppendInt16(7); },
      [](VariantBuilder& builder) { builder.AppendInt32(7); });
  AssertConverted(
      ::arrow::int64(), [](VariantBuilder& builder) { builder.AppendInt8(7); },
      [](VariantBuilder& builder) { builder.AppendInt64(7); });
  AssertConverted(
      ::arrow::float32(), [](VariantBuilder& builder) { builder.AppendInt16(9); },
      [](VariantBuilder& builder) { builder.AppendFloat(9); });
  AssertConverted(
      ::arrow::float64(), [](VariantBuilder& builder) { builder.AppendInt16(9); },
      [](VariantBuilder& builder) { builder.AppendDouble(9); });
  AssertConverted(
      ::arrow::boolean(), [](VariantBuilder& builder) { builder.AppendString("true"); },
      [](VariantBuilder& builder) { builder.AppendBoolean(true); });
  AssertConverted(
      ::arrow::utf8_view(),
      [](VariantBuilder& builder) { builder.AppendShortString("hello"); },
      [](VariantBuilder& builder) { builder.AppendShortString("hello"); });
  const std::string long_string(80, 'x');
  AssertConverted(
      ::arrow::utf8(),
      [&long_string](VariantBuilder& builder) { builder.AppendString(long_string); },
      [&long_string](VariantBuilder& builder) { builder.AppendString(long_string); });
  AssertConverted(
      ::arrow::large_utf8(),
      [&long_string](VariantBuilder& builder) { builder.AppendString(long_string); },
      [&long_string](VariantBuilder& builder) { builder.AppendString(long_string); });
  AssertConverted(
      ::arrow::binary(), [](VariantBuilder& builder) { builder.AppendBinary("abc"); },
      [](VariantBuilder& builder) { builder.AppendBinary("abc"); });
  AssertConverted(
      ::arrow::binary_view(),
      [](VariantBuilder& builder) { builder.AppendBinary("abc"); },
      [](VariantBuilder& builder) { builder.AppendBinary("abc"); });
  AssertConverted(
      ::arrow::large_binary(),
      [](VariantBuilder& builder) { builder.AppendBinary("abc"); },
      [](VariantBuilder& builder) { builder.AppendBinary("abc"); });
  AssertConverted(
      ::arrow::date32(), [](VariantBuilder& builder) { builder.AppendDate(123); },
      [](VariantBuilder& builder) { builder.AppendDate(123); });
  AssertConverted(
      ::arrow::time64(::arrow::TimeUnit::MICRO),
      [](VariantBuilder& builder) { builder.AppendTimeNTZMicros(123); },
      [](VariantBuilder& builder) { builder.AppendTimeNTZMicros(123); });
}

TEST(TestVariantShred, MixedPrimitive) {
  VariantArrayBuilder builder;
  builder.AppendInt8(1);
  builder.AppendInt16(2);
  builder.AppendInt8(3);
  builder.AppendDouble(4);
  auto shredded = ShredVariantArray(*builder.Finish(), ::arrow::int64());
  for (int64_t row = 0; row < shredded->length(); ++row) {
    VariantBuilder expected;
    expected.AppendInt64(row + 1);
    AssertUnshreddedValue(*shredded, row, expected.Finish());
  }
  ValidateVariants<true>(ChunkedArray{shredded});
}

TEST(TestVariantShred, PrimitiveFallback) {
  VariantBuilder overflow;
  overflow.AppendInt64(128);
  AssertFallback(overflow.Finish(), ::arrow::int8());

  VariantBuilder fractional;
  fractional.AppendDouble(1.5);
  AssertFallback(fractional.Finish(), ::arrow::int64());

  VariantBuilder nan;
  nan.AppendDouble(std::numeric_limits<double>::quiet_NaN());
  AssertFallback(nan.Finish(), ::arrow::int64());

  VariantBuilder infinity;
  infinity.AppendDouble(std::numeric_limits<double>::infinity());
  AssertFallback(infinity.Finish(), ::arrow::int64());

  VariantBuilder malformed;
  malformed.AppendString("not-a-decimal");
  AssertFallback(malformed.Finish(), ::arrow::decimal64(18, 2));

  VariantBuilder wrong_family;
  wrong_family.AppendString("42");
  AssertFallback(wrong_family.Finish(), ::arrow::int64());

  VariantBuilder null_value;
  null_value.AppendVariantNull();
  AssertFallback(null_value.Finish(), ::arrow::int64());

  VariantBuilder decimal_boolean;
  decimal_boolean.AppendDecimal4(::arrow::Decimal32(1), /*scale=*/0);
  AssertFallback(decimal_boolean.Finish(), ::arrow::boolean());

  VariantBuilder boolean_decimal;
  boolean_decimal.AppendBoolean(true);
  AssertFallback(boolean_decimal.Finish(), ::arrow::decimal32(9, 0));
}

TEST(TestVariantShred, Decimal) {
  AssertConverted(
      ::arrow::decimal32(9, 2),
      [](VariantBuilder& builder) {
        builder.AppendDecimal4(::arrow::Decimal32(1234), /*scale=*/2);
      },
      [](VariantBuilder& builder) {
        builder.AppendDecimal4(::arrow::Decimal32(1234), /*scale=*/2);
      });
  AssertConverted(
      ::arrow::decimal32(9, 2), [](VariantBuilder& builder) { builder.AppendInt16(12); },
      [](VariantBuilder& builder) {
        builder.AppendDecimal4(::arrow::Decimal32(1200), /*scale=*/2);
      });
  AssertConverted(
      ::arrow::decimal64(18, 3),
      [](VariantBuilder& builder) {
        builder.AppendDecimal4(::arrow::Decimal32(1234), /*scale=*/2);
      },
      [](VariantBuilder& builder) {
        builder.AppendDecimal8(::arrow::Decimal64(12340), /*scale=*/3);
      });
  AssertConverted(
      ::arrow::decimal128(38, 2),
      [](VariantBuilder& builder) { builder.AppendString("12.34"); },
      [](VariantBuilder& builder) {
        builder.AppendDecimal16(::arrow::Decimal128(1234), /*scale=*/2);
      });
}

TEST(TestVariantShred, DecimalScales) {
  VariantArrayBuilder builder;
  builder.AppendDecimal4(::arrow::Decimal32(12), /*scale=*/1);
  builder.AppendDecimal4(::arrow::Decimal32(123), /*scale=*/2);
  builder.AppendDecimal4(::arrow::Decimal32(34), /*scale=*/1);
  auto shredded = ShredVariantArray(*builder.Finish(), ::arrow::decimal64(18, 3));
  constexpr std::array<int64_t, 3> expected_values = {1200, 1230, 3400};
  for (int64_t row = 0; row < shredded->length(); ++row) {
    VariantBuilder expected;
    expected.AppendDecimal8(::arrow::Decimal64(expected_values[row]), /*scale=*/3);
    AssertUnshreddedValue(*shredded, row, expected.Finish());
  }
  ValidateVariants<true>(ChunkedArray{shredded});
}

TEST(TestVariantShred, DecimalFallback) {
  VariantBuilder precision_overflow;
  precision_overflow.AppendDecimal8(::arrow::Decimal64(1234), /*scale=*/0);
  AssertFallback(precision_overflow.Finish(), ::arrow::decimal32(3, 0));
}

TEST(TestVariantShred, Temporal) {
  AssertConverted(
      ::arrow::timestamp(::arrow::TimeUnit::MICRO),
      [](VariantBuilder& builder) { builder.AppendTimestampMicros(12, false); },
      [](VariantBuilder& builder) { builder.AppendTimestampMicros(12, false); });
  AssertConverted(
      ::arrow::timestamp(::arrow::TimeUnit::NANO, "UTC"),
      [](VariantBuilder& builder) { builder.AppendTimestampMicros(12, true); },
      [](VariantBuilder& builder) { builder.AppendTimestampNanos(12000, true); });

  VariantBuilder wrong_timezone;
  wrong_timezone.AppendTimestampMicros(12, false);
  AssertFallback(wrong_timezone.Finish(),
                 ::arrow::timestamp(::arrow::TimeUnit::MICRO, "UTC"));

  VariantBuilder nanos_to_micros;
  nanos_to_micros.AppendTimestampNanos(12000, true);
  AssertFallback(nanos_to_micros.Finish(),
                 ::arrow::timestamp(::arrow::TimeUnit::MICRO, "UTC"));

  VariantBuilder overflow;
  overflow.AppendTimestampMicros(std::numeric_limits<int64_t>::max() / 1000 + 1, true);
  AssertFallback(overflow.Finish(), ::arrow::timestamp(::arrow::TimeUnit::NANO, "UTC"));
}

TEST(TestVariantShred, Uuid) {
  constexpr std::string_view uuid = "0123456789abcdef";
  AssertConverted(
      ::arrow::fixed_size_binary(16),
      [uuid](VariantBuilder& builder) { builder.AppendUuid(uuid); },
      [uuid](VariantBuilder& builder) { builder.AppendUuid(uuid); });
  AssertConverted(
      ::arrow::extension::uuid(),
      [uuid](VariantBuilder& builder) { builder.AppendUuid(uuid); },
      [uuid](VariantBuilder& builder) { builder.AppendUuid(uuid); });
}

TEST(TestVariantShred, Object) {
  VariantArrayBuilder builder;
  auto object = builder.StartObject();
  object.AppendInt8("a", 1);
  object.AppendString("b", "x");
  object.Finish();
  auto input = builder.Finish();

  auto target = ::arrow::struct_(
      {::arrow::field("a", ::arrow::int64(), /*nullable=*/true,
                      ::arrow::key_value_metadata({{"ignored", "metadata"}}))});
  auto shredded = ShredVariantArray(*input, target);
  const auto& typed = checked_cast<const StructArray&>(*shredded->typed_value());
  ASSERT_TRUE(typed.type()->field(0)->type()->Equals(
      ::arrow::struct_({::arrow::field("value", ::arrow::binary_view()),
                        ::arrow::field("typed_value", ::arrow::int64())})));
  ASSERT_FALSE(typed.type()->field(0)->nullable());
  ASSERT_EQ(nullptr, typed.type()->field(0)->metadata());
  const auto& field_group = checked_cast<const StructArray&>(*typed.field(0));
  ASSERT_TRUE(field_group.field(0)->IsNull(0));
  ASSERT_FALSE(field_group.field(1)->IsNull(0));

  VariantBuilder expected_builder;
  auto expected_object = expected_builder.StartObject();
  expected_object.AppendInt64("a", 1);
  expected_object.AppendString("b", "x");
  expected_object.Finish();
  ValidateVariants<true>(ChunkedArray{shredded});
  AssertUnshreddedValue(*shredded, 0, expected_builder.Finish());
}

TEST(TestVariantShred, ObjectResidual) {
  VariantBuilder input_builder;
  auto input_object = input_builder.StartObject();
  input_object.AppendString("a", "bad");
  input_object.AppendInt8("extra", 2);
  input_object.Finish();
  auto input = input_builder.Finish();
  auto target = ::arrow::struct_({::arrow::field("a", ::arrow::int64())});
  auto shredded = ShredVariantArray(*SingleValue(input), target);

  const auto& typed = checked_cast<const StructArray&>(*shredded->typed_value());
  const auto& field_group = checked_cast<const StructArray&>(*typed.field(0));
  ASSERT_FALSE(field_group.field(0)->IsNull(0));
  ASSERT_TRUE(field_group.field(1)->IsNull(0));
  ASSERT_FALSE(shredded->value()->IsNull(0));
  ValidateVariants<true>(ChunkedArray{shredded});
  AssertUnshreddedValue(*shredded, 0, input);

  VariantBuilder missing_builder;
  missing_builder.AddFieldName("a");
  auto missing_object = missing_builder.StartObject();
  missing_object.AppendInt8("extra", 2);
  missing_object.Finish();
  auto missing = ShredVariantArray(*SingleValue(missing_builder.Finish()), target);
  const auto& missing_typed = checked_cast<const StructArray&>(*missing->typed_value());
  const auto& missing_group = checked_cast<const StructArray&>(*missing_typed.field(0));
  ASSERT_FALSE(missing_group.IsNull(0));
  ASSERT_TRUE(missing_group.field(0)->IsNull(0));
  ASSERT_TRUE(missing_group.field(1)->IsNull(0));

  VariantArrayBuilder null_builder;
  auto null_object = null_builder.StartObject();
  null_object.AppendVariantNull("a");
  null_object.Finish();
  auto explicit_null = ShredVariantArray(*null_builder.Finish(), target);
  const auto& null_typed =
      checked_cast<const StructArray&>(*explicit_null->typed_value());
  const auto& null_group = checked_cast<const StructArray&>(*null_typed.field(0));
  ASSERT_FALSE(null_group.field(0)->IsNull(0));
  ASSERT_TRUE(null_group.field(1)->IsNull(0));
  VariantBuilder null_value_builder;
  null_value_builder.AppendVariantNull();
  ASSERT_EQ(std::string_view{*null_value_builder.Finish().value},
            checked_cast<const BinaryViewArray&>(*null_group.field(0)).GetView(0));

  VariantBuilder non_object_builder;
  non_object_builder.AddFieldName("a");
  non_object_builder.AppendInt8(1);
  AssertFallback(non_object_builder.Finish(), target);
}

TEST(TestVariantShred, MetadataNames) {
  VariantArrayBuilder builder;
  builder.AppendInt8(1);
  auto target = ::arrow::struct_({::arrow::field("missing", ::arrow::int64())});
  ASSERT_THROW(ShredVariantArray(*builder.Finish(), target),
               ParquetInvalidOrCorruptedFileException);

  VariantBuilder nested_builder;
  nested_builder.AddFieldName("a");
  auto object = nested_builder.StartObject();
  object.Finish();
  auto nested_target = ::arrow::struct_(
      {::arrow::field("a", ::arrow::struct_({::arrow::field("b", ::arrow::int64())}))});
  ASSERT_THROW(ShredVariantArray(*SingleValue(nested_builder.Finish()), nested_target),
               ParquetInvalidOrCorruptedFileException);
}

TEST(TestVariantShred, ListKinds) {
  VariantArrayBuilder builder;
  auto list = builder.StartList();
  list.AppendInt8(1);
  list.AppendString("bad");
  list.AppendVariantNull();
  list.Finish();
  builder.AppendInt8(7);
  builder.AppendNull();
  auto input = builder.Finish();

  auto element_metadata = ::arrow::key_value_metadata({{"key", "value"}});
  auto element =
      ::arrow::field("element", ::arrow::int64(), /*nullable=*/true, element_metadata);
  for (const auto& target : ListTargets(element)) {
    auto shredded = ShredVariantArray(*input, target);
    const auto& typed = *shredded->typed_value();
    ASSERT_EQ(target->id(), typed.type_id());
    const auto& typed_list_type = checked_cast<const BaseListType&>(*typed.type());
    ASSERT_EQ("element", typed_list_type.value_field()->name());
    ASSERT_FALSE(typed_list_type.value_field()->nullable());
    ASSERT_TRUE(typed_list_type.value_field()->metadata()->Equals(*element_metadata));
    ASSERT_FALSE(typed.IsNull(0));
    ASSERT_TRUE(typed.IsNull(1));
    ASSERT_TRUE(typed.IsNull(2));

    switch (target->id()) {
      case Type::LIST:
        AssertListOffsets<::arrow::ListType>(typed, {0, 3, 3, 3});
        break;
      case Type::LARGE_LIST:
        AssertListOffsets<::arrow::LargeListType>(typed, {0, 3, 3, 3});
        break;
      case Type::LIST_VIEW:
        AssertListViewDimensions<::arrow::ListViewType>(typed, {0, 3, 3}, {3, 0, 0});
        break;
      case Type::LARGE_LIST_VIEW:
        AssertListViewDimensions<::arrow::LargeListViewType>(typed, {0, 3, 3}, {3, 0, 0});
        break;
      case Type::FIXED_SIZE_LIST:
        ASSERT_EQ(1, typed.data()->buffers.size());
        ASSERT_EQ(9, internal::ValuesArray(typed)->length());
        break;
      default:
        FAIL() << "Expected list-like target";
    }

    const auto& values = checked_cast<const StructArray&>(*internal::ValuesArray(typed));
    const auto& residual = checked_cast<const BinaryViewArray&>(*values.field(0));
    ASSERT_TRUE(residual.IsNull(0));
    ASSERT_FALSE(residual.IsNull(1));
    ASSERT_FALSE(residual.IsNull(2));
    ASSERT_FALSE(values.field(1)->IsNull(0));
    ASSERT_TRUE(values.field(1)->IsNull(1));
    ASSERT_TRUE(values.field(1)->IsNull(2));
    VariantBuilder null_value_builder;
    null_value_builder.AppendVariantNull();
    ASSERT_EQ(std::string_view{*null_value_builder.Finish().value}, residual.GetView(2));

    const auto& root_residual = checked_cast<const BinaryViewArray&>(*shredded->value());
    VariantBuilder scalar_builder;
    scalar_builder.AppendInt8(7);
    auto scalar = scalar_builder.Finish();
    ASSERT_EQ(std::string_view{*scalar.value}, root_residual.GetView(1));
    ASSERT_TRUE(shredded->IsNull(2));

    VariantBuilder expected_builder;
    auto expected_list = expected_builder.StartList();
    expected_list.AppendInt64(1);
    expected_list.AppendString("bad");
    expected_list.AppendVariantNull();
    expected_list.Finish();
    ValidateVariants<true>(ChunkedArray{shredded});
    AssertUnshreddedValue(*shredded, 0, expected_builder.Finish());
    AssertUnshreddedValue(*shredded, 1, scalar);
  }
}

TEST(TestVariantShred, ListResidual) {
  VariantBuilder scalar;
  scalar.AppendInt8(1);
  auto scalar_input = scalar.Finish();
  auto target = ::arrow::list(::arrow::int64());
  AssertFallback(scalar_input, target);

  VariantArrayBuilder builder;
  auto list = builder.StartList();
  list.AppendInt8(1);
  list.AppendString("bad");
  list.Finish();
  auto shredded = ShredVariantArray(*builder.Finish(), target);
  const auto& typed = *shredded->typed_value();
  const auto& values = checked_cast<const StructArray&>(*internal::ValuesArray(typed));
  const auto& residual = checked_cast<const BinaryViewArray&>(*values.field(0));

  VariantBuilder bad_element;
  bad_element.AppendString("bad");
  ASSERT_EQ(std::string_view{*bad_element.Finish().value}, residual.GetView(1));
}

TEST(TestVariantShred, Nested) {
  VariantArrayBuilder builder;
  auto root = builder.StartObject();
  auto items = root.StartList("items");
  auto item = items.StartObject();
  item.AppendInt8("x", 3);
  item.Finish();
  items.Finish();
  root.Finish();
  auto input = builder.Finish();

  auto target = ::arrow::struct_({::arrow::field(
      "items",
      ::arrow::list(::arrow::struct_({::arrow::field("x", ::arrow::int64())})))});
  auto shredded = ShredVariantArray(*input, target);

  VariantBuilder expected_builder;
  auto expected_root = expected_builder.StartObject();
  auto expected_items = expected_root.StartList("items");
  auto expected_item = expected_items.StartObject();
  expected_item.AppendInt64("x", 3);
  expected_item.Finish();
  expected_items.Finish();
  expected_root.Finish();
  ValidateVariants<true>(ChunkedArray{shredded});
  AssertUnshreddedValue(*shredded, 0, expected_builder.Finish());
}

TEST(TestVariantShred, FixedSize) {
  VariantArrayBuilder builder;
  auto list = builder.StartList();
  list.AppendInt8(1);
  list.AppendInt8(2);
  list.Finish();
  ASSERT_THROW(
      ShredVariantArray(*builder.Finish(),
                        ::arrow::fixed_size_list(::arrow::field("item", ::arrow::int64()),
                                                 /*list_size=*/3)),
      ParquetException);
}

TEST(TestVariantShred, NullsAndSlices) {
  VariantArrayBuilder builder;
  builder.AppendInt8(0);
  builder.AppendNull();
  builder.AppendVariantNull();
  builder.AppendInt16(2);
  auto full = builder.Finish();
  auto input = checked_pointer_cast<VariantArray>(full->Slice(1, 3));
  auto shredded = ShredVariantArray(*input, ::arrow::int64());

  ASSERT_EQ(3, shredded->length());
  ASSERT_TRUE(shredded->IsNull(0));
  ASSERT_FALSE(shredded->IsNull(1));
  ASSERT_FALSE(shredded->IsNull(2));
  ASSERT_EQ(0, shredded->offset());
  ASSERT_EQ(input->metadata()->data().get(), shredded->metadata()->data().get());

  VariantBuilder null_expected;
  null_expected.AppendVariantNull();
  AssertUnshreddedValue(*shredded, 1, null_expected.Finish());
  VariantBuilder int_expected;
  int_expected.AppendInt64(2);
  AssertUnshreddedValue(*shredded, 2, int_expected.Finish());
  ValidateVariants<true>(ChunkedArray{shredded});
}

TEST(TestVariantShred, PerRowMetadata) {
  VariantArrayBuilder builder;
  auto first = builder.StartObject();
  first.AppendInt8("a", 1);
  first.AppendString("first", "x");
  first.Finish();
  auto second = builder.StartObject();
  second.AppendInt16("a", 2);
  second.AppendBoolean("second", true);
  second.Finish();
  auto input = builder.Finish();
  auto shredded = ShredVariantArray(
      *input, ::arrow::struct_({::arrow::field("a", ::arrow::int64())}));

  ASSERT_EQ(input->metadata()->data().get(), shredded->metadata()->data().get());
  const auto& input_metadata = checked_cast<const BinaryViewArray&>(*input->metadata());
  const auto& output_metadata =
      checked_cast<const BinaryViewArray&>(*shredded->metadata());
  for (int64_t row = 0; row < input->length(); ++row) {
    ASSERT_EQ(input_metadata.GetView(row), output_metadata.GetView(row));
  }

  VariantBuilder first_expected_builder;
  auto first_expected = first_expected_builder.StartObject();
  first_expected.AppendInt64("a", 1);
  first_expected.AppendString("first", "x");
  first_expected.Finish();
  AssertUnshreddedValue(*shredded, 0, first_expected_builder.Finish());
  VariantBuilder second_expected_builder;
  auto second_expected = second_expected_builder.StartObject();
  second_expected.AppendInt64("a", 2);
  second_expected.AppendBoolean("second", true);
  second_expected.Finish();
  AssertUnshreddedValue(*shredded, 1, second_expected_builder.Finish());
  for (int64_t row = 0; row < input->length(); ++row) {
    VariantMetadataView::Make(output_metadata.GetView(row));
  }
  ValidateVariants<true>(ChunkedArray{shredded});
}

TEST(TestVariantShred, Empty) {
  VariantArrayBuilder empty_builder;
  auto empty = ShredVariantArray(
      *empty_builder.Finish(), ::arrow::struct_({::arrow::field("a", ::arrow::int64())}));
  ASSERT_EQ(0, empty->length());
  ASSERT_TRUE(empty->is_shredded());
  ValidateVariants<true>(ChunkedArray{empty});
}

TEST(TestVariantShred, AllNull) {
  VariantArrayBuilder null_builder;
  null_builder.AppendNull();
  null_builder.AppendNull();
  auto input = null_builder.Finish();
  auto shredded = ShredVariantArray(*input, ::arrow::list(::arrow::int64()));
  ASSERT_EQ(2, shredded->length());
  ASSERT_TRUE(shredded->IsNull(0));
  ASSERT_TRUE(shredded->IsNull(1));
  ASSERT_TRUE(shredded->is_shredded());
  ASSERT_EQ(input->metadata()->data().get(), shredded->metadata()->data().get());
  ASSERT_NE(input->storage().get(), shredded->storage().get());
  ValidateVariants<true>(ChunkedArray{shredded});
}

TEST(TestVariantShred, HiddenNullBytes) {
  VariantBuilder valid_builder;
  valid_builder.AppendInt8(1);
  auto valid = valid_builder.Finish();
  auto metadata = BinaryArrayFromValues(
      {std::string_view("\xff", 1), std::string_view{*valid.metadata}});
  auto values = BinaryArrayFromValues(
      {std::string_view("\xff", 1), std::string_view{*valid.value}});
  auto storage_type =
      ::arrow::struct_({::arrow::field("metadata", ::arrow::binary(), /*nullable=*/false),
                        ::arrow::field("value", ::arrow::binary(), /*nullable=*/false)});
  std::shared_ptr<::arrow::Buffer> null_bitmap;
  ::arrow::BitmapFromVector(std::vector<bool>{false, true}, &null_bitmap);
  auto hidden =
      MakeVariantArrayFromChildren(storage_type, {metadata, values}, null_bitmap);

  auto shredded = ShredVariantArray(*hidden, ::arrow::int64());
  ASSERT_TRUE(shredded->IsNull(0));
  ASSERT_FALSE(shredded->IsNull(1));
  ValidateVariants<true>(ChunkedArray{shredded});

  auto visible = MakeVariantArrayFromChildren(storage_type, {metadata, values});
  ASSERT_THROW(ShredVariantArray(*visible, ::arrow::int64()),
               ParquetInvalidOrCorruptedFileException);
}

}  // namespace parquet::variant
