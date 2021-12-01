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

#include "arrow/engine/substrait/serde.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/util/type_resolver_util.h>
#include <gtest/gtest.h>

#include "arrow/compute/exec/expression_internal.h"
#include "arrow/engine/substrait/extension_types.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"

namespace arrow {
namespace engine {

const std::shared_ptr<Schema> kBoringSchema = schema({
    field("bool", boolean()),
    field("i8", int8()),
    field("i32", int32()),
    field("i32_req", int32(), /*nullable=*/false),
    field("u32", uint32()),
    field("i64", int64()),
    field("f32", float32()),
    field("f32_req", float32(), /*nullable=*/false),
    field("f64", float64()),
    field("date64", date64()),
    field("str", utf8()),
    field("list_i32", list(int32())),
    field("struct_i32_str", struct_({
                                field("i32", int32()),
                                field("str", utf8()),
                            })),
    field("dict_str", dictionary(int32(), utf8())),
    field("dict_i32", dictionary(int32(), int32())),
    field("ts_ns", timestamp(TimeUnit::NANO)),
});

// map to an index-only field reference
inline FieldRef BoringRef(FieldRef ref) {
  auto path = *ref.FindOne(*kBoringSchema);
  return {std::move(path)};
}

inline compute::Expression UseBoringRefs(const compute::Expression& expr) {
  if (expr.literal()) return expr;

  if (auto ref = expr.field_ref()) {
    return compute::field_ref(*ref->FindOne(*kBoringSchema));
  }

  auto modified_call = *CallNotNull(expr);
  for (auto& arg : modified_call.arguments) {
    arg = UseBoringRefs(arg);
  }
  return compute::Expression{std::move(modified_call)};
}

google::protobuf::util::TypeResolver* GetGeneratedTypeResolver() {
  static std::unique_ptr<google::protobuf::util::TypeResolver> type_resolver;
  if (!type_resolver) {
    type_resolver.reset(google::protobuf::util::NewTypeResolverForDescriptorPool(
        /*url_prefix=*/"", google::protobuf::DescriptorPool::generated_pool()));
  }
  return type_resolver.get();
}

std::shared_ptr<Buffer> SubstraitFromJSON(util::string_view json,
                                          util::string_view type_name) {
  std::string type_url = "/io.substrait." + type_name.to_string();

  google::protobuf::io::ArrayInputStream json_stream{json.data(),
                                                     static_cast<int>(json.size())};

  std::string out;
  google::protobuf::io::StringOutputStream out_stream{&out};

  auto status = google::protobuf::util::JsonToBinaryStream(
      GetGeneratedTypeResolver(), type_url, &json_stream, &out_stream);
  DCHECK(status.ok()) << "JsonToBinaryStream returned " << status;

  return Buffer::FromString(std::move(out));
}

TEST(Substrait, BasicTypeFromJSON) {
  auto ExpectEq = [](util::string_view json, std::shared_ptr<DataType> expected_type) {
    ARROW_SCOPED_TRACE(expected_type->ToString());

    auto buf = SubstraitFromJSON(json, "Type");
    ASSERT_OK_AND_ASSIGN(auto actual, DeserializeType(*buf));

    ASSERT_EQ(*actual, *expected_type);
  };

  ExpectEq(R"({"bool": {}})", boolean());

  ExpectEq(R"({"i8": {}})", int8());
  ExpectEq(R"({"i16": {}})", int16());
  ExpectEq(R"({"i32": {}})", int32());
  ExpectEq(R"({"i64": {}})", int64());

  ExpectEq(R"({"fp32": {}})", float32());
  ExpectEq(R"({"fp64": {}})", float64());

  ExpectEq(R"({"string": {}})", utf8());
  ExpectEq(R"({"binary": {}})", binary());

  ExpectEq(R"({"timestamp": {}})", timestamp(TimeUnit::MICRO));
  ExpectEq(R"({"date": {}})", date32());
  ExpectEq(R"({"time": {}})", time64(TimeUnit::MICRO));
  ExpectEq(R"({"timestamp_tz": {}})", timestamp(TimeUnit::MICRO, "UTC"));

  ExpectEq(R"({"uuid": {}})", uuid());

  ExpectEq(R"({"fixed_char": {"length": 32}})", fixed_char(32));
  ExpectEq(R"({"varchar": {"length": 1024}})", varchar(1024));
  ExpectEq(R"({"fixed_binary": {"length": 32}})", fixed_size_binary(32));

  ExpectEq(R"({"decimal": {"precision": 27, "scale": 5}})", decimal128(27, 5));

  ExpectEq(R"({"struct": {
    "types": [
      {"i64": {}},
      {"list": {"type": {"string":{}} }}
    ]
  }})",
           struct_({
               field("", int64()),
               field("", list(utf8())),
           }));
}

TEST(Substrait, BasicTypeRoundTrip) {
  for (auto type : {
           boolean(),

           int8(),
           int16(),
           int32(),
           int64(),

           float32(),
           float64(),

           date32(),
           timestamp(TimeUnit::MICRO),
           timestamp(TimeUnit::MICRO, "UTC"),
           time64(TimeUnit::MICRO),

           decimal128(27, 5),

           struct_({
               field("", int64()),
               field("", list(utf8())),
           }),

           uuid(),
           fixed_char(32),
           varchar(1024),
       }) {
    ARROW_SCOPED_TRACE(type->ToString());
    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeType(*type));
    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeType(*serialized));
    EXPECT_EQ(*roundtripped, *type);
  }
}

TEST(Substrait, UnsupportedTypes) {
  for (auto type : {
           uint8(),
           uint16(),
           uint32(),
           uint64(),

           float16(),

           date64(),
           timestamp(TimeUnit::SECOND),
           timestamp(TimeUnit::NANO),
           timestamp(TimeUnit::MICRO, "New York"),
           time32(TimeUnit::SECOND),
           time32(TimeUnit::MILLI),
           time64(TimeUnit::NANO),
           month_interval(),
           day_time_interval(),

           decimal256(76, 67),

           sparse_union({field("i8", int8()), field("f32", float32())}),
           dense_union({field("i8", int8()), field("f32", float32())}),
           dictionary(int32(), utf8()),

           fixed_size_list(float16(), 3),

           duration(TimeUnit::MICRO),

           large_utf8(),
           large_binary(),
           large_list(utf8()),

           month_day_nano_interval(),
       }) {
    ARROW_SCOPED_TRACE(type->ToString());
    EXPECT_THAT(SerializeType(*type), Raises(StatusCode::NotImplemented));
  }
}

TEST(Substrait, BasicLiteralRoundTrip) {
  for (Datum datum : {
           Datum(true),

           Datum(int8_t(34)),
           Datum(int16_t(34)),
           Datum(int32_t(34)),
           Datum(int64_t(34)),

           Datum(3.5F),
           Datum(7.125),
       }) {
    ARROW_SCOPED_TRACE(datum.scalar()->ToString());
    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(compute::literal(datum)));
    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeExpression(*serialized));
    ASSERT_TRUE(roundtripped.literal());
    EXPECT_THAT(*roundtripped.literal(), DataEq(datum));
  }
}

TEST(Substrait, FieldRefRoundTrip) {
  for (FieldRef ref : {
           // by name
           FieldRef("i32"),
           FieldRef("ts_ns"),
           FieldRef("struct_i32_str"),

           // by index
           FieldRef(0),
           FieldRef(1),
           FieldRef(kBoringSchema->num_fields() - 1),
           FieldRef(kBoringSchema->GetFieldIndex("struct_i32_str")),

           // nested
           FieldRef("struct_i32_str", "i32"),
           FieldRef(kBoringSchema->GetFieldIndex("struct_i32_str"), 1),
       }) {
    ARROW_SCOPED_TRACE(ref.ToString());
    ASSERT_OK_AND_ASSIGN(auto expr, compute::field_ref(ref).Bind(*kBoringSchema));
    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(expr));
    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeExpression(*serialized));
    ASSERT_TRUE(roundtripped.field_ref());

    ASSERT_OK_AND_ASSIGN(auto expected, ref.FindOne(*kBoringSchema));
    ASSERT_OK_AND_ASSIGN(auto actual, roundtripped.field_ref()->FindOne(*kBoringSchema));
    EXPECT_EQ(actual.indices(), expected.indices());
  }
}

TEST(Substrait, CallSpecialCaseRoundTrip) {
  for (compute::Expression expr : {
           compute::call("if_else",
                         {
                             compute::literal(true),
                             compute::field_ref({"struct_i32_str", 1}),
                             compute::field_ref("str"),
                         }),
           compute::call("list_element",
                         {
                             compute::field_ref("list_i32"),
                             compute::literal(3),
                         }),
       }) {
    ARROW_SCOPED_TRACE(expr.ToString());
    ASSERT_OK_AND_ASSIGN(expr, expr.Bind(*kBoringSchema));
    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(expr));
    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeExpression(*serialized));
    ASSERT_OK_AND_ASSIGN(roundtripped, roundtripped.Bind(*kBoringSchema));
    EXPECT_EQ(UseBoringRefs(roundtripped), UseBoringRefs(expr));
  }
}

}  // namespace engine
}  // namespace arrow

