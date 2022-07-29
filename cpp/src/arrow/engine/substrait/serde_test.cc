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
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/scanner.h"
#include "arrow/engine/substrait/extension_types.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/key_value_metadata.h"

using testing::ElementsAre;
using testing::Eq;
using testing::HasSubstr;

namespace arrow {

using internal::checked_cast;

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
    field("struct", struct_({
                        field("i32", int32()),
                        field("str", utf8()),
                        field("struct_i32_str",
                              struct_({field("i32", int32()), field("str", utf8())})),
                    })),
    field("list_struct", list(struct_({
                             field("i32", int32()),
                             field("str", utf8()),
                             field("struct_i32_str", struct_({field("i32", int32()),
                                                              field("str", utf8())})),
                         }))),
    field("dict_str", dictionary(int32(), utf8())),
    field("dict_i32", dictionary(int32(), int32())),
    field("ts_ns", timestamp(TimeUnit::NANO)),
});

std::shared_ptr<DataType> StripFieldNames(std::shared_ptr<DataType> type) {
  if (type->id() == Type::STRUCT) {
    FieldVector fields(type->num_fields());
    for (int i = 0; i < type->num_fields(); ++i) {
      fields[i] = type->field(i)->WithName("");
    }
    return struct_(std::move(fields));
  }

  if (type->id() == Type::LIST) {
    return list(type->field(0)->WithName(""));
  }

  return type;
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

TEST(Substrait, SupportedTypes) {
  auto ExpectEq = [](util::string_view json, std::shared_ptr<DataType> expected_type) {
    ARROW_SCOPED_TRACE(json);

    ExtensionSet empty;
    ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Type", json));
    ASSERT_OK_AND_ASSIGN(auto type, DeserializeType(*buf, empty));

    EXPECT_EQ(*type, *expected_type);

    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeType(*type, &empty));
    EXPECT_EQ(empty.num_types(), 0);

    // FIXME chokes on NULLABILITY_UNSPECIFIED
    // EXPECT_THAT(internal::CheckMessagesEquivalent("Type", *buf, *serialized), Ok());

    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeType(*serialized, empty));

    EXPECT_EQ(*roundtripped, *expected_type);
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
  ExpectEq(R"({"interval_year": {}})", interval_year());
  ExpectEq(R"({"interval_day": {}})", interval_day());

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

  ExpectEq(R"({"map": {
    "key": {"string":{"nullability": "NULLABILITY_REQUIRED"}},
    "value": {"string":{}}
  }})",
           map(utf8(), field("", utf8()), false));
}

TEST(Substrait, SupportedExtensionTypes) {
  ExtensionSet ext_set;

  for (auto expected_type : {
           null(),
           uint8(),
           uint16(),
           uint32(),
           uint64(),
       }) {
    auto anchor = ext_set.num_types();

    EXPECT_THAT(ext_set.EncodeType(*expected_type), ResultWith(Eq(anchor)));
    ASSERT_OK_AND_ASSIGN(
        auto buf,
        internal::SubstraitFromJSON(
            "Type", "{\"user_defined_type_reference\": " + std::to_string(anchor) + "}"));

    ASSERT_OK_AND_ASSIGN(auto type, DeserializeType(*buf, ext_set));
    EXPECT_EQ(*type, *expected_type);

    auto size = ext_set.num_types();
    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeType(*type, &ext_set));
    EXPECT_EQ(ext_set.num_types(), size) << "was already added to the set above";

    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeType(*serialized, ext_set));
    EXPECT_EQ(*roundtripped, *expected_type);
  }
}

TEST(Substrait, NamedStruct) {
  ExtensionSet ext_set;

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("NamedStruct", R"({
    "struct": {
      "types": [
        {"i64": {}},
        {"list": {"type": {"string":{}} }},
        {"struct": {
          "types": [
            {"fp32": {"nullability": "NULLABILITY_REQUIRED"}},
            {"string": {}}
          ]
        }},
        {"list": {"type": {"string":{}} }},
      ]
    },
    "names": ["a", "b", "c", "d", "e", "f"]
  })"));
  ASSERT_OK_AND_ASSIGN(auto schema, DeserializeSchema(*buf, ext_set));
  Schema expected_schema({
      field("a", int64()),
      field("b", list(utf8())),
      field("c", struct_({
                     field("d", float32(), /*nullable=*/false),
                     field("e", utf8()),
                 })),
      field("f", list(utf8())),
  });
  EXPECT_EQ(*schema, expected_schema);

  ASSERT_OK_AND_ASSIGN(auto serialized, SerializeSchema(*schema, &ext_set));
  ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeSchema(*serialized, ext_set));
  EXPECT_EQ(*roundtripped, expected_schema);

  // too few names
  ASSERT_OK_AND_ASSIGN(buf, internal::SubstraitFromJSON("NamedStruct", R"({
    "struct": {"types": [{"i32": {}}, {"i32": {}}, {"i32": {}}]},
    "names": []
  })"));
  EXPECT_THAT(DeserializeSchema(*buf, ext_set), Raises(StatusCode::Invalid));

  // too many names
  ASSERT_OK_AND_ASSIGN(buf, internal::SubstraitFromJSON("NamedStruct", R"({
    "struct": {"types": []},
    "names": ["a", "b", "c"]
  })"));
  EXPECT_THAT(DeserializeSchema(*buf, ext_set), Raises(StatusCode::Invalid));

  // no schema metadata allowed
  EXPECT_THAT(SerializeSchema(Schema({}, key_value_metadata({{"ext", "yes"}})), &ext_set),
              Raises(StatusCode::Invalid));

  // no schema metadata allowed
  EXPECT_THAT(
      SerializeSchema(Schema({field("a", int32(), key_value_metadata({{"ext", "yes"}}))}),
                      &ext_set),
      Raises(StatusCode::Invalid));
}

TEST(Substrait, NoEquivalentArrowType) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON(
                                     "Type", R"({"user_defined_type_reference": 99})"));
  ExtensionSet empty;
  ASSERT_THAT(
      DeserializeType(*buf, empty),
      Raises(StatusCode::Invalid, HasSubstr("did not have a corresponding anchor")));
}

TEST(Substrait, NoEquivalentSubstraitType) {
  for (auto type : {
           date64(),
           timestamp(TimeUnit::SECOND),
           timestamp(TimeUnit::NANO),
           timestamp(TimeUnit::MICRO, "New York"),
           time32(TimeUnit::SECOND),
           time32(TimeUnit::MILLI),
           time64(TimeUnit::NANO),

           decimal256(76, 67),

           sparse_union({field("i8", int8()), field("f32", float32())}),
           dense_union({field("i8", int8()), field("f32", float32())}),
           dictionary(int32(), utf8()),

           fixed_size_list(float16(), 3),

           duration(TimeUnit::MICRO),

           large_utf8(),
           large_binary(),
           large_list(utf8()),
       }) {
    ARROW_SCOPED_TRACE(type->ToString());
    ExtensionSet set;
    EXPECT_THAT(SerializeType(*type, &set), Raises(StatusCode::NotImplemented));
  }
}

TEST(Substrait, SupportedLiterals) {
  auto ExpectEq = [](util::string_view json, Datum expected_value) {
    ARROW_SCOPED_TRACE(json);

    ASSERT_OK_AND_ASSIGN(
        auto buf, internal::SubstraitFromJSON("Expression",
                                              "{\"literal\":" + json.to_string() + "}"));
    ExtensionSet ext_set;
    ASSERT_OK_AND_ASSIGN(auto expr, DeserializeExpression(*buf, ext_set));

    ASSERT_TRUE(expr.literal());
    ASSERT_THAT(*expr.literal(), DataEq(expected_value));

    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(expr, &ext_set));
    EXPECT_EQ(ext_set.num_functions(), 0);  // shouldn't need extensions for core literals

    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeExpression(*serialized, ext_set));

    ASSERT_TRUE(roundtripped.literal());
    ASSERT_THAT(*roundtripped.literal(), DataEq(expected_value));
  };

  ExpectEq(R"({"boolean": true})", Datum(true));

  ExpectEq(R"({"i8": 34})", Datum(int8_t(34)));
  ExpectEq(R"({"i16": 34})", Datum(int16_t(34)));
  ExpectEq(R"({"i32": 34})", Datum(int32_t(34)));
  ExpectEq(R"({"i64": "34"})", Datum(int64_t(34)));

  ExpectEq(R"({"fp32": 3.5})", Datum(3.5F));
  ExpectEq(R"({"fp64": 7.125})", Datum(7.125));

  ExpectEq(R"({"string": "hello world"})", Datum("hello world"));

  ExpectEq(R"({"binary": "enp6"})", BinaryScalar(Buffer::FromString("zzz")));

  ExpectEq(R"({"timestamp": "579"})", TimestampScalar(579, TimeUnit::MICRO));

  ExpectEq(R"({"date": "5"})", Date32Scalar(5));

  ExpectEq(R"({"time": "64"})", Time64Scalar(64, TimeUnit::MICRO));

  ExpectEq(R"({"interval_year_to_month": {"years": 34, "months": 3}})",
           ExtensionScalar(FixedSizeListScalar(ArrayFromJSON(int32(), "[34, 3]")),
                           interval_year()));

  ExpectEq(R"({"interval_day_to_second": {"days": 34, "seconds": 3}})",
           ExtensionScalar(FixedSizeListScalar(ArrayFromJSON(int32(), "[34, 3]")),
                           interval_day()));

  ExpectEq(R"({"fixed_char": "zzz"})",
           ExtensionScalar(
               FixedSizeBinaryScalar(Buffer::FromString("zzz"), fixed_size_binary(3)),
               fixed_char(3)));

  ExpectEq(R"({"var_char": {"value": "zzz", "length": 1024}})",
           ExtensionScalar(StringScalar("zzz"), varchar(1024)));

  ExpectEq(R"({"fixed_binary": "enp6"})",
           FixedSizeBinaryScalar(Buffer::FromString("zzz"), fixed_size_binary(3)));

  ExpectEq(
      R"({"decimal": {"value": "0gKWSQAAAAAAAAAAAAAAAA==", "precision": 27, "scale": 5}})",
      Decimal128Scalar(Decimal128("123456789.0"), decimal128(27, 5)));

  ExpectEq(R"({"timestamp_tz": "579"})", TimestampScalar(579, TimeUnit::MICRO, "UTC"));

  // special case for empty lists
  ExpectEq(R"({"empty_list": {"type": {"i32": {}}}})",
           ScalarFromJSON(list(int32()), "[]"));

  ExpectEq(R"({"struct": {
    "fields": [
      {"i64": "32"},
      {"list": {"values": [
        {"string": "hello"},
        {"string": "world"}
      ]}}
    ]
  }})",
           ScalarFromJSON(struct_({
                              field("", int64()),
                              field("", list(utf8())),
                          }),
                          R"([32, ["hello", "world"]])"));

  // check null scalars:
  for (auto type : {
           boolean(),

           int8(),
           int64(),

           timestamp(TimeUnit::MICRO),
           interval_year(),

           struct_({
               field("", int64()),
               field("", list(utf8())),
           }),
       }) {
    ExtensionSet set;
    ASSERT_OK_AND_ASSIGN(auto buf, SerializeType(*type, &set));
    ASSERT_OK_AND_ASSIGN(auto json, internal::SubstraitToJSON("Type", *buf));
    ExpectEq("{\"null\": " + json + "}", MakeNullScalar(type));
  }
}

TEST(Substrait, CannotDeserializeLiteral) {
  ExtensionSet ext_set;

  // Invalid: missing List.element_type
  ASSERT_OK_AND_ASSIGN(
      auto buf, internal::SubstraitFromJSON("Expression",
                                            R"({"literal": {"list": {"values": []}}})"));
  EXPECT_THAT(DeserializeExpression(*buf, ext_set), Raises(StatusCode::Invalid));

  // Invalid: required null literal
  ASSERT_OK_AND_ASSIGN(
      buf,
      internal::SubstraitFromJSON(
          "Expression",
          R"({"literal": {"null": {"bool": {"nullability": "NULLABILITY_REQUIRED"}}}})"));
  EXPECT_THAT(DeserializeExpression(*buf, ext_set), Raises(StatusCode::Invalid));

  // no equivalent arrow scalar
  // FIXME no way to specify scalars of user_defined_type_reference
}

TEST(Substrait, FieldRefRoundTrip) {
  for (FieldRef ref : {
           // by name
           FieldRef("i32"),
           FieldRef("ts_ns"),
           FieldRef("struct"),

           // by index
           FieldRef(0),
           FieldRef(1),
           FieldRef(kBoringSchema->num_fields() - 1),
           FieldRef(kBoringSchema->GetFieldIndex("struct")),

           // nested
           FieldRef("struct", "i32"),
           FieldRef("struct", "struct_i32_str", "i32"),
           FieldRef(kBoringSchema->GetFieldIndex("struct"), 1),
       }) {
    ARROW_SCOPED_TRACE(ref.ToString());
    ASSERT_OK_AND_ASSIGN(auto expr, compute::field_ref(ref).Bind(*kBoringSchema));

    ExtensionSet ext_set;
    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(expr, &ext_set));
    EXPECT_EQ(ext_set.num_functions(),
              0);  // shouldn't need extensions for core field references
    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeExpression(*serialized, ext_set));
    ASSERT_TRUE(roundtripped.field_ref());

    ASSERT_OK_AND_ASSIGN(auto expected, ref.FindOne(*kBoringSchema));
    ASSERT_OK_AND_ASSIGN(auto actual, roundtripped.field_ref()->FindOne(*kBoringSchema));
    EXPECT_EQ(actual.indices(), expected.indices());
  }
}

TEST(Substrait, RecursiveFieldRef) {
  FieldRef ref("struct", "str");

  ARROW_SCOPED_TRACE(ref.ToString());
  ASSERT_OK_AND_ASSIGN(auto expr, compute::field_ref(ref).Bind(*kBoringSchema));
  ExtensionSet ext_set;
  ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(expr, &ext_set));
  ASSERT_OK_AND_ASSIGN(auto expected, internal::SubstraitFromJSON("Expression", R"({
    "selection": {
      "directReference": {
        "structField": {
          "field": 12,
          "child": {
            "structField": {
              "field": 1
            }
          }
        }
      },
      "rootReference": {}
    }
  })"));
  ASSERT_OK(internal::CheckMessagesEquivalent("Expression", *serialized, *expected));
}

TEST(Substrait, FieldRefsInExpressions) {
  ASSERT_OK_AND_ASSIGN(auto expr,
                       compute::call("struct_field",
                                     {compute::call("if_else",
                                                    {
                                                        compute::literal(true),
                                                        compute::field_ref("struct"),
                                                        compute::field_ref("struct"),
                                                    })},
                                     compute::StructFieldOptions({0}))
                           .Bind(*kBoringSchema));

  ExtensionSet ext_set;
  ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(expr, &ext_set));
  ASSERT_OK_AND_ASSIGN(auto expected, internal::SubstraitFromJSON("Expression", R"({
    "selection": {
      "directReference": {
        "structField": {
          "field": 0
        }
      },
      "expression": {
        "if_then": {
          "ifs": [
            {
              "if": {"literal": {"boolean": true}},
              "then": {"selection": {"directReference": {"structField": {"field": 12}}}}
            }
          ],
          "else": {"selection": {"directReference": {"structField": {"field": 12}}}}
        }
      }
    }
  })"));
  ASSERT_OK(internal::CheckMessagesEquivalent("Expression", *serialized, *expected));
}

TEST(Substrait, CallSpecialCaseRoundTrip) {
  for (compute::Expression expr : {
           compute::call("if_else",
                         {
                             compute::literal(true),
                             compute::field_ref({"struct", 1}),
                             compute::field_ref("str"),
                         }),

           compute::call(
               "case_when",
               {
                   compute::call("make_struct",
                                 {compute::literal(false), compute::literal(true)},
                                 compute::MakeStructOptions({"cond1", "cond2"})),
                   compute::field_ref({"struct", "str"}),
                   compute::field_ref({"struct", "struct_i32_str", "str"}),
                   compute::field_ref("str"),
               }),

           compute::call("list_element",
                         {
                             compute::field_ref("list_i32"),
                             compute::literal(3),
                         }),

           compute::call("struct_field",
                         {compute::call("list_element",
                                        {
                                            compute::field_ref("list_struct"),
                                            compute::literal(42),
                                        })},
                         arrow::compute::StructFieldOptions({1})),

           compute::call("struct_field",
                         {compute::call("list_element",
                                        {
                                            compute::field_ref("list_struct"),
                                            compute::literal(42),
                                        })},
                         arrow::compute::StructFieldOptions({2, 0})),

           compute::call("struct_field",
                         {compute::call("if_else",
                                        {
                                            compute::literal(true),
                                            compute::field_ref("struct"),
                                            compute::field_ref("struct"),
                                        })},
                         compute::StructFieldOptions({0})),
       }) {
    ARROW_SCOPED_TRACE(expr.ToString());
    ASSERT_OK_AND_ASSIGN(expr, expr.Bind(*kBoringSchema));

    ExtensionSet ext_set;
    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(expr, &ext_set));

    // These are special cased as core expressions in substrait; shouldn't require any
    // extensions.
    EXPECT_EQ(ext_set.num_functions(), 0);

    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeExpression(*serialized, ext_set));
    ASSERT_OK_AND_ASSIGN(roundtripped, roundtripped.Bind(*kBoringSchema));
    EXPECT_EQ(UseBoringRefs(roundtripped), UseBoringRefs(expr));
  }
}

TEST(Substrait, CallExtensionFunction) {
  for (compute::Expression expr : {
           compute::call("add", {compute::literal(0), compute::literal(1)}),
       }) {
    ARROW_SCOPED_TRACE(expr.ToString());
    ASSERT_OK_AND_ASSIGN(expr, expr.Bind(*kBoringSchema));

    ExtensionSet ext_set;
    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(expr, &ext_set));

    // These require an extension, so we should have a single-element ext_set.
    EXPECT_EQ(ext_set.num_functions(), 1);

    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeExpression(*serialized, ext_set));
    ASSERT_OK_AND_ASSIGN(roundtripped, roundtripped.Bind(*kBoringSchema));
    EXPECT_EQ(UseBoringRefs(roundtripped), UseBoringRefs(expr));
  }
}

TEST(Substrait, ReadRel) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Rel", R"({
    "read": {
      "base_schema": {
        "struct": {
          "types": [ {"i64": {}}, {"bool": {}} ]
        },
        "names": ["i", "b"]
      },
      "filter": {
        "selection": {
          "directReference": {
            "structField": {
              "field": 1
            }
          }
        }
      },
      "local_files": {
        "items": [
          {
            "uri_file": "file:///tmp/dat1.parquet",
            "format": "FILE_FORMAT_PARQUET"
          },
          {
            "uri_file": "file:///tmp/dat2.parquet",
            "format": "FILE_FORMAT_PARQUET"
          }
        ]
      }
    }
  })"));
  ExtensionSet ext_set;
  ASSERT_OK_AND_ASSIGN(auto rel, DeserializeRelation(*buf, ext_set));

  // converting a ReadRel produces a scan Declaration
  ASSERT_EQ(rel.factory_name, "scan");
  const auto& scan_node_options =
      checked_cast<const dataset::ScanNodeOptions&>(*rel.options);

  // filter on the boolean field (#1)
  EXPECT_EQ(scan_node_options.scan_options->filter, compute::field_ref(1));

  // dataset is a FileSystemDataset in parquet format with the specified schema
  ASSERT_EQ(scan_node_options.dataset->type_name(), "filesystem");
  const auto& dataset =
      checked_cast<const dataset::FileSystemDataset&>(*scan_node_options.dataset);
  EXPECT_THAT(dataset.files(), ElementsAre("/tmp/dat1.parquet", "/tmp/dat2.parquet"));
  EXPECT_EQ(dataset.format()->type_name(), "parquet");
  EXPECT_EQ(*dataset.schema(), Schema({field("i", int64()), field("b", boolean())}));
}

TEST(Substrait, ExtensionSetFromPlan) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", R"({
    "relations": [
      {"rel": {
        "read": {
          "base_schema": {
            "struct": {
              "types": [ {"i64": {}}, {"bool": {}} ]
            },
            "names": ["i", "b"]
          },
          "local_files": { "items": [] }
        }
      }}
    ],
    "extension_uris": [
      {
        "extension_uri_anchor": 7,
        "uri": "https://github.com/apache/arrow/blob/master/format/substrait/extension_types.yaml"
      }
    ],
    "extensions": [
      {"extension_type": {
        "extension_uri_reference": 7,
        "type_anchor": 42,
        "name": "null"
      }},
      {"extension_type_variation": {
        "extension_uri_reference": 7,
        "type_variation_anchor": 23,
        "name": "u8"
      }},
      {"extension_function": {
        "extension_uri_reference": 7,
        "function_anchor": 42,
        "name": "add"
      }}
    ]
  })"));

  ExtensionSet ext_set;
  ASSERT_OK_AND_ASSIGN(
      auto sink_decls,
      DeserializePlan(
          *buf, [] { return std::shared_ptr<compute::SinkNodeConsumer>{nullptr}; },
          &ext_set));

  EXPECT_OK_AND_ASSIGN(auto decoded_null_type, ext_set.DecodeType(42));
  EXPECT_EQ(decoded_null_type.id.uri, kArrowExtTypesUri);
  EXPECT_EQ(decoded_null_type.id.name, "null");
  EXPECT_EQ(*decoded_null_type.type, NullType());
  EXPECT_FALSE(decoded_null_type.is_variation);

  EXPECT_OK_AND_ASSIGN(auto decoded_uint8_type, ext_set.DecodeType(23));
  EXPECT_EQ(decoded_uint8_type.id.uri, kArrowExtTypesUri);
  EXPECT_EQ(decoded_uint8_type.id.name, "u8");
  EXPECT_EQ(*decoded_uint8_type.type, UInt8Type());
  EXPECT_TRUE(decoded_uint8_type.is_variation);

  EXPECT_OK_AND_ASSIGN(auto decoded_add_func, ext_set.DecodeFunction(42));
  EXPECT_EQ(decoded_add_func.id.uri, kArrowExtTypesUri);
  EXPECT_EQ(decoded_add_func.id.name, "add");
  EXPECT_EQ(decoded_add_func.name, "add");
}

TEST(Substrait, ExtensionSetFromPlanMissingFunc) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", R"({
    "relations": [],
    "extension_uris": [
      {
        "extension_uri_anchor": 7,
        "uri": "https://github.com/apache/arrow/blob/master/format/substrait/extension_types.yaml"
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 7,
        "function_anchor": 42,
        "name": "does_not_exist"
      }}
    ]
  })"));

  ExtensionSet ext_set;
  ASSERT_RAISES(
      Invalid,
      DeserializePlan(
          *buf, [] { return std::shared_ptr<compute::SinkNodeConsumer>{nullptr}; },
          &ext_set));
}

}  // namespace engine
}  // namespace arrow
