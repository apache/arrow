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

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>

#include <gmock/gmock.h>
#include <gtest/gtest-matchers.h>
#include <gtest/gtest.h>

#include "arrow/acero/asof_join_node.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/map_node.h"
#include "arrow/acero/options.h"
#include "arrow/acero/util.h"
#include "arrow/buffer.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/expression.h"
#include "arrow/compute/expression_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/plan.h"
#include "arrow/dataset/scanner.h"
#include "arrow/datum.h"
#include "arrow/engine/substrait/extension_set.h"
#include "arrow/engine/substrait/extension_types.h"
#include "arrow/engine/substrait/options.h"
#include "arrow/engine/substrait/serde.h"
#include "arrow/engine/substrait/test_util.h"
#include "arrow/engine/substrait/util.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/io/type_fwd.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/writer.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/async_generator_fwd.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/future.h"
#include "arrow/util/hash_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/iterator.h"
#include "arrow/util/key_value_metadata.h"

using testing::ElementsAre;
using testing::Eq;
using testing::HasSubstr;
using testing::UnorderedElementsAre;

namespace arrow {

using internal::checked_cast;
using internal::hash_combine;
namespace engine {

Status AddPassFactory(
    const std::string& factory_name,
    acero::ExecFactoryRegistry* registry = acero::default_exec_factory_registry()) {
  using acero::ExecNode;
  using acero::ExecNodeOptions;
  using acero::ExecPlan;
  using compute::ExecBatch;
  struct PassNode : public acero::MapNode {
    static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                  const acero::ExecNodeOptions& options) {
      RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "PassNode"));
      return plan->EmplaceNode<PassNode>(plan, inputs, inputs[0]->output_schema());
    }

    PassNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
             std::shared_ptr<Schema> output_schema)
        : MapNode(plan, inputs, output_schema) {}

    const char* kind_name() const override { return "PassNode"; }
    Result<ExecBatch> ProcessBatch(ExecBatch batch) override { return batch; }
  };
  return registry->AddFactory(factory_name, PassNode::Make);
}

const auto kNullConsumer = std::make_shared<acero::NullSinkNodeConsumer>();

void WriteIpcData(const std::string& path,
                  const std::shared_ptr<fs::FileSystem> file_system,
                  const std::shared_ptr<Table> input) {
  EXPECT_OK_AND_ASSIGN(auto out_stream, file_system->OpenOutputStream(path));
  ASSERT_OK_AND_ASSIGN(
      auto file_writer,
      MakeFileWriter(out_stream, input->schema(), ipc::IpcWriteOptions::Defaults()));
  ASSERT_OK(file_writer->WriteTable(*input));
  ASSERT_OK(file_writer->Close());
}

void CheckRoundTripResult(const std::shared_ptr<Table> expected_table,
                          std::shared_ptr<Buffer>& buf,
                          const std::vector<int>& include_columns = {},
                          const ConversionOptions& conversion_options = {},
                          const compute::SortOptions* sort_options = NULLPTR) {
  std::shared_ptr<ExtensionIdRegistry> sp_ext_id_reg = MakeExtensionIdRegistry();
  ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
  ExtensionSet ext_set(ext_id_reg);
  ASSERT_OK_AND_ASSIGN(auto sink_decls, DeserializePlans(
                                            *buf, [] { return kNullConsumer; },
                                            ext_id_reg, &ext_set, conversion_options));
  auto& other_declrs = std::get<acero::Declaration>(sink_decls[0].inputs[0]);

  ASSERT_OK_AND_ASSIGN(auto output_table,
                       acero::DeclarationToTable(other_declrs, /*use_threads=*/false));

  if (!include_columns.empty()) {
    ASSERT_OK_AND_ASSIGN(output_table, output_table->SelectColumns(include_columns));
  }
  if (sort_options) {
    ASSERT_OK_AND_ASSIGN(auto sort_indices,
                         SortIndices(output_table, std::move(*sort_options)));
    ASSERT_OK_AND_ASSIGN(auto maybe_table,
                         compute::Take(output_table, std::move(sort_indices),
                                       compute::TakeOptions::NoBoundsCheck()));
    output_table = maybe_table.table();
  }
  ASSERT_OK_AND_ASSIGN(output_table, output_table->CombineChunks());
  ASSERT_OK_AND_ASSIGN(auto merged_expected, expected_table->CombineChunks());
  engine::AssertTablesEqualIgnoringOrder(merged_expected, output_table);
}

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

int CountProjectNodeOptionsInDeclarations(const acero::Declaration& input) {
  int counter = 0;
  if (input.factory_name == "project") {
    counter++;
  }
  const auto& inputs = input.inputs;
  for (const auto& in : inputs) {
    counter += CountProjectNodeOptionsInDeclarations(std::get<acero::Declaration>(in));
  }
  return counter;
}
/// Validate the number of expected ProjectNodes
///
/// Project nodes are sometimes added by emit elements and we may want to
/// verify that we are not adding too many
void ValidateNumProjectNodes(int expected_projections, const std::shared_ptr<Buffer>& buf,
                             const ConversionOptions& conversion_options) {
  std::shared_ptr<ExtensionIdRegistry> sp_ext_id_reg = MakeExtensionIdRegistry();
  ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
  ExtensionSet ext_set(ext_id_reg);
  ASSERT_OK_AND_ASSIGN(auto sink_decls, DeserializePlans(
                                            *buf, [] { return kNullConsumer; },
                                            ext_id_reg, &ext_set, conversion_options));
  auto& other_declrs = std::get<acero::Declaration>(sink_decls[0].inputs[0]);
  int num_projections = CountProjectNodeOptionsInDeclarations(other_declrs);
  ASSERT_EQ(num_projections, expected_projections);
}

TEST(Substrait, SupportedTypes) {
  auto ExpectEq = [](std::string_view json, std::shared_ptr<DataType> expected_type) {
    ARROW_SCOPED_TRACE(json);

    ExtensionSet empty;
    ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON(
                                       "Type", json, /*ignore_unknown_fields=*/false));
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
            "Type",
            "{\"user_defined\": { \"type_reference\": " + std::to_string(anchor) +
                ", \"nullability\": \"NULLABILITY_NULLABLE\" } }",
            /*ignore_unknown_fields=*/false));

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

  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("NamedStruct", R"({
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
  })",
                                                   /*ignore_unknown_fields=*/false));
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
  })",
                                                        /*ignore_unknown_fields=*/false));
  EXPECT_THAT(DeserializeSchema(*buf, ext_set), Raises(StatusCode::Invalid));

  // too many names
  ASSERT_OK_AND_ASSIGN(buf, internal::SubstraitFromJSON("NamedStruct", R"({
    "struct": {"types": []},
    "names": ["a", "b", "c"]
  })",
                                                        /*ignore_unknown_fields=*/false));
  EXPECT_THAT(DeserializeSchema(*buf, ext_set), Raises(StatusCode::Invalid));

  ConversionOptions conversion_options;
  conversion_options.strictness = ConversionStrictness::EXACT_ROUNDTRIP;

  // no schema metadata allowed with EXACT_ROUNDTRIP
  EXPECT_THAT(SerializeSchema(Schema({}, key_value_metadata({{"ext", "yes"}})), &ext_set,
                              conversion_options),
              Raises(StatusCode::Invalid));

  ASSERT_OK(SerializeSchema(Schema({}, key_value_metadata({{"ext", "yes"}})), &ext_set));

  // no field metadata allowed with EXACT_ROUNDTRIP
  EXPECT_THAT(
      SerializeSchema(Schema({field("a", int32(), key_value_metadata({{"ext", "yes"}}))}),
                      &ext_set, conversion_options),
      Raises(StatusCode::Invalid));
}

TEST(Substrait, NoEquivalentArrowType) {
  ASSERT_OK_AND_ASSIGN(
      auto buf,
      internal::SubstraitFromJSON("Type", R"({"user_defined": {"type_reference": 99}})",
                                  /*ignore_unknown_fields=*/false));
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
  auto ExpectEq = [](std::string_view json, Datum expected_value) {
    ARROW_SCOPED_TRACE(json);
    for (bool nullable : {false, true}) {
      std::string json_with_nullable;
      if (nullable) {
        auto final_closing_brace = json.find_last_of('}');
        ASSERT_NE(std::string_view::npos, final_closing_brace);
        json_with_nullable =
            std::string(json.substr(0, final_closing_brace)) + ", \"nullable\": true}";
        json = json_with_nullable;
      }
      ASSERT_OK_AND_ASSIGN(
          auto buf, internal::SubstraitFromJSON("Expression",
                                                "{\"literal\":" + std::string(json) + "}",
                                                /*ignore_unknown_fields=*/false));
      ExtensionSet ext_set;
      ASSERT_OK_AND_ASSIGN(auto expr, DeserializeExpression(*buf, ext_set));

      ASSERT_TRUE(expr.literal());
      ASSERT_THAT(*expr.literal(), DataEq(expected_value));

      ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(expr, &ext_set));
      EXPECT_EQ(ext_set.num_functions(),
                0);  // shouldn't need extensions for core literals

      ASSERT_OK_AND_ASSIGN(auto roundtripped,
                           DeserializeExpression(*serialized, ext_set));

      ASSERT_TRUE(roundtripped.literal());
      ASSERT_THAT(*roundtripped.literal(), DataEq(expected_value));
    }
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
                                            R"({"literal": {"list": {"values": []}}})",
                                            /*ignore_unknown_fields=*/false));
  EXPECT_THAT(DeserializeExpression(*buf, ext_set), Raises(StatusCode::Invalid));

  // Invalid: required null literal if in strict mode
  ConversionOptions conversion_options;
  conversion_options.strictness = ConversionStrictness::EXACT_ROUNDTRIP;
  ASSERT_OK_AND_ASSIGN(
      buf,
      internal::SubstraitFromJSON(
          "Expression",
          R"({"literal": {"null": {"bool": {"nullability": "NULLABILITY_REQUIRED"}}}})",
          /*ignore_unknown_fields=*/false));
  EXPECT_THAT(DeserializeExpression(*buf, ext_set, conversion_options),
              Raises(StatusCode::Invalid));

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
  ASSERT_OK_AND_ASSIGN(auto expected,
                       internal::SubstraitFromJSON("Expression", R"({
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
  })",
                                                   /*ignore_unknown_fields=*/false));
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
  ASSERT_OK_AND_ASSIGN(auto expected,
                       internal::SubstraitFromJSON("Expression", R"({
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
  })",
                                                   /*ignore_unknown_fields=*/false));
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

TEST(Substrait, Cast) {
  ExtensionSet ext_set;
  ConversionOptions conversion_options;

  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Expression", R"({
  "selection": {
      "direct_reference": {
        "struct_field": {
          "field": 0
        }
      },
    "expression": {
      "cast": {
        "type": {
          "fp64": {
            "nullability": "NULLABILITY_NULLABLE"
          }
        },
        "input": {
          "selection": {
            "direct_reference": {
              "struct_field": {
                "field": 0
              }
            }
          }
        },
        "failure_behavior": "FAILURE_BEHAVIOR_THROW_EXCEPTION"
      }
    }
  }
})",
                                                   /*ignore_unknown_fields=*/false))

  ASSERT_OK_AND_ASSIGN(auto expr, DeserializeExpression(*buf, ext_set));

  ASSERT_TRUE(expr.call());

  ASSERT_THAT(expr.call()->arguments[0].call()->function_name, "cast");

  std::shared_ptr<compute::FunctionOptions> call_opts =
      expr.call()->arguments[0].call()->options;

  ASSERT_TRUE(!!call_opts);
  std::shared_ptr<compute::CastOptions> cast_opts =
      std::dynamic_pointer_cast<compute::CastOptions>(call_opts);
  ASSERT_TRUE(!!cast_opts);
  // It is unclear whether a Substrait cast should be safe or not.  In the meantime we are
  // assuming it is unsafe based on the behavior of many SQL engines.
  ASSERT_TRUE(cast_opts->allow_int_overflow);
  ASSERT_TRUE(cast_opts->allow_float_truncate);
  ASSERT_TRUE(cast_opts->allow_decimal_truncate);
}

TEST(Substrait, CastRequiresFailureBehavior) {
  ExtensionSet ext_set;
  ConversionOptions conversion_options;

  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Expression", R"({
  "selection": {
      "direct_reference": {
        "struct_field": {
          "field": 0
        }
      },
    "expression": {
      "cast": {
        "type": {
          "fp64": {
            "nullability": "NULLABILITY_NULLABLE"
          }
        },
        "input": {
          "selection": {
            "direct_reference": {
              "struct_field": {
                "field": 0
              }
            }
          }
        },
        "failure_behavior": "FAILURE_BEHAVIOR_UNSPECIFIED"
      }
    }
  }
})",
                                                   /*ignore_unknown_fields=*/false))

  EXPECT_THAT(DeserializeExpression(*buf, ext_set, conversion_options),
              Raises(StatusCode::Invalid, HasSubstr("FailureBehavior unspecified")));
}

TEST(Substrait, CallCastNonNullableFails) {
  ExtensionSet ext_set;
  ConversionOptions conversion_options;
  conversion_options.strictness = ConversionStrictness::EXACT_ROUNDTRIP;

  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Expression", R"({
  "selection": {
      "direct_reference": {
        "struct_field": {
          "field": 0
        }
      },
    "expression": {
      "cast": {
        "type": {
          "fp64": {
            "nullability": "NULLABILITY_REQUIRED"
          }
        },
        "input": {
          "selection": {
            "direct_reference": {
              "struct_field": {
                "field": 0
              }
            }
          }
        },
        "failure_behavior": "FAILURE_BEHAVIOR_THROW_EXCEPTION"
      }
    }
  }
})",
                                                   /*ignore_unknown_fields=*/false))

  EXPECT_THAT(DeserializeExpression(*buf, ext_set, conversion_options),
              Raises(StatusCode::Invalid, HasSubstr("must be of nullable type")));
}

TEST(Substrait, ReadRel) {
  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Rel", R"({
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
            "parquet": {}
          },
          {
            "uri_file": "file:///tmp/dat2.parquet",
            "parquet": {}
          }
        ]
      }
    }
  })",
                                                   /*ignore_unknown_fields=*/false));
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
  EXPECT_THAT(dataset.files(),
              UnorderedElementsAre("/tmp/dat1.parquet", "/tmp/dat2.parquet"));
  EXPECT_EQ(dataset.format()->type_name(), "parquet");
  EXPECT_EQ(*dataset.schema(), Schema({field("i", int64()), field("b", boolean())}));
}

/// \brief Create a NamedTableProvider that provides `table` regardless of the name
NamedTableProvider AlwaysProvideSameTable(std::shared_ptr<Table> table) {
  return [table = std::move(table)](const std::vector<std::string>&, const Schema&) {
    std::shared_ptr<acero::ExecNodeOptions> options =
        std::make_shared<acero::TableSourceNodeOptions>(table);
    return acero::Declaration("table_source", {}, options, "mock_source");
  };
}

TEST(Substrait, RelWithHint) {
  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Rel", R"({
    "read": {
      "common": {
        "hint": {
          "stats": {
            "row_count": 1
          }
        },
        "direct": { }
      },
      "base_schema": {
        "struct": {
          "types": [ {"i64": {}}, {"bool": {}} ]
        },
        "names": ["i", "b"]
      },
      "named_table": { "names": [ "foo" ] }
    }
  })",
                                                   /*ignore_unknown_fields=*/false));

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = AlwaysProvideSameTable(nullptr);

  ExtensionSet ext_set;
  ASSERT_OK_AND_ASSIGN(auto rel, DeserializeRelation(*buf, ext_set, conversion_options));

  conversion_options.strictness = ConversionStrictness::EXACT_ROUNDTRIP;
  ASSERT_RAISES(NotImplemented, DeserializeRelation(*buf, ext_set, conversion_options));
}

TEST(Substrait, ExtensionSetFromPlan) {
  std::string substrait_json = R"({
    "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
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
        "uri": ")" + default_extension_types_uri() +
                               R"("
      },
      {
        "extension_uri_anchor": 18,
        "uri": ")" + kSubstraitArithmeticFunctionsUri +
                               R"("
      }
    ],
    "extensions": [
      {"extension_type": {
        "extension_uri_reference": 7,
        "type_anchor": 42,
        "name": "null"
      }},
      {"extension_function": {
        "extension_uri_reference": 18,
        "function_anchor": 42,
        "name": "add"
      }}
    ]
})";
  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));
  for (auto sp_ext_id_reg :
       {std::shared_ptr<ExtensionIdRegistry>(), MakeExtensionIdRegistry()}) {
    ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
    ExtensionSet ext_set(ext_id_reg);
    ASSERT_OK_AND_ASSIGN(auto sink_decls,
                         DeserializePlans(
                             *buf, [] { return kNullConsumer; }, ext_id_reg, &ext_set));

    EXPECT_OK_AND_ASSIGN(auto decoded_null_type, ext_set.DecodeType(42));
    EXPECT_EQ(decoded_null_type.id.uri, kArrowExtTypesUri);
    EXPECT_EQ(decoded_null_type.id.name, "null");
    EXPECT_EQ(*decoded_null_type.type, NullType());

    EXPECT_OK_AND_ASSIGN(Id decoded_add_func_id, ext_set.DecodeFunction(42));
    EXPECT_EQ(decoded_add_func_id.uri, kSubstraitArithmeticFunctionsUri);
    EXPECT_EQ(decoded_add_func_id.name, "add");
  }
}

TEST(Substrait, ExtensionSetFromPlanMissingFunc) {
  std::string substrait_json = R"({
    "relations": [],
    "extension_uris": [
      {
        "extension_uri_anchor": 7,
        "uri": ")" + default_extension_types_uri() +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 7,
        "function_anchor": 42,
        "name": "does_not_exist"
      }}
    ]
  })";
  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));

  for (auto sp_ext_id_reg :
       {std::shared_ptr<ExtensionIdRegistry>(), MakeExtensionIdRegistry()}) {
    ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
    ExtensionSet ext_set(ext_id_reg);
    // Since the function is not referenced this plan is ok unless we are asking for
    // strict conversion.
    ConversionOptions options;
    options.strictness = ConversionStrictness::EXACT_ROUNDTRIP;
    ASSERT_RAISES(Invalid,
                  DeserializePlans(
                      *buf, [] { return kNullConsumer; }, ext_id_reg, &ext_set, options));
  }
}

TEST(Substrait, ExtensionSetFromPlanExhaustedFactory) {
  std::string substrait_json = R"({
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
        "uri": ")" + default_extension_types_uri() +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 7,
        "function_anchor": 42,
        "name": "add"
      }}
    ]
  })";
  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));

  for (auto sp_ext_id_reg :
       {std::shared_ptr<ExtensionIdRegistry>(), MakeExtensionIdRegistry()}) {
    ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
    ExtensionSet ext_set(ext_id_reg);
    ASSERT_RAISES(
        Invalid,
        DeserializePlans(
            *buf, []() -> std::shared_ptr<acero::SinkNodeConsumer> { return nullptr; },
            ext_id_reg, &ext_set));
    ASSERT_RAISES(
        Invalid,
        DeserializePlans(
            *buf, []() -> std::shared_ptr<dataset::WriteNodeOptions> { return nullptr; },
            ext_id_reg, &ext_set));
  }
}

TEST(Substrait, ExtensionSetFromPlanRegisterFunc) {
  std::string substrait_json = R"({
    "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
    "relations": [],
    "extension_uris": [
      {
        "extension_uri_anchor": 7,
        "uri": ")" + default_extension_types_uri() +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 7,
        "function_anchor": 42,
        "name": "new_func"
      }}
    ]
  })";
  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));

  auto sp_ext_id_reg = MakeExtensionIdRegistry();
  ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
  // invalid before registration
  ExtensionSet ext_set_invalid(ext_id_reg);
  ConversionOptions conversion_options;
  conversion_options.strictness = ConversionStrictness::EXACT_ROUNDTRIP;
  ASSERT_RAISES(Invalid, DeserializePlans(
                             *buf, [] { return kNullConsumer; }, ext_id_reg,
                             &ext_set_invalid, conversion_options));
  ASSERT_OK(ext_id_reg->AddSubstraitCallToArrow(
      {default_extension_types_uri(), "new_func"}, "multiply"));
  // valid after registration
  ExtensionSet ext_set_valid(ext_id_reg);
  ASSERT_OK_AND_ASSIGN(auto sink_decls,
                       DeserializePlans(
                           *buf, [] { return kNullConsumer; }, ext_id_reg, &ext_set_valid,
                           conversion_options));
  EXPECT_OK_AND_ASSIGN(Id decoded_add_func_id, ext_set_valid.DecodeFunction(42));
  EXPECT_EQ(decoded_add_func_id.uri, kArrowExtTypesUri);
  EXPECT_EQ(decoded_add_func_id.name, "new_func");
}

Result<std::string> GetSubstraitJSON() {
  ARROW_ASSIGN_OR_RAISE(std::string dir_string,
                        arrow::internal::GetEnvVar("PARQUET_TEST_DATA"));
  auto file_name =
      arrow::internal::PlatformFilename::FromString(dir_string)->Join("binary.parquet");
  auto file_path = file_name->ToString();

  std::string substrait_json = R"({
    "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
    "relations": [
      {"rel": {
        "read": {
          "base_schema": {
            "struct": {
              "types": [
                         {"binary": {}}
                       ]
            },
            "names": [
                      "foo"
                      ]
          },
          "local_files": {
            "items": [
              {
                "uri_file": "file://FILENAME_PLACEHOLDER",
                "parquet": {}
              }
            ]
          }
        }
      }}
    ]
  })";
  std::string filename_placeholder = "FILENAME_PLACEHOLDER";
  substrait_json.replace(substrait_json.find(filename_placeholder),
                         filename_placeholder.size(), file_path);
  return substrait_json;
}

TEST(Substrait, DeserializeWithConsumerFactory) {
  ASSERT_OK_AND_ASSIGN(std::string substrait_json, GetSubstraitJSON());
  ASSERT_OK_AND_ASSIGN(auto buf, SerializeJsonPlan(substrait_json));
  ASSERT_OK_AND_ASSIGN(auto declarations,
                       DeserializePlans(*buf, acero::NullSinkNodeConsumer::Make));
  ASSERT_EQ(declarations.size(), 1);
  acero::Declaration* decl = &declarations[0];
  ASSERT_EQ(decl->factory_name, "consuming_sink");
  ASSERT_OK_AND_ASSIGN(auto plan, acero::ExecPlan::Make());
  ASSERT_OK_AND_ASSIGN(auto sink_node, declarations[0].AddToPlan(plan.get()));
  ASSERT_STREQ(sink_node->kind_name(), "ConsumingSinkNode");
  ASSERT_EQ(sink_node->num_inputs(), 1);
  auto& prev_node = sink_node->inputs()[0];
  ASSERT_STREQ(prev_node->kind_name(), "SourceNode");

  plan->StartProducing();
  ASSERT_FINISHES_OK(plan->finished());
}

TEST(Substrait, DeserializeSinglePlanWithConsumerFactory) {
  ASSERT_OK_AND_ASSIGN(std::string substrait_json, GetSubstraitJSON());
  ASSERT_OK_AND_ASSIGN(auto buf, SerializeJsonPlan(substrait_json));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<acero::ExecPlan> plan,
                       DeserializePlan(*buf, acero::NullSinkNodeConsumer::Make()));
  ASSERT_EQ(2, plan->nodes().size());
  acero::ExecNode* sink_node = plan->nodes()[1];
  ASSERT_STREQ(sink_node->kind_name(), "ConsumingSinkNode");
  ASSERT_EQ(sink_node->num_inputs(), 1);
  auto& prev_node = sink_node->inputs()[0];
  ASSERT_STREQ(prev_node->kind_name(), "SourceNode");

  plan->StartProducing();
  ASSERT_FINISHES_OK(plan->finished());
}

TEST(Substrait, DeserializeWithWriteOptionsFactory) {
  dataset::internal::Initialize();
  fs::TimePoint mock_now = std::chrono::system_clock::now();
  fs::FileInfo testdir = ::arrow::fs::Dir("testdir");
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<fs::FileSystem> fs,
                       fs::internal::MockFileSystem::Make(mock_now, {testdir}));
  auto write_options_factory = [&fs] {
    std::shared_ptr<dataset::IpcFileFormat> format =
        std::make_shared<dataset::IpcFileFormat>();
    dataset::FileSystemDatasetWriteOptions options;
    options.file_write_options = format->DefaultWriteOptions();
    options.filesystem = fs;
    options.basename_template = "chunk-{i}.arrow";
    options.base_dir = "testdir";
    options.partitioning =
        std::make_shared<dataset::DirectoryPartitioning>(arrow::schema({}));
    return std::make_shared<dataset::WriteNodeOptions>(options);
  };
  ASSERT_OK_AND_ASSIGN(std::string substrait_json, GetSubstraitJSON());
  ASSERT_OK_AND_ASSIGN(auto buf, SerializeJsonPlan(substrait_json));
  ASSERT_OK_AND_ASSIGN(auto declarations, DeserializePlans(*buf, write_options_factory));
  ASSERT_EQ(declarations.size(), 1);
  acero::Declaration* decl = &declarations[0];
  ASSERT_EQ(decl->factory_name, "write");
  ASSERT_EQ(decl->inputs.size(), 1);
  decl = std::get_if<acero::Declaration>(&decl->inputs[0]);
  ASSERT_NE(decl, nullptr);
  ASSERT_EQ(decl->factory_name, "scan");
  ASSERT_OK_AND_ASSIGN(auto plan, acero::ExecPlan::Make());
  ASSERT_OK_AND_ASSIGN(auto sink_node, declarations[0].AddToPlan(plan.get()));
  ASSERT_STREQ(sink_node->kind_name(), "ConsumingSinkNode");
  ASSERT_EQ(sink_node->num_inputs(), 1);
  auto& prev_node = sink_node->inputs()[0];
  ASSERT_STREQ(prev_node->kind_name(), "SourceNode");

  plan->StartProducing();
  ASSERT_FINISHES_OK(plan->finished());
}

static void test_with_registries(
    std::function<void(ExtensionIdRegistry*, compute::FunctionRegistry*)> test) {
  auto default_func_reg = compute::GetFunctionRegistry();
  auto nested_ext_id_reg = MakeExtensionIdRegistry();
  auto nested_func_reg = compute::FunctionRegistry::Make(default_func_reg);
  test(nullptr, default_func_reg);
  test(nullptr, nested_func_reg.get());
  test(nested_ext_id_reg.get(), default_func_reg);
  test(nested_ext_id_reg.get(), nested_func_reg.get());
}

TEST(Substrait, GetRecordBatchReader) {
  ASSERT_OK_AND_ASSIGN(std::string substrait_json, GetSubstraitJSON());
  test_with_registries([&substrait_json](ExtensionIdRegistry* ext_id_reg,
                                         compute::FunctionRegistry* func_registry) {
    ASSERT_OK_AND_ASSIGN(auto buf, SerializeJsonPlan(substrait_json));
    ASSERT_OK_AND_ASSIGN(auto reader, ExecuteSerializedPlan(*buf));
    ASSERT_OK_AND_ASSIGN(auto table, Table::FromRecordBatchReader(reader.get()));
    // Note: assuming the binary.parquet file contains fixed amount of records
    // in case of a test failure, re-evalaute the content in the file
    EXPECT_EQ(table->num_rows(), 12);
  });
}

TEST(Substrait, InvalidPlan) {
  std::string substrait_json = R"({
    "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
    "relations": [
    ]
  })";
  test_with_registries([&substrait_json](ExtensionIdRegistry* ext_id_reg,
                                         compute::FunctionRegistry* func_registry) {
    ASSERT_OK_AND_ASSIGN(auto buf, SerializeJsonPlan(substrait_json));
    ASSERT_RAISES(Invalid, ExecuteSerializedPlan(*buf));
  });
}

TEST(Substrait, InvalidMinimumVersion) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", R"({
    "version": { "major_number": 0, "minor_number": 18, "patch_number": 0 },
    "relations": [{
      "rel": {
        "read": {
          "base_schema": {
            "names": ["A"],
            "struct": {
              "types": [{
                "i32": {}
              }]
            }
          },
          "named_table": { "names": ["x"] }
        }
      }
    }],
    "extensionUris": [],
    "extensions": [],
  })"));

  ASSERT_RAISES(Invalid, DeserializePlans(*buf, [] { return kNullConsumer; }));
}

TEST(Substrait, JoinPlanBasic) {
  std::string substrait_json = R"({
  "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
  "relations": [{
    "rel": {
      "join": {
        "left": {
          "read": {
            "base_schema": {
              "names": ["A", "B", "C"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "local_files": {
              "items": [
                {
                  "uri_file": "file:///tmp/dat1.parquet",
                  "parquet": {}
                }
              ]
            }
          }
        },
        "right": {
          "read": {
            "base_schema": {
              "names": ["X", "Y", "A"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "local_files": {
              "items": [
                {
                  "uri_file": "file:///tmp/dat2.parquet",
                  "parquet": {}
                }
              ]
            }
          }
        },
        "expression": {
          "scalarFunction": {
            "functionReference": 0,
            "arguments": [{
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 0
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }, {
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 5
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }],
            "output_type": {
              "bool": {}
            }
          }
        },
        "type": "JOIN_TYPE_INNER"
      }
    }
  }],
  "extension_uris": [
      {
        "extension_uri_anchor": 0,
        "uri": ")" + std::string(kSubstraitComparisonFunctionsUri) +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "equal"
      }}
    ]
  })";
  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));
  for (auto sp_ext_id_reg :
       {std::shared_ptr<ExtensionIdRegistry>(), MakeExtensionIdRegistry()}) {
    ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
    ExtensionSet ext_set(ext_id_reg);
    ASSERT_OK_AND_ASSIGN(auto sink_decls,
                         DeserializePlans(
                             *buf, [] { return kNullConsumer; }, ext_id_reg, &ext_set));

    auto join_decl = sink_decls[0].inputs[0];

    const auto& join_rel = std::get<acero::Declaration>(join_decl);

    const auto& join_options =
        checked_cast<const acero::HashJoinNodeOptions&>(*join_rel.options);

    EXPECT_EQ(join_rel.factory_name, "hashjoin");
    EXPECT_EQ(join_options.join_type, acero::JoinType::INNER);

    const auto& left_rel = std::get<acero::Declaration>(join_rel.inputs[0]);
    const auto& right_rel = std::get<acero::Declaration>(join_rel.inputs[1]);

    const auto& l_options =
        checked_cast<const dataset::ScanNodeOptions&>(*left_rel.options);
    const auto& r_options =
        checked_cast<const dataset::ScanNodeOptions&>(*right_rel.options);

    AssertSchemaEqual(
        l_options.dataset->schema(),
        schema({field("A", int32()), field("B", int32()), field("C", int32())}));
    AssertSchemaEqual(
        r_options.dataset->schema(),
        schema({field("X", int32()), field("Y", int32()), field("A", int32())}));

    EXPECT_EQ(join_options.key_cmp[0], acero::JoinKeyCmp::EQ);
  }
}

TEST(Substrait, JoinPlanInvalidKeyCmp) {
  std::string substrait_json = R"({
  "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
  "relations": [{
    "rel": {
      "join": {
        "left": {
          "read": {
            "base_schema": {
              "names": ["A", "B", "C"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "local_files": {
              "items": [
                {
                  "uri_file": "file:///tmp/dat1.parquet",
                  "parquet": {}
                }
              ]
            }
          }
        },
        "right": {
          "read": {
            "base_schema": {
              "names": ["X", "Y", "A"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "local_files": {
              "items": [
                {
                  "uri_file": "file:///tmp/dat2.parquet",
                  "parquet": {}
                }
              ]
            }
          }
        },
        "expression": {
          "scalarFunction": {
            "functionReference": 0,
            "arguments": [{
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 0
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }, {
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 5
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }],
            "output_type": {
              "bool": {}
            }
          }
        },
        "type": "JOIN_TYPE_INNER"
      }
    }
  }],
  "extension_uris": [
      {
        "extension_uri_anchor": 0,
        "uri": ")" + std::string(kSubstraitArithmeticFunctionsUri) +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "add"
      }}
    ]
  })";
  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));
  for (auto sp_ext_id_reg :
       {std::shared_ptr<ExtensionIdRegistry>(), MakeExtensionIdRegistry()}) {
    ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
    ExtensionSet ext_set(ext_id_reg);
    ASSERT_RAISES(Invalid, DeserializePlans(
                               *buf, [] { return kNullConsumer; }, ext_id_reg, &ext_set));
  }
}

TEST(Substrait, JoinPlanInvalidExpression) {
  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", R"({
  "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
  "relations": [{
    "rel": {
      "join": {
        "left": {
          "read": {
            "base_schema": {
              "names": ["A", "B", "C"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "local_files": {
              "items": [
                {
                  "uri_file": "file:///tmp/dat1.parquet",
                  "parquet": {}
                }
              ]
            }
          }
        },
        "right": {
          "read": {
            "base_schema": {
              "names": ["X", "Y", "A"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "local_files": {
              "items": [
                {
                  "uri_file": "file:///tmp/dat2.parquet",
                  "parquet": {}
                }
              ]
            }
          }
        },
        "expression": {"literal": {"list": {"values": []}}},
        "type": "JOIN_TYPE_INNER"
      }
    }
  }]
  })",
                                                   /*ignore_unknown_fields=*/false));
  for (auto sp_ext_id_reg :
       {std::shared_ptr<ExtensionIdRegistry>(), MakeExtensionIdRegistry()}) {
    ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
    ExtensionSet ext_set(ext_id_reg);
    ASSERT_RAISES(Invalid, DeserializePlans(
                               *buf, [] { return kNullConsumer; }, ext_id_reg, &ext_set));
  }
}

TEST(Substrait, JoinPlanInvalidKeys) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", R"({
  "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
  "relations": [{
    "rel": {
      "join": {
        "left": {
          "read": {
            "base_schema": {
              "names": ["A", "B", "C"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "local_files": {
              "items": [
                {
                  "uri_file": "file:///tmp/dat1.parquet",
                  "parquet": {}
                }
              ]
            }
          }
        },
        "expression": {
          "scalarFunction": {
            "functionReference": 0,
            "arguments": [{
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 0
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }, {
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 5
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }]
          }
        },
        "type": "JOIN_TYPE_INNER"
      }
    }
  }]
  })"));
  for (auto sp_ext_id_reg :
       {std::shared_ptr<ExtensionIdRegistry>(), MakeExtensionIdRegistry()}) {
    ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
    ExtensionSet ext_set(ext_id_reg);
    ASSERT_RAISES(Invalid, DeserializePlans(
                               *buf, [] { return kNullConsumer; }, ext_id_reg, &ext_set));
  }
}

TEST(Substrait, AggregateBasic) {
  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", R"({
    "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
    "relations": [{
      "rel": {
        "aggregate": {
          "input": {
            "read": {
              "base_schema": {
                "names": ["A", "B", "C"],
                "struct": {
                  "types": [{
                    "i32": {}
                  }, {
                    "i32": {}
                  }, {
                    "i32": {}
                  }]
                }
              },
              "local_files": {
                "items": [
                  {
                    "uri_file": "file:///tmp/dat.parquet",
                    "parquet": {}
                  }
                ]
              }
            }
          },
          "groupings": [{
            "groupingExpressions": [{
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                }
              }
            }]
          }],
          "measures": [{
            "measure": {
              "functionReference": 0,
              "arguments": [{
                "value": {
                  "selection": {
                    "directReference": {
                      "structField": {
                        "field": 1
                      }
                    }
                  }
                }
            }],
              "sorts": [],
              "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
              "outputType": {
                "i64": {}
              }
            }
          }]
        }
      }
    }],
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "sum"
      }
    }],
  })",
                                                   /*ignore_unknown_fields=*/false));

  auto sp_ext_id_reg = MakeExtensionIdRegistry();
  ASSERT_OK_AND_ASSIGN(auto sink_decls,
                       DeserializePlans(*buf, [] { return kNullConsumer; }));
  auto agg_decl = sink_decls[0].inputs[0];

  const auto& agg_rel = std::get<acero::Declaration>(agg_decl);

  const auto& agg_options =
      checked_cast<const acero::AggregateNodeOptions&>(*agg_rel.options);

  EXPECT_EQ(agg_rel.factory_name, "aggregate");
  EXPECT_EQ(agg_options.aggregates[0].name, "");
  EXPECT_EQ(agg_options.aggregates[0].function, "hash_sum");
}

TEST(Substrait, AggregateInvalidRel) {
  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", R"({
    "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
    "relations": [{
      "rel": {
        "aggregate": {
        }
      }
    }],
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "sum"
      }
    }],
  })",
                                                   /*ignore_unknown_fields=*/false));

  ASSERT_RAISES(Invalid, DeserializePlans(*buf, [] { return kNullConsumer; }));
}

TEST(Substrait, AggregateInvalidFunction) {
  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", R"({
    "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
    "relations": [{
      "rel": {
        "aggregate": {
          "input": {
            "read": {
              "base_schema": {
                "names": ["A", "B", "C"],
                "struct": {
                  "types": [{
                    "i32": {}
                  }, {
                    "i32": {}
                  }, {
                    "i32": {}
                  }]
                }
              },
              "local_files": {
                "items": [
                  {
                    "uri_file": "file:///tmp/dat.parquet",
                    "parquet": {}
                  }
                ]
              }
            }
          },
          "groupings": [{
            "groupingExpressions": [{
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                }
              }
            }]
          }],
          "measures": [{
          }]
        }
      }
    }],
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "sum"
      }
    }],
  })",
                                                   /*ignore_unknown_fields=*/false));

  ASSERT_RAISES(Invalid, DeserializePlans(*buf, [] { return kNullConsumer; }));
}

TEST(Substrait, AggregateInvalidAggFuncArgs) {
  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", R"({
    "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
    "relations": [{
      "rel": {
        "aggregate": {
          "input": {
            "read": {
              "base_schema": {
                "names": ["A", "B", "C"],
                "struct": {
                  "types": [{
                    "i32": {}
                  }, {
                    "i32": {}
                  }, {
                    "i32": {}
                  }]
                }
              },
              "local_files": {
                "items": [
                  {
                    "uri_file": "file:///tmp/dat.parquet",
                    "parquet": {}
                  }
                ]
              }
            }
          },
          "groupings": [{
            "groupingExpressions": [{
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                }
              }
            }]
          }],
          "measures": [{
            "measure": {
              "functionReference": 0,
              "arguments": [],
              "sorts": [],
              "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
              "invocation": "AGGREGATION_INVOCATION_ALL",
              "outputType": {
                "i64": {}
              }
            }
          }]
        }
      }
    }],
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "sum"
      }
    }],
  })",
                                                   /*ignore_unknown_fields=*/false));

  ASSERT_RAISES(Invalid, DeserializePlans(*buf, [] { return kNullConsumer; }));
}

TEST(Substrait, AggregateWithFilter) {
  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", R"({
    "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
    "relations": [{
      "rel": {
        "aggregate": {
          "input": {
            "read": {
              "base_schema": {
                "names": ["A", "B", "C"],
                "struct": {
                  "types": [{
                    "i32": {}
                  }, {
                    "i32": {}
                  }, {
                    "i32": {}
                  }]
                }
              },
              "local_files": {
                "items": [
                  {
                    "uri_file": "file:///tmp/dat.parquet",
                    "parquet": {}
                  }
                ]
              }
            }
          },
          "groupings": [{
            "groupingExpressions": [{
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                }
              }
            }]
          }],
          "measures": [{
            "measure": {
              "functionReference": 0,
              "arguments": [],
              "sorts": [],
              "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
              "invocation": "AGGREGATION_INVOCATION_ALL",
              "outputType": {
                "i64": {}
              }
            }
          }]
        }
      }
    }],
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/apache/arrow/blob/main/format/substrait/extension_types.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "equal"
      }
    }],
  })",
                                                   /*ignore_unknown_fields=*/false));

  ASSERT_RAISES(NotImplemented, DeserializePlans(*buf, [] { return kNullConsumer; }));
}

TEST(Substrait, AggregateBadPhase) {
  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", R"({
    "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
    "relations": [{
      "rel": {
        "aggregate": {
          "input": {
            "read": {
              "base_schema": {
                "names": ["A", "B", "C"],
                "struct": {
                  "types": [{
                    "i32": {}
                  }, {
                    "i32": {}
                  }, {
                    "i32": {}
                  }]
                }
              },
              "local_files": {
                "items": [
                  {
                    "uri_file": "file:///tmp/dat.parquet",
                    "parquet": {}
                  }
                ]
              }
            }
          },
          "groupings": [{
            "groupingExpressions": [{
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                }
              }
            }]
          }],
          "measures": [{
            "measure": {
              "functionReference": 0,
              "arguments": [],
              "sorts": [],
              "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
              "invocation": "AGGREGATION_INVOCATION_DISTINCT",
              "outputType": {
                "i64": {}
              }
            }
          }]
        }
      }
    }],
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/apache/arrow/blob/main/format/substrait/extension_types.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "equal"
      }
    }],
  })",
                                                   /*ignore_unknown_fields=*/false));

  ASSERT_RAISES(NotImplemented, DeserializePlans(*buf, [] { return kNullConsumer; }));
}

TEST(SubstraitRoundTrip, BasicPlan) {
  arrow::dataset::internal::Initialize();

  auto dummy_schema = schema(
      {field("key", int32()), field("shared", int32()), field("distinct", int32())});

  // creating a dummy dataset using a dummy table
  auto table = TableFromJSON(dummy_schema, {R"([
      [1, 1, 10],
      [3, 4, 20]
    ])",
                                            R"([
      [0, 2, 1],
      [1, 3, 2],
      [4, 1, 3],
      [3, 1, 3],
      [1, 2, 5]
    ])",
                                            R"([
      [2, 2, 12],
      [5, 3, 12],
      [1, 3, 12]
    ])"});

  auto format = std::make_shared<arrow::dataset::IpcFileFormat>();
  auto filesystem = std::make_shared<fs::LocalFileSystem>();
  const std::string file_name = "serde_test.arrow";

  ASSERT_OK_AND_ASSIGN(auto tempdir,
                       arrow::internal::TemporaryDir::Make("substrait-tempdir-"));
  ASSERT_OK_AND_ASSIGN(auto file_path, tempdir->path().Join(file_name));
  std::string file_path_str = file_path.ToString();

  WriteIpcData(file_path_str, filesystem, table);

  std::vector<fs::FileInfo> files;
  const std::vector<std::string> f_paths = {file_path_str};

  for (const auto& f_path : f_paths) {
    ASSERT_OK_AND_ASSIGN(auto f_file, filesystem->GetFileInfo(f_path));
    files.push_back(std::move(f_file));
  }

  ASSERT_OK_AND_ASSIGN(auto ds_factory, dataset::FileSystemDatasetFactory::Make(
                                            filesystem, std::move(files), format, {}));
  ASSERT_OK_AND_ASSIGN(auto dataset, ds_factory->Finish(dummy_schema));

  auto scan_options = std::make_shared<dataset::ScanOptions>();
  scan_options->projection = compute::project({}, {});
  const std::string filter_col_left = "shared";
  const std::string filter_col_right = "distinct";
  auto comp_left_value = compute::field_ref(filter_col_left);
  auto comp_right_value = compute::field_ref(filter_col_right);
  auto filter = compute::equal(comp_left_value, comp_right_value);

  arrow::AsyncGenerator<std::optional<compute::ExecBatch>> sink_gen;

  auto declarations = acero::Declaration::Sequence(
      {acero::Declaration({"scan", dataset::ScanNodeOptions{dataset, scan_options}, "s"}),
       acero::Declaration({"filter", acero::FilterNodeOptions{filter}, "f"}),
       acero::Declaration({"sink", acero::SinkNodeOptions{&sink_gen}, "e"})});

  std::shared_ptr<ExtensionIdRegistry> sp_ext_id_reg = MakeExtensionIdRegistry();
  ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
  ExtensionSet ext_set(ext_id_reg);

  ASSERT_OK_AND_ASSIGN(auto serialized_plan, SerializePlan(declarations, &ext_set));

  ASSERT_OK_AND_ASSIGN(
      auto sink_decls,
      DeserializePlans(
          *serialized_plan, [] { return kNullConsumer; }, ext_id_reg, &ext_set));
  // filter declaration
  const auto& roundtripped_filter = std::get<acero::Declaration>(sink_decls[0].inputs[0]);
  const auto& filter_opts =
      checked_cast<const acero::FilterNodeOptions&>(*(roundtripped_filter.options));
  auto roundtripped_expr = filter_opts.filter_expression;

  if (auto* call = roundtripped_expr.call()) {
    EXPECT_EQ(call->function_name, "equal");
    auto args = call->arguments;
    auto left_index = args[0].field_ref()->field_path()->indices()[0];
    EXPECT_EQ(dummy_schema->field_names()[left_index], filter_col_left);
    auto right_index = args[1].field_ref()->field_path()->indices()[0];
    EXPECT_EQ(dummy_schema->field_names()[right_index], filter_col_right);
  }
  // scan declaration
  const auto& roundtripped_scan =
      std::get<acero::Declaration>(roundtripped_filter.inputs[0]);
  const auto& dataset_opts =
      checked_cast<const dataset::ScanNodeOptions&>(*(roundtripped_scan.options));
  const auto& roundripped_ds = dataset_opts.dataset;
  EXPECT_TRUE(roundripped_ds->schema()->Equals(*dummy_schema));
  ASSERT_OK_AND_ASSIGN(auto roundtripped_frgs, roundripped_ds->GetFragments());
  ASSERT_OK_AND_ASSIGN(auto expected_frgs, dataset->GetFragments());

  auto roundtrip_frg_vec = IteratorToVector(std::move(roundtripped_frgs));
  auto expected_frg_vec = IteratorToVector(std::move(expected_frgs));
  EXPECT_EQ(expected_frg_vec.size(), roundtrip_frg_vec.size());
  int64_t idx = 0;
  for (auto fragment : expected_frg_vec) {
    const auto* l_frag = checked_cast<const dataset::FileFragment*>(fragment.get());
    const auto* r_frag =
        checked_cast<const dataset::FileFragment*>(roundtrip_frg_vec[idx++].get());
    EXPECT_TRUE(l_frag->Equals(*r_frag));
  }
}

TEST(SubstraitRoundTrip, BasicPlanEndToEnd) {
  compute::ExecContext exec_context;
  arrow::dataset::internal::Initialize();

  auto dummy_schema = schema(
      {field("key", int32()), field("shared", int32()), field("distinct", int32())});

  // creating a dummy dataset using a dummy table
  auto table = TableFromJSON(dummy_schema, {R"([
      [1, 1, 10],
      [3, 4, 4]
    ])",
                                            R"([
      [0, 2, 1],
      [1, 3, 2],
      [4, 1, 1],
      [3, 1, 3],
      [1, 2, 2]
    ])",
                                            R"([
      [2, 2, 12],
      [5, 3, 12],
      [1, 3, 3]
    ])"});

  auto format = std::make_shared<arrow::dataset::IpcFileFormat>();
  auto filesystem = std::make_shared<fs::LocalFileSystem>();
  const std::string file_name = "serde_test.arrow";

  ASSERT_OK_AND_ASSIGN(auto tempdir,
                       arrow::internal::TemporaryDir::Make("substrait-tempdir-"));
  ASSERT_OK_AND_ASSIGN(auto file_path, tempdir->path().Join(file_name));
  std::string file_path_str = file_path.ToString();

  WriteIpcData(file_path_str, filesystem, table);

  std::vector<fs::FileInfo> files;
  const std::vector<std::string> f_paths = {file_path_str};

  for (const auto& f_path : f_paths) {
    ASSERT_OK_AND_ASSIGN(auto f_file, filesystem->GetFileInfo(f_path));
    files.push_back(std::move(f_file));
  }

  ASSERT_OK_AND_ASSIGN(auto ds_factory, dataset::FileSystemDatasetFactory::Make(
                                            filesystem, std::move(files), format, {}));
  ASSERT_OK_AND_ASSIGN(auto dataset, ds_factory->Finish(dummy_schema));

  auto scan_options = std::make_shared<dataset::ScanOptions>();
  scan_options->projection = compute::project({}, {});
  const std::string filter_col_left = "shared";
  const std::string filter_col_right = "distinct";
  auto comp_left_value = compute::field_ref(filter_col_left);
  auto comp_right_value = compute::field_ref(filter_col_right);
  auto filter = compute::equal(comp_left_value, comp_right_value);

  auto declarations = acero::Declaration::Sequence(
      {acero::Declaration({"scan", dataset::ScanNodeOptions{dataset, scan_options}, "s"}),
       acero::Declaration({"filter", acero::FilterNodeOptions{filter}, "f"})});

  ASSERT_OK_AND_ASSIGN(auto expected_table, acero::DeclarationToTable(declarations));

  std::shared_ptr<ExtensionIdRegistry> sp_ext_id_reg = MakeExtensionIdRegistry();
  ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
  ExtensionSet ext_set(ext_id_reg);

  ASSERT_OK_AND_ASSIGN(auto serialized_plan, SerializePlan(declarations, &ext_set));

  ASSERT_OK_AND_ASSIGN(
      auto sink_decls,
      DeserializePlans(
          *serialized_plan, [] { return kNullConsumer; }, ext_id_reg, &ext_set));
  // filter declaration
  auto& roundtripped_filter = std::get<acero::Declaration>(sink_decls[0].inputs[0]);
  const auto& filter_opts =
      checked_cast<const acero::FilterNodeOptions&>(*(roundtripped_filter.options));
  auto roundtripped_expr = filter_opts.filter_expression;

  if (auto* call = roundtripped_expr.call()) {
    EXPECT_EQ(call->function_name, "equal");
    auto args = call->arguments;
    auto left_index = args[0].field_ref()->field_path()->indices()[0];
    EXPECT_EQ(dummy_schema->field_names()[left_index], filter_col_left);
    auto right_index = args[1].field_ref()->field_path()->indices()[0];
    EXPECT_EQ(dummy_schema->field_names()[right_index], filter_col_right);
  }
  // scan declaration
  const auto& roundtripped_scan =
      std::get<acero::Declaration>(roundtripped_filter.inputs[0]);
  const auto& dataset_opts =
      checked_cast<const dataset::ScanNodeOptions&>(*(roundtripped_scan.options));
  const auto& roundripped_ds = dataset_opts.dataset;
  EXPECT_TRUE(roundripped_ds->schema()->Equals(*dummy_schema));
  ASSERT_OK_AND_ASSIGN(auto roundtripped_frgs, roundripped_ds->GetFragments());
  ASSERT_OK_AND_ASSIGN(auto expected_frgs, dataset->GetFragments());

  auto roundtrip_frg_vec = IteratorToVector(std::move(roundtripped_frgs));
  auto expected_frg_vec = IteratorToVector(std::move(expected_frgs));
  EXPECT_EQ(expected_frg_vec.size(), roundtrip_frg_vec.size());
  int64_t idx = 0;
  for (auto fragment : expected_frg_vec) {
    const auto* l_frag = checked_cast<const dataset::FileFragment*>(fragment.get());
    const auto* r_frag =
        checked_cast<const dataset::FileFragment*>(roundtrip_frg_vec[idx++].get());
    EXPECT_TRUE(l_frag->Equals(*r_frag));
  }
  ASSERT_OK_AND_ASSIGN(auto rnd_trp_table,
                       acero::DeclarationToTable(roundtripped_filter));
  engine::AssertTablesEqualIgnoringOrder(expected_table, rnd_trp_table);
}

TEST(SubstraitRoundTrip, FilterNamedTable) {
  arrow::dataset::internal::Initialize();

  const std::vector<std::string> table_names{"table", "1"};
  const auto dummy_schema =
      schema({field("A", int32()), field("B", int32()), field("C", int32())});
  auto filter = compute::equal(compute::field_ref("A"), compute::field_ref("B"));

  auto declarations = acero::Declaration::Sequence(
      {acero::Declaration(
           {"named_table", acero::NamedTableNodeOptions{table_names, dummy_schema}, "n"}),
       acero::Declaration({"filter", acero::FilterNodeOptions{filter}, "f"})});

  ExtensionSet ext_set{};
  ASSERT_OK_AND_ASSIGN(auto serialized_plan, SerializePlan(declarations, &ext_set));

  // creating a dummy dataset using a dummy table
  auto input_table = TableFromJSON(dummy_schema, {R"([
      [1, 1, 10],
      [3, 5, 20],
      [4, 1, 30],
      [2, 1, 40],
      [5, 5, 50],
      [2, 2, 60]
  ])"});

  NamedTableProvider table_provider =
      [&input_table, &table_names, &dummy_schema](
          const std::vector<std::string>& names,
          const Schema& schema) -> Result<acero::Declaration> {
    if (table_names != names) {
      return Status::Invalid("Table name mismatch");
    }
    if (!dummy_schema->Equals(schema)) {
      return Status::Invalid("Table schema mismatch");
    }

    std::shared_ptr<acero::ExecNodeOptions> options =
        std::make_shared<acero::TableSourceNodeOptions>(input_table);
    return acero::Declaration("table_source", {}, std::move(options), "mock_source");
  };
  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);

  auto expected_table = TableFromJSON(dummy_schema, {R"([
    [1, 1, 10],
    [5, 5, 50],
    [2, 2, 60]
  ])"});

  CheckRoundTripResult(std::move(expected_table), serialized_plan,
                       /*include_columns=*/{}, conversion_options);
}

TEST(SubstraitRoundTrip, ProjectRel) {
  compute::ExecContext exec_context;
  auto dummy_schema =
      schema({field("A", int32()), field("B", int32()), field("C", int32())});

  // creating a dummy dataset using a dummy table
  auto input_table = TableFromJSON(dummy_schema, {R"([
      [1, 1, 10],
      [3, 5, 20],
      [4, 1, 30],
      [2, 1, 40],
      [5, 5, 50],
      [2, 2, 60]
  ])"});

  std::string substrait_json = R"({
  "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
  "relations": [{
    "rel": {
      "project": {
        "expressions": [{
          "scalarFunction": {
            "functionReference": 0,
            "arguments": [{
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 0
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }, {
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 1
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }],
            "output_type": {
              "bool": {}
            }
          }
        },
        ],
        "input" : {
          "read": {
            "base_schema": {
              "names": ["A", "B", "C"],
                "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "namedTable": {
              "names": ["A"]
            }
          }
        }
      }
    }
  }],
  "extension_uris": [
      {
        "extension_uri_anchor": 0,
        "uri": ")" + std::string(kSubstraitComparisonFunctionsUri) +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "equal"
      }}
    ]
  })";

  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));
  auto output_schema = schema({field("A", int32()), field("B", int32()),
                               field("C", int32()), field("equal", boolean())});
  auto expected_table = TableFromJSON(output_schema, {R"([
    [1, 1, 10, true],
    [3, 5, 20, false],
    [4, 1, 30, false],
    [2, 1, 40, false],
    [5, 5, 50, true],
    [2, 2, 60, true]
  ])"});

  NamedTableProvider table_provider = AlwaysProvideSameTable(std::move(input_table));

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);

  CheckRoundTripResult(std::move(expected_table), buf,
                       /*include_columns=*/{}, conversion_options);
}

TEST(SubstraitRoundTrip, ProjectRelOnFunctionWithEmit) {
  auto dummy_schema =
      schema({field("A", int32()), field("B", int32()), field("C", int32())});

  // creating a dummy dataset using a dummy table
  auto input_table = TableFromJSON(dummy_schema, {R"([
      [1, 1, 10],
      [3, 5, 20],
      [4, 1, 30],
      [2, 1, 40],
      [5, 5, 50],
      [2, 2, 60]
  ])"});

  std::string substrait_json = R"({
  "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
  "relations": [{
    "rel": {
      "project": {
        "common": {
          "emit": {
            "outputMapping": [0, 2, 3]
          }
        },
        "expressions": [{
          "scalarFunction": {
            "functionReference": 0,
            "arguments": [{
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 0
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }, {
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 1
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }],
            "output_type": {
              "bool": {}
            }
          }
        },
        ],
        "input" : {
          "read": {
            "base_schema": {
              "names": ["A", "B", "C"],
                "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "namedTable": {
              "names": ["A"]
            }
          }
        }
      }
    }
  }],
  "extension_uris": [
      {
        "extension_uri_anchor": 0,
        "uri": ")" + std::string(kSubstraitComparisonFunctionsUri) +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "equal"
      }}
    ]
  })";

  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));
  auto output_schema =
      schema({field("A", int32()), field("C", int32()), field("equal", boolean())});
  auto expected_table = TableFromJSON(output_schema, {R"([
      [1, 10, true],
      [3, 20, false],
      [4, 30, false],
      [2, 40, false],
      [5, 50, true],
      [2, 60, true]
  ])"});
  NamedTableProvider table_provider = AlwaysProvideSameTable(std::move(input_table));

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);
  ValidateNumProjectNodes(1, buf, conversion_options);
  CheckRoundTripResult(std::move(expected_table), buf,
                       /*include_columns=*/{}, conversion_options);
}

TEST(SubstraitRoundTrip, ProjectRelOnFunctionWithAllEmit) {
  compute::ExecContext exec_context;
  auto dummy_schema = schema({field("A", int32()), field("B", int32())});

  // creating a dummy dataset using a dummy table
  auto input_table = TableFromJSON(dummy_schema, {R"([
      [1, 1],
      [3, 5],
      [4, 1],
      [2, 1],
      [5, 5],
      [2, 2]
  ])"});

  std::string substrait_json = R"({
  "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
  "relations":[
      {
         "rel":{
            "project":{
               "common":{
                  "emit":{
                     "outputMapping":[
                        0,
                        1,
                        2,
                        3
                     ]
                  }
               },
               "expressions":[
                  {
                     "scalarFunction":{
                        "functionReference":0,
                        "arguments":[
                           {
                              "value":{
                                 "selection":{
                                    "directReference":{
                                       "structField":{
                                          "field":0
                                       }
                                    },
                                    "rootReference":{}
                                 }
                              }
                           },
                           {
                              "value":{
                                 "selection":{
                                    "directReference":{
                                       "structField":{
                                          "field":1
                                       }
                                    },
                                    "rootReference":{}
                                 }
                              }
                           }
                        ],
                        "output_type":{
                           "bool":{}
                        }
                     }
                  }
               ],
               "input":{
                  "project":{
                     "common":{
                        "emit":{
                           "outputMapping":[
                              0,
                              1,
                              2
                           ]
                        }
                     },
                     "expressions":[
                        {
                           "scalarFunction":{
                              "functionReference":0,
                              "arguments":[
                                 {
                                    "value":{
                                       "selection":{
                                          "directReference":{
                                             "structField":{
                                                "field":0
                                             }
                                          },
                                          "rootReference":{}
                                       }
                                    }
                                 },
                                 {
                                    "value":{
                                       "selection":{
                                          "directReference":{
                                             "structField":{
                                                "field":1
                                             }
                                          },
                                          "rootReference":{}
                                       }
                                    }
                                 }
                              ],
                              "output_type":{
                                 "bool":{}
                              }
                           }
                        }
                     ],
                     "input":{
                        "read":{
                           "base_schema":{
                              "names":[
                                 "A",
                                 "B"
                              ],
                              "struct":{
                                 "types":[
                                    {
                                       "i32":{}
                                    },
                                    {
                                       "i32":{}
                                    }
                                 ]
                              }
                           },
                           "namedTable":{
                              "names":[
                                 "TABLE"
                              ]
                           }
                        }
                     }
                  }
               }
            }
         }
      }
   ],
  "extension_uris": [
      {
        "extension_uri_anchor": 0,
        "uri": ")" + std::string(kSubstraitComparisonFunctionsUri) +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "equal"
      }}
    ]
  })";

  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));
  auto output_schema = schema({field("A", int32()), field("B", int32()),
                               field("eq1", boolean()), field("eq2", boolean())});
  auto expected_table = TableFromJSON(output_schema, {R"([
      [1, 1, true, true],
      [3, 5, false, false],
      [4, 1, false, false],
      [2, 1, false, false],
      [5, 5, true, true],
      [2, 2, true, true]
  ])"});
  NamedTableProvider table_provider = AlwaysProvideSameTable(std::move(input_table));

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);

  ValidateNumProjectNodes(2, buf, conversion_options);

  CheckRoundTripResult(std::move(expected_table), buf,
                       /*include_columns=*/{}, conversion_options);
}

TEST(SubstraitRoundTrip, ReadRelWithEmit) {
  auto dummy_schema =
      schema({field("A", int32()), field("B", int32()), field("C", int32())});

  // creating a dummy dataset using a dummy table
  auto input_table = TableFromJSON(dummy_schema, {R"([
      [1, 1, 10],
      [3, 4, 20]
  ])"});

  std::string substrait_json = R"({
  "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
  "relations": [{
    "rel": {
      "read": {
        "common": {
          "emit": {
            "outputMapping": [1, 2]
          }
        },
        "base_schema": {
          "names": ["A", "B", "C"],
            "struct": {
            "types": [{
              "i32": {}
            }, {
              "i32": {}
            }, {
              "i32": {}
            }]
          }
        },
        "namedTable": {
          "names" : ["A"]
        }
      }
    }
  }],
  })";

  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));
  auto output_schema = schema({field("B", int32()), field("C", int32())});
  auto expected_table = TableFromJSON(output_schema, {R"([
      [1, 10],
      [4, 20]
  ])"});

  NamedTableProvider table_provider = AlwaysProvideSameTable(std::move(input_table));

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);
  ValidateNumProjectNodes(1, buf, conversion_options);
  CheckRoundTripResult(std::move(expected_table), buf,
                       /*include_columns=*/{}, conversion_options);
}

TEST(SubstraitRoundTrip, FilterRelWithEmit) {
  auto dummy_schema = schema({field("A", int32()), field("B", int32()),
                              field("C", int32()), field("D", int32())});

  // creating a dummy dataset using a dummy table
  auto input_table = TableFromJSON(dummy_schema, {R"([
      [10, 1, 80, 7],
      [20, 2, 70, 6],
      [30, 3, 30, 5],
      [40, 4, 20, 4],
      [40, 5, 40, 3],
      [20, 6, 20, 2],
      [30, 7, 30, 1]
  ])"});

  std::string substrait_json = R"({
  "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
  "relations": [{
    "rel": {
      "filter": {
        "common": {
          "emit": {
            "outputMapping": [1, 3]
          }
        },
        "condition": {
          "scalarFunction": {
            "functionReference": 0,
            "arguments": [{
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 0
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }, {
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 2
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }],
            "output_type": {
              "bool": {}
            }
          }
        },
        "input" : {
          "read": {
            "base_schema": {
              "names": ["A", "B", "C", "D"],
                "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                },{
                  "i32": {}
                }]
              }
            },
            "namedTable": {
              "names" : ["A"]
            }
          }
        }
      }
    }
  }],
  "extension_uris": [
      {
        "extension_uri_anchor": 0,
        "uri": ")" + std::string(kSubstraitComparisonFunctionsUri) +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "equal"
      }}
    ]
  })";

  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));
  auto output_schema = schema({field("B", int32()), field("D", int32())});
  auto expected_table = TableFromJSON(output_schema, {R"([
      [3, 5],
      [5, 3],
      [6, 2],
      [7, 1]
  ])"});
  NamedTableProvider table_provider = AlwaysProvideSameTable(std::move(input_table));

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);
  ValidateNumProjectNodes(1, buf, conversion_options);
  CheckRoundTripResult(std::move(expected_table), buf,
                       /*include_columns=*/{}, conversion_options);
}

TEST(SubstraitRoundTrip, JoinRel) {
  auto left_schema = schema({field("A", int32()), field("B", int32())});

  auto right_schema = schema({field("X", int32()), field("Y", int32())});

  // creating a dummy dataset using a dummy table
  auto left_table = TableFromJSON(left_schema, {R"([
      [10, 1],
      [20, 2],
      [30, 3]
  ])"});

  auto right_table = TableFromJSON(right_schema, {R"([
      [10, 11],
      [80, 21],
      [31, 31]
  ])"});

  std::string substrait_json = R"({
  "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
  "relations": [{
    "rel": {
      "join": {
        "left": {
          "read": {
            "base_schema": {
              "names": ["A", "B"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "namedTable": {
              "names" : ["left"]
            }
          }
        },
        "right": {
          "read": {
            "base_schema": {
              "names": ["X", "Y"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "namedTable": {
              "names" : ["right"]
            }
          }
        },
        "expression": {
          "scalarFunction": {
            "functionReference": 0,
            "arguments": [{
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 0
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }, {
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 2
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }],
            "output_type": {
              "bool": {}
            }
          }
        },
        "type": "JOIN_TYPE_INNER"
      }
    }
  }],
  "extension_uris": [
      {
        "extension_uri_anchor": 0,
        "uri": ")" + std::string(kSubstraitComparisonFunctionsUri) +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "equal"
      }}
    ]
  })";

  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));

  // include these columns for comparison
  auto output_schema = schema({
      field("A", int32()),
      field("B", int32()),
      field("X", int32()),
      field("Y", int32()),
  });

  auto expected_table = TableFromJSON(std::move(output_schema), {R"([
      [10, 1, 10, 11]
  ])"});

  NamedTableProvider table_provider =
      [left_table, right_table](const std::vector<std::string>& names, const Schema&) {
        std::shared_ptr<Table> output_table;
        for (const auto& name : names) {
          if (name == "left") {
            output_table = left_table;
          }
          if (name == "right") {
            output_table = right_table;
          }
        }
        std::shared_ptr<acero::ExecNodeOptions> options =
            std::make_shared<acero::TableSourceNodeOptions>(std::move(output_table));
        return acero::Declaration("table_source", {}, options, "mock_source");
      };

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);

  CheckRoundTripResult(std::move(expected_table), buf,
                       /*include_columns=*/{}, conversion_options);
}

TEST(SubstraitRoundTrip, JoinRelWithEmit) {
  auto left_schema = schema({field("A", int32()), field("B", int32())});

  auto right_schema = schema({field("X", int32()), field("Y", int32())});

  // creating a dummy dataset using a dummy table
  auto left_table = TableFromJSON(left_schema, {R"([
      [10, 1],
      [20, 2],
      [30, 3]
  ])"});

  auto right_table = TableFromJSON(right_schema, {R"([
      [10, 11],
      [80, 21],
      [31, 31]
  ])"});

  std::string substrait_json = R"({
  "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
  "relations": [{
    "rel": {
      "join": {
        "common": {
          "emit": {
            "outputMapping": [0, 1, 3]
          }
        },
        "left": {
          "read": {
            "base_schema": {
              "names": ["A", "B"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "namedTable" : {
              "names" : ["left"]
            }
          }
        },
        "right": {
          "read": {
            "base_schema": {
              "names": ["X", "Y"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "namedTable" : {
              "names" : ["right"]
            }
          }
        },
        "expression": {
          "scalarFunction": {
            "functionReference": 0,
            "arguments": [{
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 0
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }, {
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 2
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }],
            "output_type": {
              "bool": {}
            }
          }
        },
        "type": "JOIN_TYPE_INNER"
      }
    }
  }],
  "extension_uris": [
      {
        "extension_uri_anchor": 0,
        "uri": ")" + std::string(kSubstraitComparisonFunctionsUri) +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "equal"
      }}
    ]
  })";

  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));
  auto output_schema = schema({
      field("A", int32()),
      field("B", int32()),
      field("Y", int32()),
  });

  auto expected_table = TableFromJSON(std::move(output_schema), {R"([
      [10, 1, 11]
  ])"});

  NamedTableProvider table_provider =
      [left_table, right_table](const std::vector<std::string>& names, const Schema&) {
        std::shared_ptr<Table> output_table;
        for (const auto& name : names) {
          if (name == "left") {
            output_table = left_table;
          }
          if (name == "right") {
            output_table = right_table;
          }
        }
        std::shared_ptr<acero::ExecNodeOptions> options =
            std::make_shared<acero::TableSourceNodeOptions>(std::move(output_table));
        return acero::Declaration("table_source", {}, options, "mock_source");
      };

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);
  ValidateNumProjectNodes(1, buf, conversion_options);
  CheckRoundTripResult(std::move(expected_table), buf,
                       /*include_columns=*/{}, conversion_options);
}

TEST(SubstraitRoundTrip, AggregateRel) {
  auto dummy_schema =
      schema({field("A", int32()), field("B", int32()), field("C", int32())});

  // creating a dummy dataset using a dummy table
  auto input_table = TableFromJSON(dummy_schema, {R"([
      [10, 1, 80],
      [20, 2, 70],
      [30, 3, 30],
      [40, 4, 20],
      [40, 5, 40],
      [20, 6, 20],
      [30, 7, 30]
  ])"});

  std::string substrait_json = R"({
    "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
    "relations": [{
      "rel": {
        "aggregate": {
          "input": {
            "read": {
              "base_schema": {
                "names": ["A", "B", "C"],
                "struct": {
                  "types": [{
                    "i32": {}
                  }, {
                    "i32": {}
                  }, {
                    "i32": {}
                  }]
                }
              },
              "namedTable" : {
                "names": ["A"]
              }
            }
          },
          "groupings": [{
            "groupingExpressions": [{
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                }
              }
            }]
          }],
          "measures": [{
            "measure": {
              "functionReference": 0,
              "arguments": [{
                "value": {
                  "selection": {
                    "directReference": {
                      "structField": {
                        "field": 2
                      }
                    }
                  }
                }
            }],
              "sorts": [],
              "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
              "invocation": "AGGREGATION_INVOCATION_ALL",
              "outputType": {
                "i64": {}
              }
            }
          }]
        }
      }
    }],
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "sum"
      }
    }],
  })";

  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));
  auto output_schema = schema({field("keys", int32()), field("aggregates", int64())});
  auto expected_table = TableFromJSON(output_schema, {R"([
      [10, 80],
      [20, 90],
      [30, 60],
      [40, 60]
  ])"});

  NamedTableProvider table_provider = AlwaysProvideSameTable(std::move(input_table));

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);

  CheckRoundTripResult(std::move(expected_table), buf,
                       /*include_columns=*/{}, conversion_options);
}

TEST(SubstraitRoundTrip, AggregateRelOptions) {
  auto dummy_schema =
      schema({field("A", int32()), field("B", int32()), field("C", int32())});

  // creating a dummy dataset using a dummy table
  auto input_table = TableFromJSON(dummy_schema, {R"([
      [10, 1, 10],
      [20, 2, 10],
      [30, 3, 20],
      [30, 1, 20],
      [20, 2, 30],
      [10, 3, 30]
  ])"});

  std::string substrait_json = R"({
    "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
    "relations": [{
      "rel": {
        "aggregate": {
          "input": {
            "read": {
              "base_schema": {
                "names": ["A", "B", "C"],
                "struct": {
                  "types": [{
                    "i32": {}
                  }, {
                    "i32": {}
                  }, {
                    "i32": {}
                  }]
                }
              },
              "namedTable" : {
                "names": ["A"]
              }
            }
          },
          "groupings": [{
            "groupingExpressions": [{
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                }
              }
            }]
          }],
          "measures": [{
            "measure": {
              "functionReference": 0,
              "arguments": [{
                "value": {
                  "selection": {
                    "directReference": {
                      "structField": {
                        "field": 2
                      }
                    }
                  }
                }
            }],
              "options": [{
                "name": "distribution",
                "preference": [
                  "POPULATION"
                ]
              }],
              "sorts": [],
              "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
              "invocation": "AGGREGATION_INVOCATION_ALL",
              "outputType": {
                "i64": {}
              }
            }
          }]
        }
      }
    }],
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "variance"
      }
    }],
  })";

  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));
  auto output_schema = schema({field("keys", int32()), field("aggregates", float64())});
  auto expected_table = TableFromJSON(output_schema, {R"([
      [10, 200],
      [20, 200],
      [30, 0]
  ])"});

  NamedTableProvider table_provider = AlwaysProvideSameTable(std::move(input_table));

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);

  CheckRoundTripResult(std::move(expected_table), buf,
                       /*include_columns=*/{}, conversion_options);
}

TEST(SubstraitRoundTrip, AggregateRelEmit) {
  auto dummy_schema =
      schema({field("A", int32()), field("B", int32()), field("C", int32())});

  // creating a dummy dataset using a dummy table
  auto input_table = TableFromJSON(dummy_schema, {R"([
      [10, 1, 80],
      [20, 2, 70],
      [30, 3, 30],
      [40, 4, 20],
      [40, 5, 40],
      [20, 6, 20],
      [30, 7, 30]
  ])"});

  // TODO: fixme https://issues.apache.org/jira/browse/ARROW-17484
  std::string substrait_json = R"({
    "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
    "relations": [{
      "rel": {
        "aggregate": {
          "common": {
          "emit": {
            "outputMapping": [1]
          }
        },
          "input": {
            "read": {
              "base_schema": {
                "names": ["A", "B", "C"],
                "struct": {
                  "types": [{
                    "i32": {}
                  }, {
                    "i32": {}
                  }, {
                    "i32": {}
                  }]
                }
              },
              "namedTable" : {
                "names" : ["A"]
              }
            }
          },
          "groupings": [{
            "groupingExpressions": [{
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                }
              }
            }]
          }],
          "measures": [{
            "measure": {
              "functionReference": 0,
              "arguments": [{
                "value": {
                  "selection": {
                    "directReference": {
                      "structField": {
                        "field": 2
                      }
                    }
                  }
                }
            }],
              "sorts": [],
              "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
              "invocation": "AGGREGATION_INVOCATION_ALL",
              "outputType": {
                "i64": {}
              }
            }
          }]
        }
      }
    }],
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "sum"
      }
    }],
  })";

  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));
  auto output_schema = schema({field("aggregates", int64())});
  auto expected_table = TableFromJSON(output_schema, {R"([
      [80],
      [90],
      [60],
      [60]
  ])"});

  NamedTableProvider table_provider = AlwaysProvideSameTable(std::move(input_table));

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);
  ValidateNumProjectNodes(1, buf, conversion_options);
  CheckRoundTripResult(std::move(expected_table), buf,
                       /*include_columns=*/{}, conversion_options);
}

TEST(Substrait, IsthmusPlan) {
  // This is a plan generated from Isthmus
  // isthmus -c "CREATE TABLE T1(foo int)" "SELECT foo + 1 FROM T1"
  std::string substrait_json = R"({
    "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
    "extensionUris": [{
      "extensionUriAnchor": 1,
      "uri": "/functions_arithmetic.yaml"
    }],
    "extensions": [{
      "extensionFunction": {
        "extensionUriReference": 1,
        "functionAnchor": 0,
        "name": "add:i32_i32"
      }
    }],
    "relations": [{
      "root": {
        "input": {
          "project": {
            "common": {
              "emit": {
                "outputMapping": [1]
              }
            },
            "input": {
              "read": {
                "common": {
                  "direct": {
                  }
                },
                "baseSchema": {
                  "names": ["FOO"],
                  "struct": {
                    "types": [{
                      "i32": {
                        "typeVariationReference": 0,
                        "nullability": "NULLABILITY_NULLABLE"
                      }
                    }],
                    "typeVariationReference": 0,
                    "nullability": "NULLABILITY_REQUIRED"
                  }
                },
                "namedTable": {
                  "names": ["T1"]
                }
              }
            },
            "expressions": [{
              "scalarFunction": {
                "functionReference": 0,
                "outputType": {
                  "i32": {
                    "typeVariationReference": 0,
                    "nullability": "NULLABILITY_NULLABLE"
                  }
                },
                "arguments": [{
                  "value": {
                    "selection": {
                      "directReference": {
                        "structField": {
                          "field": 0
                        }
                      },
                      "rootReference": {
                      }
                    }
                  }
                }, {
                  "value": {
                    "literal": {
                      "i32": 1,
                      "nullable": false,
                      "typeVariationReference": 0
                    }
                  }
                }]
              }
            }]
          }
        },
        "names": ["EXPR$0"]
      }
    }],
    "expectedTypeUrls": []
  })";

  auto test_schema = schema({field("foo", int32())});
  auto input_table = TableFromJSON(test_schema, {"[[1], [2], [5]]"});
  NamedTableProvider table_provider = AlwaysProvideSameTable(std::move(input_table));
  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);

  ASSERT_OK_AND_ASSIGN(auto buf,
                       internal::SubstraitFromJSON("Plan", substrait_json,
                                                   /*ignore_unknown_fields=*/false));
  ValidateNumProjectNodes(1, buf, conversion_options);
  auto expected_table = TableFromJSON(test_schema, {"[[2], [3], [6]]"});
  CheckRoundTripResult(std::move(expected_table), buf,
                       /*include_columns=*/{}, conversion_options);
}

NamedTableProvider ProvideMadeTable(
    std::function<Result<std::shared_ptr<Table>>(const std::vector<std::string>&)> make) {
  return [make](const std::vector<std::string>& names,
                const Schema&) -> Result<acero::Declaration> {
    ARROW_ASSIGN_OR_RAISE(auto table, make(names));
    std::shared_ptr<acero::ExecNodeOptions> options =
        std::make_shared<acero::TableSourceNodeOptions>(table);
    return acero::Declaration("table_source", {}, options, "mock_source");
  };
}

TEST(Substrait, ProjectWithMultiFieldExpressions) {
  auto dummy_schema =
      schema({field("A", int32()), field("B", int32()), field("C", int32())});

  // creating a dummy dataset using a dummy table
  auto input_table = TableFromJSON(dummy_schema, {R"([
      [10, 1, 80],
      [20, 2, 70],
      [30, 3, 30]
  ])"});

  const std::string substrait_json = R"({
    "extensionUris": [{
        "extensionUriAnchor": 1,
        "uri": "/functions_arithmetic.yaml"
    }],
      "extensions": [{
        "extensionFunction": {
          "extensionUriReference": 1,
          "functionAnchor": 0,
          "name": "add:i32_i32"
        }
    }],
    "relations": [{
      "root": {
        "input": {
          "project": {
            "common": {
              "emit": {
                "outputMapping": [0, 3, 6]
              }
            },
            "input": {
              "read": {
                "common": {
                  "direct": {
                  }
                },
                "baseSchema": {
                  "names": ["A", "B", "C"],
                  "struct": {
                    "types": [{
                      "i32": {
                        "typeVariationReference": 0,
                        "nullability": "NULLABILITY_REQUIRED"
                      }
                    }, {
                      "i32": {
                        "typeVariationReference": 0,
                        "nullability": "NULLABILITY_REQUIRED"
                      }
                    }, {
                      "i32": {
                        "typeVariationReference": 0,
                        "nullability": "NULLABILITY_REQUIRED"
                      }
                    }],
                    "typeVariationReference": 0,
                    "nullability": "NULLABILITY_REQUIRED"
                  }
                },
                "namedTable": {
                  "names": ["SIMPLEDATA"]
                }
              }
            },
            "expressions": [{
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                },
                "rootReference": {
                }
              }
            }, {
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 1
                  }
                },
                "rootReference": {
                }
              }
            }, {
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 2
                  }
                },
                "rootReference": {
                }
              }
            },{
              "scalarFunction": {
                "functionReference": 0,
                "outputType": {
                  "i32": {
                    "typeVariationReference": 0,
                    "nullability": "NULLABILITY_NULLABLE"
                  }
                },
                "arguments": [{
                  "value": {
                    "selection": {
                      "directReference": {
                        "structField": {
                          "field": 0
                        }
                      },
                      "rootReference": {
                      }
                    }
                  }
                }, {
                  "value": {
                    "literal": {
                      "i32": 1,
                      "nullable": false,
                      "typeVariationReference": 0
                    }
                  }
                }]
              }
            }]
          }
        },
        "names": ["A", "B", "C"]
      }
    }]
  })";

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));
  auto output_schema =
      schema({field("A", int32()), field("A1", int32()), field("A+1", int32())});
  auto expected_table = TableFromJSON(output_schema, {R"([
      [10, 10, 11],
      [20, 20, 21],
      [30, 30, 31]
  ])"});

  NamedTableProvider table_provider = AlwaysProvideSameTable(std::move(input_table));

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);
  ValidateNumProjectNodes(1, buf, conversion_options);
  CheckRoundTripResult(std::move(expected_table), buf,
                       /*include_columns=*/{}, conversion_options);
}

TEST(Substrait, NestedProjectWithMultiFieldExpressions) {
  auto dummy_schema = schema({field("A", int32())});

  // creating a dummy dataset using a dummy table
  auto input_table = TableFromJSON(dummy_schema, {R"([
      [10],
      [20],
      [30]
  ])"});

  const std::string substrait_json = R"({
  "extensionUris": [
    {
      "extensionUriAnchor": 1,
      "uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
    }
  ],
  "extensions": [
    {
      "extensionFunction": {
        "extensionUriReference": 1,
        "functionAnchor": 2,
        "name": "add"
      }
    }
  ],
  "relations": [
    {
      "rel": {
        "project": {
          "input": {
            "project": {
              "common": {"emit": {"outputMapping": [2]}},
              "input": {
                "read": {
                  "baseSchema": {
                    "names": ["int"],
                    "struct": {"types": [{"i32": {}}]}
                  },
                  "namedTable": {
                    "names": ["SIMPLEDATA"]
                  }
                }
              },
              "expressions": [
                {"selection": {"directReference": {"structField": {"field": 0}}}},
                {
                  "scalarFunction": {
                    "functionReference": 2,
                    "outputType": {"i32": {}},
                    "arguments": [
                      {"value": {"selection": {"directReference": {"structField": {"field": 0}}}}},
                      {"value": {"literal": {"fp64": 10}}}
                    ]
                  }
                }
              ]
            }
          },
          "expressions": [
            {"selection": {"directReference": {"structField": {"field": 0}}}}
          ]
        }
      }
    }
  ]
})";

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));

  auto output_schema = schema({field("A", float32()), field("B", float32())});
  auto expected_table = TableFromJSON(output_schema, {R"([
      [20, 20],
      [30, 30],
      [40, 40]
  ])"});

  NamedTableProvider table_provider = AlwaysProvideSameTable(std::move(input_table));

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);
  ValidateNumProjectNodes(2, buf, conversion_options);
  CheckRoundTripResult(std::move(expected_table), buf,
                       /*include_columns=*/{}, conversion_options);
}

TEST(Substrait, NestedEmitProjectWithMultiFieldExpressions) {
  auto dummy_schema = schema({field("A", int32())});

  // creating a dummy dataset using a dummy table
  auto input_table = TableFromJSON(dummy_schema, {R"([
      [10],
      [20],
      [30]
  ])"});

  const std::string substrait_json = R"({
  "extensionUris": [
    {
      "extensionUriAnchor": 1,
      "uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
    }
  ],
  "extensions": [
    {
      "extensionFunction": {
        "extensionUriReference": 1,
        "functionAnchor": 2,
        "name": "add"
      }
    }
  ],
  "relations": [
    {
      "rel": {
        "project": {
          "common": {"emit": {"outputMapping": [2]}},
          "input": {
            "project": {
              "common": {"emit": {"outputMapping": [1, 2]}},
              "input": {
                "read": {
                  "baseSchema": {
                    "names": ["int"],
                    "struct": {"types": [{"i32": {}}]}
                  },
                  "namedTable": {
                    "names": ["SIMPLEDATA"]
                  }
                }
              },
              "expressions": [
                {"selection": {"directReference": {"structField": {"field": 0}}}},
                {
                  "scalarFunction": {
                    "functionReference": 2,
                    "outputType": {"i32": {}},
                    "arguments": [
                      {"value": {"selection": {"directReference": {"structField": {"field": 0}}}}},
                      {"value": {"literal": {"fp64": 10}}}
                    ]
                  }
                }
              ]
            }
          },
          "expressions": [
            {"selection": {"directReference": {"structField": {"field": 0}}}}
          ]
        }
      }
    }
  ]
})";

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));

  auto output_schema = schema({field("A", int32())});
  auto expected_table = TableFromJSON(output_schema, {R"([
      [10],
      [20],
      [30]
  ])"});

  NamedTableProvider table_provider = AlwaysProvideSameTable(std::move(input_table));

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);
  ValidateNumProjectNodes(2, buf, conversion_options);
  CheckRoundTripResult(std::move(expected_table), buf,
                       /*include_columns=*/{}, conversion_options);
}

TEST(Substrait, ReadRelWithGlobFiles) {
#ifdef _WIN32
  GTEST_SKIP() << "GH-33861: Substrait Glob Files URI not supported for Windows";
#endif
  arrow::dataset::internal::Initialize();

  auto dummy_schema =
      schema({field("A", int32()), field("B", int32()), field("C", int32())});

  // creating a dummy dataset using a dummy table
  auto table_1 = TableFromJSON(dummy_schema, {R"([
      [1, 1, 10],
      [3, 4, 20]
    ])"});
  auto table_2 = TableFromJSON(dummy_schema, {R"([
      [11, 11, 110],
      [13, 14, 120]
    ])"});
  auto table_3 = TableFromJSON(dummy_schema, {R"([
      [21, 21, 210],
      [23, 24, 220]
    ])"});
  auto expected_table = TableFromJSON(dummy_schema, {R"([
      [1, 1, 10],
      [3, 4, 20],
      [11, 11, 110],
      [13, 14, 120],
      [21, 21, 210],
      [23, 24, 220]
    ])"});

  std::vector<std::shared_ptr<Table>> input_tables = {table_1, table_2, table_3};
  auto format = std::make_shared<arrow::dataset::IpcFileFormat>();
  auto filesystem = std::make_shared<fs::LocalFileSystem>();
  const std::vector<std::string> file_names = {"serde_test_1.arrow", "serde_test_2.arrow",
                                               "serde_test_3.arrow"};

  const std::string path_prefix = "substrait-globfiles-";
  int idx = 0;

  // creating a vector to avoid out-of-scoping Temporary directory
  // if out-of-scoped the written folder get wiped out
  std::vector<std::unique_ptr<arrow::internal::TemporaryDir>> tempdirs;
  for (size_t i = 0; i < file_names.size(); i++) {
    ASSERT_OK_AND_ASSIGN(auto tempdir, arrow::internal::TemporaryDir::Make(path_prefix));
    tempdirs.push_back(std::move(tempdir));
  }

  std::string sample_tempdir_path = tempdirs[0]->path().ToString();
  std::string base_tempdir_path =
      sample_tempdir_path.substr(0, sample_tempdir_path.find(path_prefix));
  std::string glob_like_path =
      "file://" + base_tempdir_path + path_prefix + "*/serde_test_*.arrow";

  for (const auto& file_name : file_names) {
    ASSERT_OK_AND_ASSIGN(auto file_path, tempdirs[idx]->path().Join(file_name));
    std::string file_path_str = file_path.ToString();
    WriteIpcData(file_path_str, filesystem, input_tables[idx++]);
  }

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", R"({
    "relations": [{
      "rel": {
        "read": {
          "base_schema": {
            "names": ["A", "B", "C"],
            "struct": {
              "types": [{
                "i32": {}
              }, {
                "i32": {}
              }, {
                "i32": {}
              }]
            }
          },
          "local_files": {
            "items": [
              {
                "uri_path_glob": ")" + glob_like_path +
                                                                         R"(",
                "arrow": {}
              }
            ]
          }
        }
      }
    }]
  })"));
  // To avoid unnecessar metadata columns being included in the final result
  std::vector<int> include_columns = {0, 1, 2};
  compute::SortOptions options({compute::SortKey("A", compute::SortOrder::Ascending)});
  CheckRoundTripResult(std::move(expected_table), buf, std::move(include_columns),
                       /*conversion_options=*/{}, &options);
}

TEST(Substrait, RootRelationOutputNames) {
  auto dummy_schema =
      schema({field("A", int32()), field("B", int32()), field("C", int32())});

  // creating a dummy dataset using a dummy table
  const std::vector<std::string> str_data_vec = {
      R"([
      [10, 1, 80],
      [20, 2, 70],
      [30, 3, 30]
  ])"};
  auto input_table = TableFromJSON(dummy_schema, str_data_vec);

  const std::string substrait_json = R"({
    "relations": [{
      "root": {
        "input": {
          "project": {
            "common": {
              "emit": {
                "outputMapping": [3, 4, 5]
              }
            },
            "input": {
              "read": {
                "common": {
                  "direct": {
                  }
                },
                "baseSchema": {
                  "names": ["A", "B", "C"],
                  "struct": {
                    "types": [{
                      "i32": {
                        "typeVariationReference": 0,
                        "nullability": "NULLABILITY_REQUIRED"
                      }
                    }, {
                      "i32": {
                        "typeVariationReference": 0,
                        "nullability": "NULLABILITY_REQUIRED"
                      }
                    }, {
                      "i32": {
                        "typeVariationReference": 0,
                        "nullability": "NULLABILITY_REQUIRED"
                      }
                    }],
                    "typeVariationReference": 0,
                    "nullability": "NULLABILITY_REQUIRED"
                  }
                },
                "namedTable": {
                  "names": ["SIMPLEDATA"]
                }
              }
            },
            "expressions": [{
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                },
                "rootReference": {
                }
              }
            }, {
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 1
                  }
                },
                "rootReference": {
                }
              }
            }, {
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 2
                  }
                },
                "rootReference": {
                }
              }
            }]
          }
        },
        "names": ["X", "Y", "Z"]
      }
    }]
  })";

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));
  auto output_schema =
      schema({field("X", int32()), field("Y", int32()), field("Z", int32())});
  auto expected_table = TableFromJSON(output_schema, str_data_vec);

  NamedTableProvider table_provider = AlwaysProvideSameTable(std::move(input_table));

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);
  ValidateNumProjectNodes(1, buf, conversion_options);
  CheckRoundTripResult(std::move(expected_table), buf,
                       /*include_columns=*/{}, conversion_options);
}

TEST(Substrait, SetRelationBasic) {
  auto dummy_schema =
      schema({field("A", int32()), field("B", int32()), field("C", int32())});

  // creating a dummy dataset using a dummy table
  auto table1 = TableFromJSON(dummy_schema, {R"([
      [10, 1, 80],
      [20, 2, 70],
      [30, 3, 30],
      [40, 4, 20],
      [50, 6, 20],
      [200, 7, 30]
  ])"});

  auto table2 = TableFromJSON(dummy_schema, {R"([
      [70, 1, 82],
      [80, 2, 72],
      [90, 3, 32],
      [100, 4, 22],
      [110, 5, 42],
      [111, 6, 22],
      [112, 7, 32]
  ])"});

  NamedTableProvider table_provider =
      [table1, table2](const std::vector<std::string>& names, const Schema&) {
        std::shared_ptr<Table> output_table;
        for (const auto& name : names) {
          if (name == "T1") {
            output_table = table1;
          }
          if (name == "T2") {
            output_table = table2;
          }
        }
        std::shared_ptr<acero::ExecNodeOptions> options =
            std::make_shared<acero::TableSourceNodeOptions>(std::move(output_table));
        return acero::Declaration("table_source", {}, options, "mock_source");
      };

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);

  std::string substrait_json = R"({
    "relations": [{
      "root": {
        "input": {
          "set": {
            "inputs": [{
              "read": {
                "baseSchema": {
                  "names": ["FOO"],
                  "struct": {
                    "types": [{
                      "i32": {
                        "typeVariationReference": 0,
                        "nullability": "NULLABILITY_NULLABLE"
                      }
                    }],
                    "typeVariationReference": 0,
                    "nullability": "NULLABILITY_REQUIRED"
                  }
                },
                "namedTable": {
                  "names": ["T1"]
                }
              }
            }, {
              "read": {
                "baseSchema": {
                  "names": ["BAR"],
                  "struct": {
                    "types": [{
                      "i32": {
                        "typeVariationReference": 0,
                        "nullability": "NULLABILITY_NULLABLE"
                      }
                    }],
                    "typeVariationReference": 0,
                    "nullability": "NULLABILITY_REQUIRED"
                  }
                },
                "namedTable": {
                  "names": ["T2"]
                }
              }
            }],
            "op": "SET_OP_UNION_ALL"
          }
        },
        "names": ["FOO"]
      }
    }]
  })";

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));

  auto expected_table = TableFromJSON(dummy_schema, {R"([
      [10, 1, 80],
      [20, 2, 70],
      [30, 3, 30],
      [40, 4, 20],
      [50, 6, 20],
      [70, 1, 82],
      [80, 2, 72],
      [90, 3, 32],
      [100, 4, 22],
      [110, 5, 42],
      [111, 6, 22],
      [112, 7, 32],
      [200, 7, 30]
  ])"});

  compute::SortOptions sort_options(
      {compute::SortKey("A", compute::SortOrder::Ascending)});
  CheckRoundTripResult(std::move(expected_table), buf, {}, conversion_options,
                       &sort_options);
}

TEST(Substrait, PlanWithAsOfJoinExtension) {
  // This demos an extension relation
  std::string substrait_json = R"({
    "extensionUris": [],
    "extensions": [],
    "relations": [{
      "root": {
        "input": {
          "extension_multi": {
            "common": {
              "emit": {
                "outputMapping": [0, 1, 2, 3]
              }
            },
            "inputs": [
              {
                "read": {
                  "common": {
                    "direct": {
                    }
                  },
                  "baseSchema": {
                    "names": ["time", "key", "value1"],
                    "struct": {
                      "types": [
                        {
                          "i32": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "i32": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "fp64": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        }
                      ],
                      "typeVariationReference": 0,
                      "nullability": "NULLABILITY_REQUIRED"
                    }
                  },
                  "namedTable": {
                    "names": ["T1"]
                  }
                }
              },
              {
                "read": {
                  "common": {
                    "direct": {
                    }
                  },
                  "baseSchema": {
                    "names": ["time", "key", "value2"],
                    "struct": {
                      "types": [
                        {
                          "i32": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "i32": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "fp64": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        }
                      ],
                      "typeVariationReference": 0,
                      "nullability": "NULLABILITY_REQUIRED"
                    }
                  },
                  "namedTable": {
                    "names": ["T2"]
                  }
                }
              }
            ],
            "detail": {
              "@type": "/arrow.substrait_ext.AsOfJoinRel",
              "keys" : [
                {
                  "on": {
                    "selection": {
                      "directReference": {
                        "structField": {
                          "field": 0,
                        }
                      },
                      "rootReference": {}
                    }
                  },
                  "by": [
                    {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 1,
                          }
                        },
                        "rootReference": {}
                      }
                    }
                  ]
		},
                {
                  "on": {
                    "selection": {
                      "directReference": {
                        "structField": {
                          "field": 0,
                        }
                      },
                      "rootReference": {}
                    }
                  },
                  "by": [
                    {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 1,
                          }
                        },
                        "rootReference": {}
                      }
                    }
                  ]
		}
	      ],
              "tolerance": -1000
            }
          }
        },
        "names": ["time", "key", "value1", "value2"]
      }
    }],
    "expectedTypeUrls": []
  })";

  std::vector<std::shared_ptr<Schema>> input_schema = {
      schema({field("time", int32()), field("key", int32()), field("value1", float64())}),
      schema(
          {field("time", int32()), field("key", int32()), field("value2", float64())})};
  NamedTableProvider table_provider = ProvideMadeTable(
      [&input_schema](
          const std::vector<std::string>& names) -> Result<std::shared_ptr<Table>> {
        if (names.size() != 1) {
          return Status::Invalid("Multiple test table names");
        }
        if (names[0] == "T1") {
          return TableFromJSON(input_schema[0],
                               {"[[2, 1, 1.1], [4, 1, 2.1], [6, 2, 3.1]]"});
        }
        if (names[0] == "T2") {
          return TableFromJSON(input_schema[1],
                               {"[[1, 1, 1.2], [3, 2, 2.2], [5, 2, 3.2]]"});
        }
        return Status::Invalid("Unknown test table name ", names[0]);
      });
  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));
  ValidateNumProjectNodes(1, buf, conversion_options);
  ASSERT_OK_AND_ASSIGN(
      auto out_schema,
      acero::asofjoin::MakeOutputSchema(
          input_schema, {{FieldRef(0), {FieldRef(1)}}, {FieldRef(0), {FieldRef(1)}}}));
  auto expected_table = TableFromJSON(
      out_schema, {"[[2, 1, 1.1, 1.2], [4, 1, 2.1, 1.2], [6, 2, 3.1, 3.2]]"});
  CheckRoundTripResult(std::move(expected_table), buf, {}, conversion_options);
}

TEST(Substrait, CompoundEmitFilterless) {
  compute::ExecContext exec_context;
  auto left_schema =
      schema({field("A", int32()), field("B", int32()), field("C", int32()),
              field("D", int32()), field("E", int32())});

  auto right_schema =
      schema({field("X", int32()), field("Y", int32()), field("W", int32()),
              field("V", int32()), field("Z", int32())});

  // creating a dummy dataset using a dummy table
  auto left_table = TableFromJSON(left_schema, {R"([
      [10, 1, 21, 32, 43],
      [20, 2, 21, 32, 43],
      [30, 3, 21, 32, 43],
      [80, 2, 21, 52, 45],
      [35, 31, 25, 36, 47]
  ])"});

  auto right_table = TableFromJSON(right_schema, {R"([
      [10, 11, 25, 36, 47],
      [80, 21, 25, 32, 40],
      [32, 31, 25, 36, 42],
      [30, 11, 25, 38, 44],
      [33, 21, 24, 32, 41]
  ])"});

  std::string substrait_json = R"({
  "version": { "major_number": 9999, "minor_number": 9999, "patch_number": 9999 },
  "relations": [{
    "rel": {
      "join": {
        "common": {
          "emit": {
            "outputMapping": [0, 2, 3, 4, 6, 7]
          }
        },
        "left": {
          "project": {
            "common": {
              "emit": {
                "outputMapping": [0, 1, 2, 5]
              }
            },
            "expressions": [{
              "scalarFunction": {
                "functionReference": 32,
                "arguments": [
                  {
                    "value": {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 1
                          }
                        },
                        "rootReference": {
                        }
                      }
                    }
                  }, {
                    "value": {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 2
                          }
                        },
                        "rootReference": {
                        }
                      }
                    }
                  }
                ],
                "output_type": {
                  "bool": {}
                }
              }
            },
            ],
            "input" : {
              "read": {
                "base_schema": {
                  "names": ["A", "B", "C", "D", "E"],
                    "struct": {
                    "types": [{
                      "i32": {}
                    }, {
                      "i32": {}
                    }, {
                      "i32": {}
                    }, {
                      "i32": {}
                    }, {
                      "i32": {}
                    }]
                  }
                },
                "namedTable": {
                  "names": ["left"]
                }
              }
            }
          }
        },
        "right": {
          "project": {
            "common": {
              "emit": {
                "outputMapping": [0, 1, 2, 5]
              }
            },
            "expressions": [{
              "scalarFunction": {
                "functionReference": 32,
                "arguments": [
                  {
                    "value": {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 1
                          }
                        },
                        "rootReference": {
                        }
                      }
                    }
                  }, {
                    "value": {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 2
                          }
                        },
                        "rootReference": {
                        }
                      }
                    }
                  }
                ],
                "output_type": {
                  "bool": {}
                }
              }
            },
            ],
            "input" : {
              "read": {
                "base_schema": {
                  "names": ["X", "Y", "W", "V", "Z"],
                    "struct": {
                    "types": [{
                      "i32": {}
                    }, {
                      "i32": {}
                    }, {
                      "i32": {}
                    }, {
                      "i32": {}
                    }, {
                      "i32": {}
                    }]
                  }
                },
                "namedTable": {
                  "names": ["right"]
                }
              }
            }
          }
        },
        "expression": {
          "scalarFunction": {
            "functionReference": 14,
            "arguments": [{
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 0
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }, {
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 4
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }],
            "output_type": {
              "bool": {}
            },
            "overflow" : {
              "ERROR": {}
            }
          }
        },
        "type": "JOIN_TYPE_INNER"
      }
    }
  }],
  "extension_uris": [
      {
        "extension_uri_anchor": 42,
        "uri": ")" + std::string(kSubstraitComparisonFunctionsUri) +
                               R"("
      },
      {
        "extension_uri_anchor": 72,
        "uri": ")" + std::string(kSubstraitArithmeticFunctionsUri) +
                               R"("
      }
    ],
    "extensions": [
      {
        "extension_function": {
          "extension_uri_reference": 42,
          "function_anchor": 14,
          "name": "equal"
        }
      },
      {
        "extension_function": {
          "extension_uri_reference": 72,
          "function_anchor": 32,
          "name": "add"
        }
      }
    ]
  })";

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));
  auto output_schema = schema({
      field("A", int32()),
      field("E", int32()),
      field("B+C", int32()),
      field("X", int32()),
      field("Z", int32()),
      field("Y+W", int32()),
  });

  auto expected_table = TableFromJSON(std::move(output_schema), {R"([
      [10, 21, 22, 10, 25, 36],
      [30, 21, 24, 30, 25, 36],
      [80, 21, 23, 80, 25, 46]
  ])"});

  NamedTableProvider table_provider =
      [left_table, right_table](const std::vector<std::string>& names, const Schema&) {
        std::shared_ptr<Table> output_table;
        for (const auto& name : names) {
          if (name == "left") {
            output_table = left_table;
          }
          if (name == "right") {
            output_table = right_table;
          }
        }
        std::shared_ptr<acero::ExecNodeOptions> options =
            std::make_shared<acero::TableSourceNodeOptions>(std::move(output_table));
        return acero::Declaration("table_source", {}, options, "mock_source");
      };

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);

  CheckRoundTripResult(std::move(expected_table), buf, {}, conversion_options);
}

TEST(Substrait, CompoundEmitWithFilter) {
#ifdef _WIN32
  GTEST_SKIP() << "ARROW-16392: Substrait File URI not supported for Windows";
#endif
  compute::ExecContext exec_context;
  auto left_schema =
      schema({field("A", int32()), field("B", int32()), field("C", int32()),
              field("D", int32()), field("E", int32())});

  auto right_schema =
      schema({field("X", int32()), field("Y", int32()), field("W", int32()),
              field("V", int32()), field("Z", int32())});

  // creating a dummy dataset using a dummy table
  auto left_table = TableFromJSON(left_schema, {R"([
      [10, 1, 20, 32, 42],
      [20, 2, 10, 32, 41],
      [30, 3, 45, 32, 40],
      [80, 2, 80, 52, 25],
      [35, 31, 25, 36, 47]
  ])"});

  auto right_table = TableFromJSON(right_schema, {R"([
      [10, 11, 5, 36, 47],
      [80, 21, 39, 32, 40],
      [32, 31, 26, 36, 42],
      [30, 11, 12, 38, 44],
      [33, 21, 11, 32, 41]
  ])"});

  std::string substrait_json = R"({
  "relations": [{
    "rel": {
      "filter": {
        "common": {
          "emit": {
            "outputMapping": [0, 2, 7]
          }
        },
        "condition": {
          "scalarFunction": {
            "functionReference": 25,
            "arguments": [{
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 1
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }, {
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 2
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }],
            "output_type": {
              "bool": {}
            }
          }
        },
        "input" : {
          "join": {
            "common": {
              "emit": {
                "outputMapping": [0, 2, 3, 4, 6, 7, 8, 9]
              }
            },
            "left": {
              "project": {
                "common": {
                  "emit": {
                    "outputMapping": [0, 1, 2, 4, 5]
                  }
                },
                "expressions": [{
                  "scalarFunction": {
                    "functionReference": 32,
                    "arguments": [
                      {
                        "value": {
                          "selection": {
                            "directReference": {
                              "structField": {
                                "field": 1
                              }
                            },
                            "rootReference": {
                            }
                          }
                        }
                      }, {
                        "value": {
                          "selection": {
                            "directReference": {
                              "structField": {
                                "field": 2
                              }
                            },
                            "rootReference": {
                            }
                          }
                        }
                      }
                    ],
                    "output_type": {
                      "bool": {}
                    }
                  }
                },
                ],
                "input" : {
                  "read": {
                    "base_schema": {
                      "names": ["A", "B", "C", "D", "E"],
                        "struct": {
                        "types": [{
                          "i32": {}
                        }, {
                          "i32": {}
                        }, {
                          "i32": {}
                        }, {
                          "i32": {}
                        }, {
                          "i32": {}
                        }]
                      }
                    },
                    "namedTable": {
                      "names": ["left"]
                    }
                  }
                }
              }
            },
            "right": {
              "project": {
                "common": {
                  "emit": {
                    "outputMapping": [0, 1, 2, 4, 5]
                  }
                },
                "expressions": [{
                  "scalarFunction": {
                    "functionReference": 32,
                    "arguments": [
                      {
                        "value": {
                          "selection": {
                            "directReference": {
                              "structField": {
                                "field": 1
                              }
                            },
                            "rootReference": {
                            }
                          }
                        }
                      }, {
                        "value": {
                          "selection": {
                            "directReference": {
                              "structField": {
                                "field": 2
                              }
                            },
                            "rootReference": {
                            }
                          }
                        }
                      }
                    ],
                    "output_type": {
                      "bool": {}
                    }
                  }
                },
                ],
                "input" : {
                  "read": {
                    "base_schema": {
                      "names": ["X", "Y", "W", "V", "Z"],
                        "struct": {
                        "types": [{
                          "i32": {}
                        }, {
                          "i32": {}
                        }, {
                          "i32": {}
                        }, {
                          "i32": {}
                        }, {
                          "i32": {}
                        }]
                      }
                    },
                    "namedTable": {
                      "names": ["right"]
                    }
                  }
                }
              }
            },
            "expression": {
              "scalarFunction": {
                "functionReference": 14,
                "arguments": [{
                  "value": {
                    "selection": {
                      "directReference": {
                        "structField": {
                          "field": 0
                        }
                      },
                      "rootReference": {
                      }
                    }
                  }
                }, {
                  "value": {
                    "selection": {
                      "directReference": {
                        "structField": {
                          "field": 5
                        }
                      },
                      "rootReference": {
                      }
                    }
                  }
                }],
                "output_type": {
                  "bool": {}
                },
                "overflow" : {
                  "ERROR": {}
                }
              }
            },
            "type": "JOIN_TYPE_INNER"
          }
        }
      }
    }
  }],
  "extension_uris": [
      {
        "extension_uri_anchor": 42,
        "uri": ")" + std::string(kSubstraitComparisonFunctionsUri) +
                               R"("
      },
      {
        "extension_uri_anchor": 72,
        "uri": ")" + std::string(kSubstraitArithmeticFunctionsUri) +
                               R"("
      }
    ],
    "extensions": [
      {
        "extension_function": {
          "extension_uri_reference": 42,
          "function_anchor": 14,
          "name": "equal"
        }
      },
      {
        "extension_function": {
          "extension_uri_reference": 42,
          "function_anchor": 25,
          "name": "lt"
        }
      },
      {
        "extension_function": {
          "extension_uri_reference": 72,
          "function_anchor": 32,
          "name": "add"
        }
      }
    ]
  })";

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));
  auto output_schema = schema({
      field("A", int32()),
      field("E", int32()),
      field("Y+W", int32()),
  });

  auto expected_table = TableFromJSON(std::move(output_schema), {R"([
      [10, 42, 16]
  ])"});

  NamedTableProvider table_provider = [left_table, right_table](
                                          const std::vector<std::string>& names,
                                          const Schema&) -> Result<acero::Declaration> {
    std::shared_ptr<Table> output_table;
    for (const auto& name : names) {
      if (name == "left") {
        output_table = left_table;
      }
      if (name == "right") {
        output_table = right_table;
      }
    }
    if (!output_table) {
      return Status::Invalid("NamedTableProvider couldn't set the table");
    }
    std::shared_ptr<acero::ExecNodeOptions> options =
        std::make_shared<acero::TableSourceNodeOptions>(std::move(output_table));
    return acero::Declaration("table_source", {}, options, "mock_source");
  };

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);

  CheckRoundTripResult(std::move(expected_table), buf, {}, conversion_options);
}

TEST(Substrait, SortAndFetch) {
  // Sort by A, ascending, take items [2, 5), then sort by B descending
  std::string substrait_json = R"({
    "version": {
        "major_number": 9999,
        "minor_number": 9999,
        "patch_number": 9999
    },
    "relations": [
        {
            "rel": {
                "sort": {
                    "input": {
                        "fetch": {
                            "input": {
                                "sort": {
                                    "input": {
                                        "read": {
                                            "base_schema": {
                                                "names": [
                                                    "A",
                                                    "B"
                                                ],
                                                "struct": {
                                                    "types": [
                                                        {
                                                            "i32": {}
                                                        },
                                                        {
                                                            "i32": {}
                                                        }
                                                    ]
                                                }
                                            },
                                            "namedTable": {
                                                "names": [
                                                    "table"
                                                ]
                                            }
                                        }
                                    },
                                    "sorts": [
                                        {
                                            "expr": {
                                                "selection": {
                                                    "directReference": {
                                                        "structField": {
                                                            "field": 0
                                                        }
                                                    },
                                                    "rootReference": {}
                                                }
                                            },
                                            "direction": "SORT_DIRECTION_ASC_NULLS_FIRST"
                                        }
                                    ]
                                }
                            },
                            "offset": 2,
                            "count": 3
                        }
                    },
                    "sorts": [
                        {
                            "expr": {
                                "selection": {
                                    "directReference": {
                                        "structField": {
                                            "field": 1
                                        }
                                    },
                                    "rootReference": {}
                                }
                            },
                            "direction": "SORT_DIRECTION_DESC_NULLS_LAST"
                        }
                    ]
                }
            }
        }
    ],
    "extension_uris": [],
    "extensions": []
})";

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));
  auto test_schema = schema({field("A", int32()), field("B", int32())});

  auto input_table = TableFromJSON(test_schema, {R"([
      [null, null],
      [5, 8],
      [null, null],
      [null, null],
      [3, 4],
      [9, 6],
      [4, 5]
  ])"});

  // First sort by A, ascending, nulls first to yield rows:
  // 0, 2, 3, 4, 6, 1, 5
  // Apply fetch to grab rows 3, 4, 6
  // Then sort by B, descending, to yield rows 6, 4, 3

  auto output_table = TableFromJSON(test_schema, {R"([
    [4, 5],
    [3, 4],
    [null, null]
  ])"});

  ConversionOptions conversion_options;
  conversion_options.named_table_provider =
      AlwaysProvideSameTable(std::move(input_table));

  CheckRoundTripResult(std::move(output_table), buf, {}, conversion_options);
}

TEST(Substrait, MixedSort) {
  // Substrait allows two sort keys with differing direction but Acero
  // does not.  We should detect this and reject it.
  std::string substrait_json = R"({
  "version": {
    "major_number": 9999,
    "minor_number": 9999,
    "patch_number": 9999
  },
  "relations": [
    {
      "rel": {
        "sort": {
          "input": {
            "read": {
              "base_schema": {
                "names": [
                  "A",
                  "B"
                ],
                "struct": {
                  "types": [
                    {
                      "i32": {}
                    },
                    {
                      "i32": {}
                    }
                  ]
                }
              },
              "namedTable": {
                "names": [
                  "table"
                ]
              }
            }
          },
          "sorts": [
            {
              "expr": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 0
                    }
                  },
                  "rootReference": {}
                }
              },
              "direction": "SORT_DIRECTION_ASC_NULLS_FIRST"
            },
            {
              "expr": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 1
                    }
                  },
                  "rootReference": {}
                }
              },
              "direction": "SORT_DIRECTION_ASC_NULLS_LAST"
            }
          ]
        }
      }
    }
  ],
  "extension_uris": [],
  "extensions": []
})";

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));
  auto test_schema = schema({field("A", int32()), field("B", int32())});

  auto input_table = TableFromJSON(test_schema, {R"([
      [null, null],
      [5, 8],
      [null, null],
      [null, null],
      [3, 4],
      [9, 6],
      [4, 5]
  ])"});

  NamedTableProvider table_provider = [&](const std::vector<std::string>& names,
                                          const Schema&) {
    std::shared_ptr<acero::ExecNodeOptions> options =
        std::make_shared<acero::TableSourceNodeOptions>(input_table);
    return acero::Declaration("table_source", {}, options, "mock_source");
  };

  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);

  ASSERT_THAT(
      DeserializePlan(*buf, /*registry=*/nullptr, /*ext_set_out=*/nullptr,
                      conversion_options),
      Raises(StatusCode::NotImplemented, testing::HasSubstr("mixed null placement")));
}

TEST(Substrait, PlanWithExtension) {
  // This demos an extension relation
  std::string substrait_json = R"({
    "extensionUris": [],
    "extensions": [],
    "relations": [{
      "root": {
        "input": {
          "extension_multi": {
            "common": {
              "emit": {
                "outputMapping": [0, 1, 2, 3]
              }
            },
            "inputs": [
              {
                "read": {
                  "common": {
                    "direct": {
                    }
                  },
                  "baseSchema": {
                    "names": ["time", "key", "value1"],
                    "struct": {
                      "types": [
                        {
                          "i32": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "i32": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "fp64": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        }
                      ],
                      "typeVariationReference": 0,
                      "nullability": "NULLABILITY_REQUIRED"
                    }
                  },
                  "namedTable": {
                    "names": ["T1"]
                  }
                }
              },
              {
                "read": {
                  "common": {
                    "direct": {
                    }
                  },
                  "baseSchema": {
                    "names": ["time", "key", "value2"],
                    "struct": {
                      "types": [
                        {
                          "i32": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "i32": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "fp64": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        }
                      ],
                      "typeVariationReference": 0,
                      "nullability": "NULLABILITY_REQUIRED"
                    }
                  },
                  "namedTable": {
                    "names": ["T2"]
                  }
                }
              }
            ],
            "detail": {
              "@type": "/arrow.substrait_ext.AsOfJoinRel",
              "keys" : [
                {
                  "on": {
                    "selection": {
                      "directReference": {
                        "structField": {
                          "field": 0,
                        }
                      },
                      "rootReference": {}
                    }
                  },
                  "by": [
                    {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 1,
                          }
                        },
                        "rootReference": {}
                      }
                    }
                  ]
		},
                {
                  "on": {
                    "selection": {
                      "directReference": {
                        "structField": {
                          "field": 0,
                        }
                      },
                      "rootReference": {}
                    }
                  },
                  "by": [
                    {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 1,
                          }
                        },
                        "rootReference": {}
                      }
                    }
                  ]
		}
	      ],
              "tolerance": -1000
            }
          }
        },
        "names": ["time", "key", "value1", "value2"]
      }
    }],
    "expectedTypeUrls": []
  })";

  std::vector<std::shared_ptr<Schema>> input_schema = {
      schema({field("time", int32()), field("key", int32()), field("value1", float64())}),
      schema(
          {field("time", int32()), field("key", int32()), field("value2", float64())})};
  NamedTableProvider table_provider = ProvideMadeTable(
      [&input_schema](
          const std::vector<std::string>& names) -> Result<std::shared_ptr<Table>> {
        if (names.size() != 1) {
          return Status::Invalid("Multiple test table names");
        }
        if (names[0] == "T1") {
          return TableFromJSON(input_schema[0],
                               {"[[2, 1, 1.1], [4, 1, 2.1], [6, 2, 3.1]]"});
        }
        if (names[0] == "T2") {
          return TableFromJSON(input_schema[1],
                               {"[[1, 1, 1.2], [3, 2, 2.2], [5, 2, 3.2]]"});
        }
        return Status::Invalid("Unknown test table name ", names[0]);
      });
  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));

  ASSERT_OK_AND_ASSIGN(
      auto out_schema,
      acero::asofjoin::MakeOutputSchema(
          input_schema, {{FieldRef(0), {FieldRef(1)}}, {FieldRef(0), {FieldRef(1)}}}));
  auto expected_table = TableFromJSON(
      out_schema, {"[[2, 1, 1.1, 1.2], [4, 1, 2.1, 1.2], [6, 2, 3.1, 3.2]]"});
  CheckRoundTripResult(std::move(expected_table), buf, {}, conversion_options);
}

TEST(Substrait, AsOfJoinDefaultEmit) {
  std::string substrait_json = R"({
    "extensionUris": [],
    "extensions": [],
    "relations": [{
      "root": {
        "input": {
          "extension_multi": {
            "inputs": [
              {
                "read": {
                  "common": {
                    "direct": {
                    }
                  },
                  "baseSchema": {
                    "names": ["time", "key", "value1"],
                    "struct": {
                      "types": [
                        {
                          "i32": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "i32": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "fp64": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        }
                      ],
                      "typeVariationReference": 0,
                      "nullability": "NULLABILITY_REQUIRED"
                    }
                  },
                  "namedTable": {
                    "names": ["T1"]
                  }
                }
              },
              {
                "read": {
                  "common": {
                    "direct": {
                    }
                  },
                  "baseSchema": {
                    "names": ["time", "key", "value2"],
                    "struct": {
                      "types": [
                        {
                          "i32": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "i32": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "fp64": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        }
                      ],
                      "typeVariationReference": 0,
                      "nullability": "NULLABILITY_REQUIRED"
                    }
                  },
                  "namedTable": {
                    "names": ["T2"]
                  }
                }
              }
            ],
            "detail": {
              "@type": "/arrow.substrait_ext.AsOfJoinRel",
              "keys" : [
                {
                  "on": {
                    "selection": {
                      "directReference": {
                        "structField": {
                          "field": 0,
                        }
                      },
                      "rootReference": {}
                    }
                  },
                  "by": [
                    {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 1,
                          }
                        },
                        "rootReference": {}
                      }
                    }
                  ]
		},
                {
                  "on": {
                    "selection": {
                      "directReference": {
                        "structField": {
                          "field": 0,
                        }
                      },
                      "rootReference": {}
                    }
                  },
                  "by": [
                    {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 1,
                          }
                        },
                        "rootReference": {}
                      }
                    }
                  ]
		}
	      ],
              "tolerance": -1000
            }
          }
        },
        "names": ["time", "key", "value1", "value2"]
      }
    }],
    "expectedTypeUrls": []
  })";

  std::vector<std::shared_ptr<Schema>> input_schema = {
      schema({field("time", int32()), field("key", int32()), field("value1", float64())}),
      schema(
          {field("time", int32()), field("key", int32()), field("value2", float64())})};
  NamedTableProvider table_provider = ProvideMadeTable(
      [&input_schema](
          const std::vector<std::string>& names) -> Result<std::shared_ptr<Table>> {
        if (names.size() != 1) {
          return Status::Invalid("Multiple test table names");
        }
        if (names[0] == "T1") {
          return TableFromJSON(input_schema[0],
                               {"[[2, 1, 1.1], [4, 1, 2.1], [6, 2, 3.1]]"});
        }
        if (names[0] == "T2") {
          return TableFromJSON(input_schema[1],
                               {"[[1, 1, 1.2], [3, 2, 2.2], [5, 2, 3.2]]"});
        }
        return Status::Invalid("Unknown test table name ", names[0]);
      });
  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));

  auto out_schema = schema({field("time", int32()), field("key", int32()),
                            field("value1", float64()), field("value2", float64())});

  auto expected_table = TableFromJSON(
      out_schema, {"[[2, 1, 1.1, 1.2], [4, 1, 2.1, 1.2], [6, 2, 3.1, 3.2]]"});
  CheckRoundTripResult(std::move(expected_table), buf, {}, conversion_options);
}

TEST(Substrait, PlanWithNamedTapExtension) {
  // This demos an extension relation
  std::string substrait_json = R"({
    "extensionUris": [],
    "extensions": [],
    "relations": [{
      "root": {
        "input": {
          "extension_multi": {
            "inputs": [
              {
                "read": {
                  "common": {
                    "direct": {
                    }
                  },
                  "baseSchema": {
                    "names": ["time", "key", "value"],
                    "struct": {
                      "types": [
                        {
                          "i32": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "i32": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "fp64": {
                            "typeVariationReference": 0,
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        }
                      ],
                      "typeVariationReference": 0,
                      "nullability": "NULLABILITY_REQUIRED"
                    }
                  },
                  "namedTable": {
                    "names": ["T"]
                  }
                }
              }
            ],
            "detail": {
              "@type": "/arrow.substrait_ext.NamedTapRel",
              "kind" : "pass_for_named_tap",
              "name" : "does_not_matter",
              "columns" : ["pass_time", "pass_key", "pass_value"]
            }
          }
        },
        "names": ["t", "k", "v"]
      }
    }],
    "expectedTypeUrls": []
  })";

  ASSERT_OK(AddPassFactory("pass_for_named_tap"));

  std::shared_ptr<Schema> input_schema =
      schema({field("time", int32()), field("key", int32()), field("value", float64())});
  NamedTableProvider table_provider = AlwaysProvideSameTable(
      TableFromJSON(input_schema, {"[[2, 1, 1.1], [4, 1, 2.1], [6, 2, 3.1]]"}));
  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);
  conversion_options.named_tap_provider =
      [](const std::string& tap_kind, std::vector<acero::Declaration::Input> inputs,
         const std::string& tap_name,
         std::shared_ptr<Schema> tap_schema) -> Result<acero::Declaration> {
    return acero::Declaration{tap_kind, std::move(inputs), acero::ExecNodeOptions{}};
  };

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));

  std::shared_ptr<Schema> output_schema =
      schema({field("t", int32()), field("k", int32()), field("v", float64())});
  auto expected_table =
      TableFromJSON(output_schema, {"[[2, 1, 1.1], [4, 1, 2.1], [6, 2, 3.1]]"});
  CheckRoundTripResult(std::move(expected_table), buf, {}, conversion_options);
}

TEST(Substrait, PlanWithSegmentedAggregateExtension) {
  // This demos an extension relation
  std::string substrait_json = R"({
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "sum"
      }
    }],
    "relations": [{
      "root": {
        "input": {
          "extension_single": {
            "input": {
              "read": {
                "common": {
                  "direct": {
                  }
                },
                "baseSchema": {
                  "names": ["time", "key", "value"],
                  "struct": {
                    "types": [
                      {
                        "i32": {
                          "typeVariationReference": 0,
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      },
                      {
                        "i32": {
                          "typeVariationReference": 0,
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      },
                      {
                        "fp64": {
                          "typeVariationReference": 0,
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      }
                    ],
                    "typeVariationReference": 0,
                    "nullability": "NULLABILITY_REQUIRED"
                  }
                },
                "namedTable": {
                  "names": ["T"]
                }
              }
            },
            "detail": {
              "@type": "/arrow.substrait_ext.SegmentedAggregateRel",
              "grouping_keys": [{
                "directReference": {
                  "structField": {
                    "field": 1
                  }
                },
                "rootReference": {}
              }],
              "segment_keys": [{
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                },
                "rootReference": {}
              }],
              "measures": [{
                "measure": {
                  "functionReference": 0,
                  "arguments": [{
                    "value": {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 2
                          }
                        }
                      }
                    }
                  }],
                  "sorts": [],
                  "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
                  "outputType": {
                    "i64": {}
                  }
                }
              }]
            }
          }
        },
        "names": ["k", "t", "v"]
      }
    }],
    "expectedTypeUrls": []
  })";

  std::shared_ptr<Schema> input_schema =
      schema({field("time", int32()), field("key", int32()), field("value", float64())});
  NamedTableProvider table_provider = AlwaysProvideSameTable(
      TableFromJSON(input_schema, {"[[1, 1, 1], [1, 2, 2], [1, 1, 3],"
                                   " [2, 2, 4], [2, 1, 5], [2, 2, 6]]"}));
  ConversionOptions conversion_options;
  conversion_options.named_table_provider = std::move(table_provider);
  conversion_options.named_tap_provider =
      [](const std::string& tap_kind, std::vector<acero::Declaration::Input> inputs,
         const std::string& tap_name,
         std::shared_ptr<Schema> tap_schema) -> Result<acero::Declaration> {
    return acero::Declaration{tap_kind, std::move(inputs), acero::ExecNodeOptions{}};
  };

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));

  std::shared_ptr<Schema> output_schema =
      schema({field("k", int32()), field("t", int32()), field("v", float64())});
  auto expected_table =
      TableFromJSON(output_schema, {"[[1, 1, 4], [1, 2, 2], [2, 2, 10], [2, 1, 5]]"});
  CheckRoundTripResult(std::move(expected_table), buf, {}, conversion_options);
}

}  // namespace engine
}  // namespace arrow
