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

#include "arrow/extension/variant_shredding.h"

#include <cstring>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_decimal.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer.h"
#include "arrow/extension/variant.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/decimal.h"

namespace arrow::extension::variant {

namespace {

Result<int32_t> GetObjectFieldCount(const uint8_t* data, int64_t length) {
  VariantMetadata empty_meta;
  ARROW_ASSIGN_OR_RAISE(auto obj, VariantObjectView::Make(empty_meta, data, length));
  return obj.num_fields();
}

Result<int32_t> GetArrayElementCount(const uint8_t* data, int64_t length) {
  VariantMetadata empty_meta;
  ARROW_ASSIGN_OR_RAISE(auto arr, VariantArrayView::Make(empty_meta, data, length));
  return arr.num_elements();
}

}  // namespace

namespace {

// Test-local helper to extract variant bytes from a BinaryArray at index i.
// Named to avoid confusion with BinaryViewArray::GetView().
std::string_view GetBinaryView(const Array& array, int64_t i) {
  if (array.type_id() == Type::BINARY) {
    auto& bin = static_cast<const BinaryArray&>(array);
    auto view = bin.GetView(i);
    return {reinterpret_cast<const char*>(view.data()), static_cast<size_t>(view.size())};
  }
  return {};
}

}  // namespace

// ===========================================================================
// VariantShreddingSchema tests
// ===========================================================================

class VariantShreddingSchemaTest : public ::testing::Test {};

TEST_F(VariantShreddingSchemaTest, PrimitiveSchema) {
  auto schema = VariantShreddingSchema::Primitive(int64());
  ASSERT_EQ(schema.kind(), VariantShreddingSchema::Kind::kPrimitive);
  ASSERT_EQ(schema.type()->id(), Type::INT64);
}

TEST_F(VariantShreddingSchemaTest, PrimitiveToArrowType) {
  auto schema = VariantShreddingSchema::Primitive(utf8());
  auto arrow_type = schema.ToArrowType();
  ASSERT_EQ(arrow_type->id(), Type::STRING);
}

TEST_F(VariantShreddingSchemaTest, ObjectSchema) {
  auto schema = VariantShreddingSchema::Object({
      {"name", VariantShreddingSchema::Primitive(utf8())},
      {"age", VariantShreddingSchema::Primitive(int64())},
  });
  ASSERT_EQ(schema.kind(), VariantShreddingSchema::Kind::kObject);
  ASSERT_EQ(schema.fields().size(), 2);
  ASSERT_EQ(schema.fields()[0].first, "name");
  ASSERT_EQ(schema.fields()[1].first, "age");
}

TEST_F(VariantShreddingSchemaTest, ObjectToArrowType) {
  auto schema = VariantShreddingSchema::Object({
      {"event_type", VariantShreddingSchema::Primitive(utf8())},
      {"event_ts", VariantShreddingSchema::Primitive(timestamp(TimeUnit::MICRO, "UTC"))},
  });
  auto arrow_type = schema.ToArrowType();
  ASSERT_EQ(arrow_type->id(), Type::STRUCT);
  auto struct_type = std::static_pointer_cast<StructType>(arrow_type);
  ASSERT_EQ(struct_type->num_fields(), 2);

  // Each field should be a struct with {value, typed_value}
  auto event_type_field = struct_type->field(0);
  ASSERT_EQ(event_type_field->name(), "event_type");
  ASSERT_EQ(event_type_field->type()->id(), Type::STRUCT);
  auto inner = std::static_pointer_cast<StructType>(event_type_field->type());
  ASSERT_EQ(inner->num_fields(), 2);
  ASSERT_EQ(inner->field(0)->name(), "value");
  ASSERT_EQ(inner->field(0)->type()->id(), Type::BINARY);
  ASSERT_TRUE(inner->field(0)->nullable());
  ASSERT_EQ(inner->field(1)->name(), "typed_value");
  ASSERT_EQ(inner->field(1)->type()->id(), Type::STRING);
  ASSERT_TRUE(inner->field(1)->nullable());
}

TEST_F(VariantShreddingSchemaTest, ArraySchema) {
  auto schema = VariantShreddingSchema::Array(VariantShreddingSchema::Primitive(int32()));
  ASSERT_EQ(schema.kind(), VariantShreddingSchema::Kind::kArray);
  ASSERT_EQ(schema.element_schema().kind(), VariantShreddingSchema::Kind::kPrimitive);
  ASSERT_EQ(schema.element_schema().type()->id(), Type::INT32);
}

TEST_F(VariantShreddingSchemaTest, ArrayToArrowType) {
  auto schema =
      VariantShreddingSchema::Array(VariantShreddingSchema::Primitive(float64()));
  auto arrow_type = schema.ToArrowType();
  ASSERT_EQ(arrow_type->id(), Type::LIST);
  auto list_type = std::static_pointer_cast<ListType>(arrow_type);
  auto elem_type = list_type->value_type();
  ASSERT_EQ(elem_type->id(), Type::STRUCT);
}

TEST_F(VariantShreddingSchemaTest, NestedObjectSchema) {
  // Schema for shredding: {"location": {"lat": double, "lon": double}}
  auto schema = VariantShreddingSchema::Object({
      {"location", VariantShreddingSchema::Object({
                       {"lat", VariantShreddingSchema::Primitive(float64())},
                       {"lon", VariantShreddingSchema::Primitive(float64())},
                   })},
  });
  ASSERT_EQ(schema.kind(), VariantShreddingSchema::Kind::kObject);
  ASSERT_EQ(schema.fields().size(), 1);
  ASSERT_EQ(schema.fields()[0].first, "location");
  ASSERT_EQ(schema.fields()[0].second.kind(), VariantShreddingSchema::Kind::kObject);
}

// ===========================================================================
// Type compatibility tests
// ===========================================================================

class VariantTypeCompatibilityTest : public ::testing::Test {
 protected:
  // Helper to build a variant value and check compatibility
  bool CheckCompatibility(std::function<Status(VariantBuilder&)> build_fn,
                          const DataType& target) {
    VariantBuilder b;
    build_fn(b).ok();
    auto result = b.Finish().ValueOrDie();
    return IsVariantCompatibleWithType(result.value.data(),
                                       static_cast<int64_t>(result.value.size()), target);
  }
};

TEST_F(VariantTypeCompatibilityTest, Int8CompatibleWithInt8) {
  ASSERT_TRUE(CheckCompatibility([](VariantBuilder& b) { return b.Int8(42); }, *int8()));
}

TEST_F(VariantTypeCompatibilityTest, Int8CompatibleWithInt64) {
  // Int8 can be widened to Int64
  ASSERT_TRUE(CheckCompatibility([](VariantBuilder& b) { return b.Int8(42); }, *int64()));
}

TEST_F(VariantTypeCompatibilityTest, Int64NotCompatibleWithInt32) {
  // Int64 cannot be narrowed to Int32
  ASSERT_FALSE(CheckCompatibility([](VariantBuilder& b) { return b.Int64(5000000000LL); },
                                  *int32()));
}

TEST_F(VariantTypeCompatibilityTest, StringCompatibleWithUtf8) {
  ASSERT_TRUE(
      CheckCompatibility([](VariantBuilder& b) { return b.String("hello"); }, *utf8()));
}

TEST_F(VariantTypeCompatibilityTest, ShortStringCompatibleWithUtf8) {
  ASSERT_TRUE(
      CheckCompatibility([](VariantBuilder& b) { return b.String("hi"); }, *utf8()));
}

TEST_F(VariantTypeCompatibilityTest, BoolCompatibleWithBool) {
  ASSERT_TRUE(
      CheckCompatibility([](VariantBuilder& b) { return b.Bool(true); }, *boolean()));
}

TEST_F(VariantTypeCompatibilityTest, BoolNotCompatibleWithInt32) {
  ASSERT_FALSE(
      CheckCompatibility([](VariantBuilder& b) { return b.Bool(true); }, *int32()));
}

TEST_F(VariantTypeCompatibilityTest, DoubleCompatibleWithFloat64) {
  ASSERT_TRUE(
      CheckCompatibility([](VariantBuilder& b) { return b.Double(3.14); }, *float64()));
}

TEST_F(VariantTypeCompatibilityTest, FloatCompatibleWithFloat64ViaWidening) {
  // Float can be widened to Double — value precision preserved, type tag changes
  ASSERT_TRUE(
      CheckCompatibility([](VariantBuilder& b) { return b.Float(3.14f); }, *float64()));
}

TEST_F(VariantTypeCompatibilityTest, FloatCompatibleWithFloat32) {
  // Float is directly compatible with its own type
  ASSERT_TRUE(
      CheckCompatibility([](VariantBuilder& b) { return b.Float(3.14f); }, *float32()));
}

TEST_F(VariantTypeCompatibilityTest, DoubleNotCompatibleWithFloat32) {
  // Double cannot be narrowed to Float
  ASSERT_FALSE(
      CheckCompatibility([](VariantBuilder& b) { return b.Double(3.14); }, *float32()));
}

TEST_F(VariantTypeCompatibilityTest, DateCompatibleWithDate32) {
  ASSERT_TRUE(
      CheckCompatibility([](VariantBuilder& b) { return b.Date(19000); }, *date32()));
}

TEST_F(VariantTypeCompatibilityTest, TimestampMicrosCompatibleWithTimestamp) {
  ASSERT_TRUE(CheckCompatibility(
      [](VariantBuilder& b) { return b.TimestampMicros(1654041600000000LL); },
      *timestamp(TimeUnit::MICRO, "UTC")));
}

TEST_F(VariantTypeCompatibilityTest, TimestampMicrosNotCompatibleWithNanos) {
  // TimestampMicros should NOT be compatible with NANO resolution target
  ASSERT_FALSE(CheckCompatibility(
      [](VariantBuilder& b) { return b.TimestampMicros(1654041600000000LL); },
      *timestamp(TimeUnit::NANO, "UTC")));
}

TEST_F(VariantTypeCompatibilityTest, TimestampMicrosNotCompatibleWithNTZ) {
  // TimestampMicros (with timezone) should NOT be compatible with NTZ target
  ASSERT_FALSE(CheckCompatibility(
      [](VariantBuilder& b) { return b.TimestampMicros(1654041600000000LL); },
      *timestamp(TimeUnit::MICRO)));
}

TEST_F(VariantTypeCompatibilityTest, TimestampNanosNTZCompatibleWithNanosNTZ) {
  ASSERT_TRUE(CheckCompatibility(
      [](VariantBuilder& b) { return b.TimestampNanosNTZ(1654041600000000000LL); },
      *timestamp(TimeUnit::NANO)));
}

TEST_F(VariantTypeCompatibilityTest, DecimalScaleMismatchNotCompatible) {
  // Decimal4 with scale=3 should NOT be compatible with Decimal128(10,2)
  ASSERT_FALSE(CheckCompatibility(
      [](VariantBuilder& b) {
        uint8_t bytes[4] = {0x39, 0x30, 0x00, 0x00};
        return b.Decimal4(3, bytes);
      },
      *decimal128(10, 2)));
}

TEST_F(VariantTypeCompatibilityTest, DecimalScaleMatchCompatible) {
  // Decimal4 with scale=2 should be compatible with Decimal128(10,2)
  ASSERT_TRUE(CheckCompatibility(
      [](VariantBuilder& b) {
        uint8_t bytes[4] = {0x39, 0x30, 0x00, 0x00};
        return b.Decimal4(2, bytes);
      },
      *decimal128(10, 2)));
}

TEST_F(VariantTypeCompatibilityTest, NullNotCompatibleWithTypedColumns) {
  // Per Rust/spec: Variant::Null is NOT compatible with any typed column.
  // It remains in the value column to distinguish "variant null" from "missing".
  ASSERT_FALSE(CheckCompatibility([](VariantBuilder& b) { return b.Null(); }, *int64()));
  ASSERT_FALSE(CheckCompatibility([](VariantBuilder& b) { return b.Null(); }, *utf8()));
  ASSERT_FALSE(
      CheckCompatibility([](VariantBuilder& b) { return b.Null(); }, *boolean()));
}

TEST_F(VariantTypeCompatibilityTest, StringNotCompatibleWithInt64) {
  ASSERT_FALSE(
      CheckCompatibility([](VariantBuilder& b) { return b.String("hello"); }, *int64()));
}

TEST_F(VariantTypeCompatibilityTest, UUIDCompatibleWithFixedSizeBinary16) {
  uint8_t uuid[16] = {};
  ASSERT_TRUE(CheckCompatibility([&uuid](VariantBuilder& b) { return b.UUID(uuid); },
                                 *fixed_size_binary(16)));
}

TEST_F(VariantTypeCompatibilityTest, UUIDNotCompatibleWithFixedSizeBinary32) {
  uint8_t uuid[16] = {};
  ASSERT_FALSE(CheckCompatibility([&uuid](VariantBuilder& b) { return b.UUID(uuid); },
                                  *fixed_size_binary(32)));
}

TEST_F(VariantTypeCompatibilityTest, Time64MicroCompatibleWithTime64Micro) {
  ASSERT_TRUE(CheckCompatibility([](VariantBuilder& b) { return b.TimeNTZ(1234567); },
                                 *time64(TimeUnit::MICRO)));
}

TEST_F(VariantTypeCompatibilityTest, Time64NanoNotCompatibleWithTime64Micro) {
  // The variant spec's kTimeNTZ stores microseconds. A time64(NANO) target
  // would cause misinterpretation of the values in the typed_value column.
  ASSERT_FALSE(CheckCompatibility([](VariantBuilder& b) { return b.TimeNTZ(1234567); },
                                  *time64(TimeUnit::NANO)));
}

// ===========================================================================
// ShredVariantColumn / ReconstructVariantColumn round-trip tests
// ===========================================================================

class VariantShredRoundTripTest : public ::testing::Test {
 protected:
  // Helper: build a binary array of encoded variant values.
  // Note: Uses .ok()/.ValueOrDie() because ASSERT_OK_AND_ASSIGN cannot be used
  // in non-void functions. Test-only; will crash with a descriptive message
  // on failure rather than producing a clean test failure.
  std::shared_ptr<Array> BuildVariantColumn(
      const std::vector<std::function<Status(VariantBuilder&)>>& builders) {
    // Use a single shared builder to produce consistent metadata
    BinaryBuilder array_builder;
    for (const auto& build_fn : builders) {
      VariantBuilder vb;
      build_fn(vb).ok();
      auto encoded = vb.Finish().ValueOrDie();
      array_builder
          .Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
          .ok();
    }
    std::shared_ptr<Array> result;
    array_builder.Finish(&result).ok();
    return result;
  }

  // Helper: build a metadata array (all same metadata)
  std::shared_ptr<Array> BuildMetadataColumn(int64_t num_rows) {
    // Empty metadata (no dictionary keys needed for primitives)
    VariantBuilder vb;
    vb.Null().ok();
    auto encoded = vb.Finish().ValueOrDie();

    BinaryBuilder builder;
    for (int64_t i = 0; i < num_rows; ++i) {
      builder
          .Append(encoded.metadata.data(), static_cast<int32_t>(encoded.metadata.size()))
          .ok();
    }
    std::shared_ptr<Array> result;
    builder.Finish(&result).ok();
    return result;
  }
};

TEST_F(VariantShredRoundTripTest, PrimitiveInt64AllMatch) {
  // All values are integers — should all go to typed_value
  auto values = BuildVariantColumn({
      [](VariantBuilder& b) { return b.Int(42); },
      [](VariantBuilder& b) { return b.Int(100); },
      [](VariantBuilder& b) { return b.Int(-7); },
  });
  auto metadata = BuildMetadataColumn(3);
  auto schema = VariantShreddingSchema::Primitive(int64());

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  // All values should be in typed_value (value column all null)
  auto value_col = shredded->field(1);    // "value"
  ASSERT_EQ(value_col->null_count(), 3);  // all null

  auto typed_col = shredded->field(2);  // "typed_value"
  ASSERT_EQ(typed_col->type_id(), Type::INT64);
  ASSERT_EQ(typed_col->null_count(), 0);  // all present

  // Verify native values
  auto& int_arr = static_cast<const Int64Array&>(*typed_col);
  ASSERT_EQ(int_arr.Value(0), 42);
  ASSERT_EQ(int_arr.Value(1), 100);
  ASSERT_EQ(int_arr.Value(2), -7);

  // Reconstruct and verify round-trip
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 3);
  // Verify the reconstructed variant bytes are valid
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_TRUE(reconstructed->IsValid(i));
  }
}

TEST_F(VariantShredRoundTripTest, PrimitiveMixedTypes) {
  // Mix of matching and non-matching types
  auto values = BuildVariantColumn({
      [](VariantBuilder& b) { return b.Int(42); },       // matches int64
      [](VariantBuilder& b) { return b.String("hi"); },  // doesn't match
      [](VariantBuilder& b) { return b.Int(99); },       // matches
      [](VariantBuilder& b) { return b.Bool(true); },    // doesn't match
  });
  auto metadata = BuildMetadataColumn(4);
  auto schema = VariantShreddingSchema::Primitive(int64());

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  // Rows 0,2 → typed_value; Rows 1,3 → value
  ASSERT_TRUE(value_col->IsNull(0));
  ASSERT_TRUE(value_col->IsValid(1));
  ASSERT_TRUE(value_col->IsNull(2));
  ASSERT_TRUE(value_col->IsValid(3));

  ASSERT_TRUE(typed_col->IsValid(0));
  ASSERT_TRUE(typed_col->IsNull(1));
  ASSERT_TRUE(typed_col->IsValid(2));
  ASSERT_TRUE(typed_col->IsNull(3));

  // Verify native int values
  auto& int_arr = static_cast<const Int64Array&>(*typed_col);
  ASSERT_EQ(int_arr.Value(0), 42);
  ASSERT_EQ(int_arr.Value(2), 99);

  // Round-trip
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 4);
}

TEST_F(VariantShredRoundTripTest, PrimitiveAllMismatch) {
  // No values match the target type — all stay in value column
  auto values = BuildVariantColumn({
      [](VariantBuilder& b) { return b.String("a"); },
      [](VariantBuilder& b) { return b.String("b"); },
  });
  auto metadata = BuildMetadataColumn(2);
  auto schema = VariantShreddingSchema::Primitive(int64());

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  // All in value, none in typed_value
  ASSERT_EQ(value_col->null_count(), 0);
  ASSERT_EQ(typed_col->null_count(), 2);

  // Round-trip: since typed_value is all null, everything comes from value
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(GetBinaryView(*values, i), GetBinaryView(*reconstructed, i));
  }
}

TEST_F(VariantShredRoundTripTest, NullVariantIsRouted) {
  // Per Rust/spec: Variant::Null is NOT shredded into typed columns.
  // It goes to the value column as raw bytes (0x00). This distinguishes
  // "variant-typed null" from "SQL NULL / missing" (both columns null).
  auto values = BuildVariantColumn({
      [](VariantBuilder& b) { return b.Null(); },
      [](VariantBuilder& b) { return b.Int(5); },
  });
  auto metadata = BuildMetadataColumn(2);
  auto schema = VariantShreddingSchema::Primitive(int64());

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);
  // Variant Null → value column has content (0x00 byte), typed_value is null
  ASSERT_TRUE(value_col->IsValid(0));
  ASSERT_TRUE(typed_col->IsNull(0));
  // Int(5) → typed_value is present, value is null
  ASSERT_TRUE(value_col->IsNull(1));
  ASSERT_TRUE(typed_col->IsValid(1));
}

TEST_F(VariantShredRoundTripTest, ZeroRowInput) {
  // Empty arrays (zero rows) should be handled gracefully without errors.
  auto values = BuildVariantColumn({});
  auto metadata = BuildMetadataColumn(0);

  // Primitive schema with zero rows
  auto prim_schema = VariantShreddingSchema::Primitive(int64());
  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, prim_schema));
  ASSERT_EQ(shredded->length(), 0);
  ASSERT_EQ(shredded->num_fields(), 3);

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);
  ASSERT_EQ(value_col->length(), 0);
  ASSERT_EQ(typed_col->length(), 0);

  // Round-trip on empty arrays
  ASSERT_OK_AND_ASSIGN(
      auto reconstructed,
      ReconstructVariantColumn(metadata, value_col, typed_col, prim_schema));
  ASSERT_EQ(reconstructed->length(), 0);

  // Object schema with zero rows
  auto obj_schema = VariantShreddingSchema::Object({
      {"a", VariantShreddingSchema::Primitive(int64())},
  });
  ASSERT_OK_AND_ASSIGN(auto obj_shredded,
                       ShredVariantColumn(metadata, values, obj_schema));
  ASSERT_EQ(obj_shredded->length(), 0);

  // Array schema with zero rows
  auto arr_schema =
      VariantShreddingSchema::Array(VariantShreddingSchema::Primitive(int64()));
  ASSERT_OK_AND_ASSIGN(auto arr_shredded,
                       ShredVariantColumn(metadata, values, arr_schema));
  ASSERT_EQ(arr_shredded->length(), 0);
}

// ===========================================================================
// Object shredding round-trip tests
// ===========================================================================

class VariantShredObjectTest : public ::testing::Test {
 protected:
  // Helper: build a variant column with object values
  struct ObjectRow {
    std::vector<std::pair<std::string, std::function<Status(VariantBuilder&)>>> fields;
  };

  std::shared_ptr<Array> BuildObjectColumn(const std::vector<ObjectRow>& rows) {
    BinaryBuilder array_builder;
    for (const auto& row : rows) {
      VariantBuilder vb;
      auto start = vb.Offset();
      std::vector<VariantBuilder::FieldEntry> fields;
      for (const auto& [key, value_fn] : row.fields) {
        fields.push_back(vb.NextField(start, key));
        value_fn(vb).ok();
      }
      vb.FinishObject(start, fields).ok();
      auto encoded = vb.Finish().ValueOrDie();
      array_builder
          .Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
          .ok();
    }
    std::shared_ptr<Array> result;
    array_builder.Finish(&result).ok();
    return result;
  }

  std::shared_ptr<Array> BuildMetadataForObjects(const std::vector<ObjectRow>& rows) {
    BinaryBuilder builder;
    for (const auto& row : rows) {
      VariantBuilder vb;
      auto start = vb.Offset();
      std::vector<VariantBuilder::FieldEntry> fields;
      for (const auto& [key, value_fn] : row.fields) {
        fields.push_back(vb.NextField(start, key));
        value_fn(vb).ok();
      }
      vb.FinishObject(start, fields).ok();
      auto encoded = vb.Finish().ValueOrDie();
      builder
          .Append(encoded.metadata.data(), static_cast<int32_t>(encoded.metadata.size()))
          .ok();
    }
    std::shared_ptr<Array> result;
    builder.Finish(&result).ok();
    return result;
  }
};

TEST_F(VariantShredObjectTest, FullyShredded) {
  // Object {"name": "Alice", "age": 30} shredded with schema {name, age}
  std::vector<ObjectRow> rows = {
      {{{"name", [](VariantBuilder& b) { return b.String("Alice"); }},
        {"age", [](VariantBuilder& b) { return b.Int(30); }}}},
      {{{"name", [](VariantBuilder& b) { return b.String("Bob"); }},
        {"age", [](VariantBuilder& b) { return b.Int(25); }}}},
  };

  auto values = BuildObjectColumn(rows);
  auto metadata = BuildMetadataForObjects(rows);

  auto schema = VariantShreddingSchema::Object({
      {"name", VariantShreddingSchema::Primitive(utf8())},
      {"age", VariantShreddingSchema::Primitive(int64())},
  });

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  // When fully shredded, residual value should be all null
  auto value_col = shredded->field(1);
  ASSERT_EQ(value_col->null_count(), 2);

  // typed_value is a struct with "name" and "age" fields
  auto typed_col = shredded->field(2);
  ASSERT_EQ(typed_col->type_id(), Type::STRUCT);

  // Verify native extraction: each field's struct should have typed_value populated
  auto& typed_struct = static_cast<const StructArray&>(*typed_col);

  // "name" field: sub-struct {value: binary, typed_value: string}
  auto name_field_struct = static_cast<const StructArray*>(typed_struct.field(0).get());
  auto name_value_col = name_field_struct->field(0);  // fallback value
  auto name_typed_col = name_field_struct->field(1);  // native typed_value

  // Both names should be in typed_value (string compatible with utf8 target)
  ASSERT_EQ(name_value_col->null_count(), 2);  // no fallback needed
  ASSERT_EQ(name_typed_col->null_count(), 0);  // both present
  auto& name_arr = static_cast<const StringArray&>(*name_typed_col);
  ASSERT_EQ(name_arr.GetView(0), "Alice");
  ASSERT_EQ(name_arr.GetView(1), "Bob");

  // "age" field: sub-struct {value: binary, typed_value: int64}
  auto age_field_struct = static_cast<const StructArray*>(typed_struct.field(1).get());
  auto age_value_col = age_field_struct->field(0);
  auto age_typed_col = age_field_struct->field(1);

  ASSERT_EQ(age_value_col->null_count(), 2);  // no fallback needed
  ASSERT_EQ(age_typed_col->null_count(), 0);  // both present
  auto& age_arr = static_cast<const Int64Array&>(*age_typed_col);
  ASSERT_EQ(age_arr.Value(0), 30);
  ASSERT_EQ(age_arr.Value(1), 25);

  // Reconstruct and verify round-trip
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 2);
  // Verify the reconstructed values decode correctly
  ASSERT_TRUE(reconstructed->IsValid(0));
  ASSERT_TRUE(reconstructed->IsValid(1));
}

TEST_F(VariantShredObjectTest, MissingFieldNativeExtraction) {
  // Object with missing field — verifies native extraction handles absent fields.
  // Row 0: {name: "Alice", age: 30} — both fields present
  // Row 1: {name: "Bob"} — "age" field missing
  // Row 2: {age: 42} — "name" field missing
  std::vector<ObjectRow> rows = {
      {{{"name", [](VariantBuilder& b) { return b.String("Alice"); }},
        {"age", [](VariantBuilder& b) { return b.Int(30); }}}},
      {{{"name", [](VariantBuilder& b) { return b.String("Bob"); }}}},
      {{{"age", [](VariantBuilder& b) { return b.Int(42); }}}},
  };

  auto values = BuildObjectColumn(rows);
  auto metadata = BuildMetadataForObjects(rows);

  auto schema = VariantShreddingSchema::Object({
      {"name", VariantShreddingSchema::Primitive(utf8())},
      {"age", VariantShreddingSchema::Primitive(int64())},
  });

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);
  auto& typed_struct = static_cast<const StructArray&>(*typed_col);

  // "name" field: rows 0,1 present, row 2 absent
  auto name_struct = static_cast<const StructArray*>(typed_struct.field(0).get());
  auto name_typed = name_struct->field(1);
  ASSERT_TRUE(name_typed->IsValid(0));  // "Alice"
  ASSERT_TRUE(name_typed->IsValid(1));  // "Bob"
  ASSERT_TRUE(name_typed->IsNull(2));   // missing
  auto& name_arr = static_cast<const StringArray&>(*name_typed);
  ASSERT_EQ(name_arr.GetView(0), "Alice");
  ASSERT_EQ(name_arr.GetView(1), "Bob");

  // "age" field: rows 0,2 present, row 1 absent
  auto age_struct = static_cast<const StructArray*>(typed_struct.field(1).get());
  auto age_typed = age_struct->field(1);
  ASSERT_TRUE(age_typed->IsValid(0));  // 30
  ASSERT_TRUE(age_typed->IsNull(1));   // missing
  ASSERT_TRUE(age_typed->IsValid(2));  // 42
  auto& age_arr = static_cast<const Int64Array&>(*age_typed);
  ASSERT_EQ(age_arr.Value(0), 30);
  ASSERT_EQ(age_arr.Value(2), 42);

  // Reconstruct and verify round-trip
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 3);
  ASSERT_TRUE(reconstructed->IsValid(0));
  ASSERT_TRUE(reconstructed->IsValid(1));
  ASSERT_TRUE(reconstructed->IsValid(2));
}

TEST_F(VariantShredObjectTest, PartiallyShredded) {
  // Object {"name": "Alice", "age": 30, "score": 95.5}
  // Schema only shreds "name" — "age" and "score" go to residual
  std::vector<ObjectRow> rows = {
      {{{"name", [](VariantBuilder& b) { return b.String("Alice"); }},
        {"age", [](VariantBuilder& b) { return b.Int(30); }},
        {"score", [](VariantBuilder& b) { return b.Double(95.5); }}}},
  };

  auto values = BuildObjectColumn(rows);
  auto metadata = BuildMetadataForObjects(rows);

  auto schema = VariantShreddingSchema::Object({
      {"name", VariantShreddingSchema::Primitive(utf8())},
  });

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  // Residual should have content (the "age" and "score" fields)
  auto value_col = shredded->field(1);
  ASSERT_TRUE(value_col->IsValid(0));  // has residual

  // Reconstruct
  auto typed_col = shredded->field(2);
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 1);
  ASSERT_TRUE(reconstructed->IsValid(0));
}

TEST_F(VariantShredObjectTest, NonObjectFallback) {
  // A string value (not an object) with an object schema → goes to residual
  BinaryBuilder array_builder;
  {
    VariantBuilder vb;
    vb.String("not an object").ok();
    auto encoded = vb.Finish().ValueOrDie();
    array_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  array_builder.Finish(&values).ok();

  BinaryBuilder meta_builder;
  {
    VariantBuilder vb;
    vb.String("x").ok();
    auto encoded = vb.Finish().ValueOrDie();
    meta_builder
        .Append(encoded.metadata.data(), static_cast<int32_t>(encoded.metadata.size()))
        .ok();
  }
  std::shared_ptr<Array> metadata;
  meta_builder.Finish(&metadata).ok();

  auto schema = VariantShreddingSchema::Object({
      {"name", VariantShreddingSchema::Primitive(utf8())},
  });

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  // Non-object → residual has the value, typed fields are null
  auto value_col = shredded->field(1);
  ASSERT_TRUE(value_col->IsValid(0));
}

// ===========================================================================
// Array shredding round-trip tests
// ===========================================================================

class VariantShredArrayTest : public ::testing::Test {
 protected:
  std::shared_ptr<Array> BuildMetadata(int64_t num_rows) {
    VariantBuilder vb;
    vb.Null().ok();
    auto encoded = vb.Finish().ValueOrDie();
    BinaryBuilder builder;
    for (int64_t i = 0; i < num_rows; ++i) {
      builder
          .Append(encoded.metadata.data(), static_cast<int32_t>(encoded.metadata.size()))
          .ok();
    }
    std::shared_ptr<Array> result;
    builder.Finish(&result).ok();
    return result;
  }
};

TEST_F(VariantShredArrayTest, SimpleArrayShred) {
  // Build a variant array value: [1, 2, 3]
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    auto start = vb.Offset();
    std::vector<int64_t> offsets;
    offsets.push_back(vb.NextElement(start));
    vb.Int(1).ok();
    offsets.push_back(vb.NextElement(start));
    vb.Int(2).ok();
    offsets.push_back(vb.NextElement(start));
    vb.Int(3).ok();
    vb.FinishArray(start, offsets).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadata(1);

  auto schema = VariantShreddingSchema::Array(VariantShreddingSchema::Primitive(int64()));

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  // Array goes to typed_value (a list), value is null
  auto value_col = shredded->field(1);
  ASSERT_TRUE(value_col->IsNull(0));

  auto typed_col = shredded->field(2);
  ASSERT_EQ(typed_col->type_id(), Type::LIST);
  auto& list = static_cast<const ListArray&>(*typed_col);
  ASSERT_EQ(list.value_length(0), 3);  // 3 elements

  // With recursive element shredding, elements are struct{value, typed_value}
  // where compatible int values go to typed_value and value is null.
  auto elem_type = list.value_type();
  ASSERT_EQ(elem_type->id(), Type::STRUCT);
  auto* elem_struct_type = static_cast<const StructType*>(elem_type.get());
  ASSERT_EQ(elem_struct_type->num_fields(), 2);
  ASSERT_EQ(elem_struct_type->field(0)->name(), "value");
  ASSERT_EQ(elem_struct_type->field(1)->name(), "typed_value");

  // Verify native int64 values in the element typed_value column
  auto* elem_struct = static_cast<const StructArray*>(list.values().get());
  auto elem_value_col = elem_struct->field(0);  // per-element residual
  auto elem_typed_col = elem_struct->field(1);  // per-element typed_value
  // All 3 ints should be in typed_value
  ASSERT_EQ(elem_value_col->null_count(), 3);
  ASSERT_EQ(elem_typed_col->null_count(), 0);
  auto& elem_int_arr = static_cast<const Int64Array&>(*elem_typed_col);
  ASSERT_EQ(elem_int_arr.Value(0), 1);
  ASSERT_EQ(elem_int_arr.Value(1), 2);
  ASSERT_EQ(elem_int_arr.Value(2), 3);

  // Round-trip reconstruction
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 1);
  ASSERT_TRUE(reconstructed->IsValid(0));
}

TEST_F(VariantShredArrayTest, ArrayShredMixedElements) {
  // Build a variant array value: [1, "hello", 3] — mixed types with int64 schema.
  // Int elements should go to typed_value, string element to per-element value.
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    auto start = vb.Offset();
    std::vector<int64_t> offsets;
    offsets.push_back(vb.NextElement(start));
    vb.Int(1).ok();
    offsets.push_back(vb.NextElement(start));
    vb.String("hello").ok();
    offsets.push_back(vb.NextElement(start));
    vb.Int(3).ok();
    vb.FinishArray(start, offsets).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadata(1);

  auto schema = VariantShreddingSchema::Array(VariantShreddingSchema::Primitive(int64()));

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  // The array is shredded — residual value is null, typed_value has the list
  ASSERT_TRUE(value_col->IsNull(0));
  ASSERT_EQ(typed_col->type_id(), Type::LIST);

  auto& list = static_cast<const ListArray&>(*typed_col);
  auto* elem_struct = static_cast<const StructArray*>(list.values().get());
  auto elem_value_col = elem_struct->field(0);
  auto elem_typed_col = elem_struct->field(1);

  // Element 0 (Int(1)) → typed_value=1, value=null
  ASSERT_TRUE(elem_value_col->IsNull(0));
  ASSERT_TRUE(elem_typed_col->IsValid(0));
  auto& elem_ints = static_cast<const Int64Array&>(*elem_typed_col);
  ASSERT_EQ(elem_ints.Value(0), 1);

  // Element 1 (String("hello")) → typed_value=null, value=variant bytes
  ASSERT_TRUE(elem_value_col->IsValid(1));
  ASSERT_TRUE(elem_typed_col->IsNull(1));

  // Element 2 (Int(3)) → typed_value=3, value=null
  ASSERT_TRUE(elem_value_col->IsNull(2));
  ASSERT_TRUE(elem_typed_col->IsValid(2));
  ASSERT_EQ(elem_ints.Value(2), 3);

  // Round-trip reconstruction
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 1);
  ASSERT_TRUE(reconstructed->IsValid(0));

  // Verify the reconstructed value decodes to an array with the expected elements
  auto recon_bytes = GetBinaryView(*reconstructed, 0);
  auto* data = reinterpret_cast<const uint8_t*>(recon_bytes.data());
  auto len = static_cast<int64_t>(recon_bytes.size());
  ASSERT_GE(len, 1);
  ASSERT_EQ(GetBasicType(data[0]), BasicType::kArray);
  ASSERT_OK_AND_ASSIGN(auto elem_count, GetArrayElementCount(data, len));
  ASSERT_EQ(elem_count, 3);
}

TEST_F(VariantShredArrayTest, NonArrayFallback) {
  // A string value with an array schema → goes to residual
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    vb.String("not an array").ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadata(1);

  auto schema = VariantShreddingSchema::Array(VariantShreddingSchema::Primitive(int64()));

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  // Non-array → residual has the value, typed_value is null
  auto value_col = shredded->field(1);
  ASSERT_TRUE(value_col->IsValid(0));

  auto typed_col = shredded->field(2);
  ASSERT_TRUE(typed_col->IsNull(0));
}

// ===========================================================================
// Additional round-trip tests for specific types (Decimal128, UUID, Timestamps)
// ===========================================================================

class VariantShredTypedRoundTripTest : public ::testing::Test {
 protected:
  std::shared_ptr<Array> BuildMetadataColumn(int64_t num_rows) {
    VariantBuilder vb;
    vb.Null().ok();
    auto encoded = vb.Finish().ValueOrDie();
    BinaryBuilder builder;
    for (int64_t i = 0; i < num_rows; ++i) {
      builder
          .Append(encoded.metadata.data(), static_cast<int32_t>(encoded.metadata.size()))
          .ok();
    }
    std::shared_ptr<Array> result;
    builder.Finish(&result).ok();
    return result;
  }
};

TEST_F(VariantShredTypedRoundTripTest, Decimal128RoundTrip) {
  // Build variant column with decimal values (scale=2)
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    // Encode 123.45 as decimal4 with scale 2 → unscaled 12345
    uint8_t scale = 2;
    int32_t unscaled = 12345;
    uint8_t bytes[4];
    std::memcpy(bytes, &unscaled, 4);
    vb.Decimal4(scale, bytes).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  {
    VariantBuilder vb;
    // Encode -999.99 as decimal4 with scale 2 → unscaled -99999
    uint8_t scale = 2;
    int32_t unscaled = -99999;
    uint8_t bytes[4];
    std::memcpy(bytes, &unscaled, 4);
    vb.Decimal4(scale, bytes).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadataColumn(2);

  // Schema: Decimal128(10, 2)
  auto schema = VariantShreddingSchema::Primitive(decimal128(10, 2));

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  // Both values should match (scale=2 matches schema)
  ASSERT_EQ(value_col->null_count(), 2);  // all in typed
  ASSERT_EQ(typed_col->null_count(), 0);  // all present

  // Verify native decimal values
  auto& dec_arr = static_cast<const Decimal128Array&>(*typed_col);
  Decimal128 val0(dec_arr.GetValue(0));
  Decimal128 val1(dec_arr.GetValue(1));
  ASSERT_EQ(val0, Decimal128(12345));
  ASSERT_EQ(val1, Decimal128(-99999));

  // Round-trip
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 2);
  ASSERT_TRUE(reconstructed->IsValid(0));
  ASSERT_TRUE(reconstructed->IsValid(1));
}

TEST_F(VariantShredTypedRoundTripTest, Decimal128ScaleMismatch) {
  // Build variant column with decimal scale=3, but schema wants scale=2
  // Should NOT be shredded (scale mismatch → goes to residual)
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    uint8_t scale = 3;
    int32_t unscaled = 12345;
    uint8_t bytes[4];
    std::memcpy(bytes, &unscaled, 4);
    vb.Decimal4(scale, bytes).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadataColumn(1);

  auto schema = VariantShreddingSchema::Primitive(decimal128(10, 2));

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  // Scale mismatch → value has content, typed is null
  ASSERT_TRUE(value_col->IsValid(0));
  ASSERT_TRUE(typed_col->IsNull(0));
}

TEST_F(VariantShredTypedRoundTripTest, UUIDRoundTrip) {
  // Build variant column with UUID values
  BinaryBuilder value_builder;
  uint8_t uuid1[16] = {0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF,
                       0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10};
  uint8_t uuid2[16] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  {
    VariantBuilder vb;
    vb.UUID(uuid1).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  {
    VariantBuilder vb;
    vb.UUID(uuid2).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadataColumn(2);

  auto schema = VariantShreddingSchema::Primitive(fixed_size_binary(16));

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  // Both UUIDs should be in typed_value
  ASSERT_EQ(value_col->null_count(), 2);
  ASSERT_EQ(typed_col->null_count(), 0);

  // Verify bytes
  auto& fsb_arr = static_cast<const FixedSizeBinaryArray&>(*typed_col);
  ASSERT_EQ(std::memcmp(fsb_arr.GetValue(0), uuid1, 16), 0);
  ASSERT_EQ(std::memcmp(fsb_arr.GetValue(1), uuid2, 16), 0);

  // Round-trip
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 2);
  ASSERT_TRUE(reconstructed->IsValid(0));
  ASSERT_TRUE(reconstructed->IsValid(1));
}

TEST_F(VariantShredTypedRoundTripTest, TimestampMicrosRoundTrip) {
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    vb.TimestampMicros(1654041600000000LL).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadataColumn(1);

  auto schema = VariantShreddingSchema::Primitive(timestamp(TimeUnit::MICRO, "UTC"));

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  ASSERT_EQ(value_col->null_count(), 1);
  ASSERT_EQ(typed_col->null_count(), 0);

  // Verify int64 value stored
  auto& int_arr = static_cast<const Int64Array&>(*typed_col);
  ASSERT_EQ(int_arr.Value(0), 1654041600000000LL);

  // Round-trip: should reconstruct as TimestampMicros (not NTZ or Nanos)
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 1);
  ASSERT_TRUE(reconstructed->IsValid(0));

  // Verify the variant bytes encode as TimestampMicros (header byte check)
  auto recon_bytes = GetBinaryView(*reconstructed, 0);
  ASSERT_GE(recon_bytes.size(), 1);
  uint8_t header = static_cast<uint8_t>(recon_bytes[0]);
  // PrimitiveType::kTimestampMicros = 12, encoded as (12 << 2) | 0 = 0x30
  ASSERT_EQ(header, 0x30);
}

TEST_F(VariantShredTypedRoundTripTest, TimestampNanosRoundTrip) {
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    vb.TimestampNanos(1654041600000000000LL).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadataColumn(1);

  // Schema says NANO resolution with timezone
  auto schema = VariantShreddingSchema::Primitive(timestamp(TimeUnit::NANO, "UTC"));

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  ASSERT_EQ(value_col->null_count(), 1);
  ASSERT_EQ(typed_col->null_count(), 0);

  // Round-trip: should reconstruct as TimestampNanos (not Micros)
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 1);
  ASSERT_TRUE(reconstructed->IsValid(0));

  // Verify the variant bytes encode as TimestampNanos
  auto recon_bytes = GetBinaryView(*reconstructed, 0);
  ASSERT_GE(recon_bytes.size(), 1);
  uint8_t header = static_cast<uint8_t>(recon_bytes[0]);
  // PrimitiveType::kTimestampNanos = 18, encoded as (18 << 2) | 0 = 0x48
  ASSERT_EQ(header, 0x48);
}

TEST_F(VariantShredTypedRoundTripTest, TimestampMicrosNTZRoundTrip) {
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    vb.TimestampMicrosNTZ(1654041600000000LL).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadataColumn(1);

  // No timezone → NTZ
  auto schema = VariantShreddingSchema::Primitive(timestamp(TimeUnit::MICRO));

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  ASSERT_EQ(value_col->null_count(), 1);
  ASSERT_EQ(typed_col->null_count(), 0);

  // Round-trip: should reconstruct as TimestampMicrosNTZ
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 1);
  ASSERT_TRUE(reconstructed->IsValid(0));

  // Verify: PrimitiveType::kTimestampMicrosNTZ = 13, encoded as (13 << 2) | 0 = 0x34
  auto recon_bytes = GetBinaryView(*reconstructed, 0);
  ASSERT_GE(recon_bytes.size(), 1);
  uint8_t header = static_cast<uint8_t>(recon_bytes[0]);
  ASSERT_EQ(header, 0x34);
}

TEST_F(VariantShredTypedRoundTripTest, TimestampNanosNTZRoundTrip) {
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    vb.TimestampNanosNTZ(1654041600000000000LL).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadataColumn(1);

  // Nano resolution, no timezone → NanosNTZ
  auto schema = VariantShreddingSchema::Primitive(timestamp(TimeUnit::NANO));

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  ASSERT_EQ(value_col->null_count(), 1);
  ASSERT_EQ(typed_col->null_count(), 0);

  // Round-trip: should reconstruct as TimestampNanosNTZ
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 1);
  ASSERT_TRUE(reconstructed->IsValid(0));

  // Verify: PrimitiveType::kTimestampNanosNTZ = 19, encoded as (19 << 2) | 0 = 0x4C
  auto recon_bytes = GetBinaryView(*reconstructed, 0);
  ASSERT_GE(recon_bytes.size(), 1);
  uint8_t header = static_cast<uint8_t>(recon_bytes[0]);
  ASSERT_EQ(header, 0x4C);
}

TEST_F(VariantShredTypedRoundTripTest, FloatWidenedToDoubleRoundTrip) {
  // Float variant shredded into a Double column — exercises Float→Double widening.
  // The value precision is preserved (float→double is lossless for the numeric value),
  // but the variant type tag changes: Float→Double on reconstruction.
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    vb.Float(3.14f).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadataColumn(1);

  // Target is Double — Float should be compatible (widening)
  auto schema = VariantShreddingSchema::Primitive(float64());

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  // Float should be shredded into the Double typed_value column
  ASSERT_EQ(value_col->null_count(), 1);  // value is null (shredded)
  ASSERT_EQ(typed_col->null_count(), 0);  // typed_value is present

  // Verify the stored double value matches the original float value
  auto& dbl_arr = static_cast<const DoubleArray&>(*typed_col);
  ASSERT_DOUBLE_EQ(dbl_arr.Value(0), static_cast<double>(3.14f));

  // Round-trip: reconstructed variant will be Double, not Float
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 1);
  ASSERT_TRUE(reconstructed->IsValid(0));

  // Verify: PrimitiveType::kDouble = 7, encoded as (7 << 2) | 0 = 0x1C
  auto recon_bytes = GetBinaryView(*reconstructed, 0);
  ASSERT_GE(recon_bytes.size(), 1);
  uint8_t header = static_cast<uint8_t>(recon_bytes[0]);
  ASSERT_EQ(header, 0x1C);  // Double, not Float (0x38)
}

TEST_F(VariantShredTypedRoundTripTest, Int8ShredTargetRoundTrip) {
  // Int8 variant shredded into an Int8 column
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    vb.Int8(42).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  {
    // Int16 variant should NOT match Int8 target (no narrowing)
    VariantBuilder vb;
    vb.Int16(300).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadataColumn(2);

  auto schema = VariantShreddingSchema::Primitive(int8());

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  // Row 0 (Int8(42)) → typed_value; Row 1 (Int16(300)) → value (no narrowing)
  ASSERT_TRUE(value_col->IsNull(0));
  ASSERT_TRUE(typed_col->IsValid(0));
  ASSERT_TRUE(value_col->IsValid(1));
  ASSERT_TRUE(typed_col->IsNull(1));

  // Verify native value
  auto& int_arr = static_cast<const Int8Array&>(*typed_col);
  ASSERT_EQ(int_arr.Value(0), 42);

  // Round-trip
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 2);
  ASSERT_TRUE(reconstructed->IsValid(0));
  ASSERT_TRUE(reconstructed->IsValid(1));
}

TEST_F(VariantShredTypedRoundTripTest, Int16ShredTargetRoundTrip) {
  // Int8 and Int16 variants shredded into an Int16 column
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    vb.Int8(42).ok();  // Int8 → compatible with Int16
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  {
    VariantBuilder vb;
    vb.Int16(300).ok();  // Int16 → compatible with Int16
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  {
    VariantBuilder vb;
    vb.Int32(100000).ok();  // Int32 → NOT compatible with Int16 (no narrowing)
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadataColumn(3);

  auto schema = VariantShreddingSchema::Primitive(int16());

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  // Rows 0,1 → typed; Row 2 → value
  ASSERT_TRUE(value_col->IsNull(0));
  ASSERT_TRUE(typed_col->IsValid(0));
  ASSERT_TRUE(value_col->IsNull(1));
  ASSERT_TRUE(typed_col->IsValid(1));
  ASSERT_TRUE(value_col->IsValid(2));
  ASSERT_TRUE(typed_col->IsNull(2));

  auto& int_arr = static_cast<const Int16Array&>(*typed_col);
  ASSERT_EQ(int_arr.Value(0), 42);
  ASSERT_EQ(int_arr.Value(1), 300);

  // Round-trip
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 3);
}

TEST_F(VariantShredTypedRoundTripTest, LargeStringShredRoundTrip) {
  // String variant shredded into a LARGE_STRING column
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    vb.String("hello world").ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  {
    // Non-string should NOT match LARGE_STRING target
    VariantBuilder vb;
    vb.Int(42).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadataColumn(2);

  auto schema = VariantShreddingSchema::Primitive(large_utf8());

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  // Row 0 (String) → typed_value; Row 1 (Int) → value
  ASSERT_TRUE(value_col->IsNull(0));
  ASSERT_TRUE(typed_col->IsValid(0));
  ASSERT_TRUE(value_col->IsValid(1));
  ASSERT_TRUE(typed_col->IsNull(1));

  // Verify native value
  auto& str_arr = static_cast<const LargeStringArray&>(*typed_col);
  ASSERT_EQ(str_arr.GetView(0), "hello world");

  // Round-trip
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 2);
  ASSERT_TRUE(reconstructed->IsValid(0));
  ASSERT_TRUE(reconstructed->IsValid(1));

  // Verify reconstructed string has correct variant header (short string: length in
  // header)
  auto recon_bytes = GetBinaryView(*reconstructed, 0);
  ASSERT_GE(recon_bytes.size(), 1);
  uint8_t header = static_cast<uint8_t>(recon_bytes[0]);
  // "hello world" = 11 bytes, short string header = (11 << 2) | 1 = 0x2D
  ASSERT_EQ(header, 0x2D);
}

TEST_F(VariantShredTypedRoundTripTest, LargeBinaryShredRoundTrip) {
  // Binary variant shredded into a LARGE_BINARY column
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    vb.Binary(std::string_view("\x00\x01\x02\x03", 4)).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  {
    // Non-binary should NOT match LARGE_BINARY target
    VariantBuilder vb;
    vb.String("not binary").ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadataColumn(2);

  auto schema = VariantShreddingSchema::Primitive(large_binary());

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  // Row 0 (Binary) → typed_value; Row 1 (String) → value
  ASSERT_TRUE(value_col->IsNull(0));
  ASSERT_TRUE(typed_col->IsValid(0));
  ASSERT_TRUE(value_col->IsValid(1));
  ASSERT_TRUE(typed_col->IsNull(1));

  // Verify native value
  auto& bin_arr = static_cast<const LargeBinaryArray&>(*typed_col);
  auto bin_view = bin_arr.GetView(0);
  ASSERT_EQ(bin_view.size(), 4);
  ASSERT_EQ(std::memcmp(bin_view.data(), "\x00\x01\x02\x03", 4), 0);

  // Round-trip
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 2);
  ASSERT_TRUE(reconstructed->IsValid(0));
  ASSERT_TRUE(reconstructed->IsValid(1));
}

// ===========================================================================
// Error handling / invalid input tests
// ===========================================================================

class VariantShredErrorTest : public ::testing::Test {
 protected:
  std::shared_ptr<Array> BuildMetadataColumn(int64_t num_rows) {
    VariantBuilder vb;
    vb.Null().ok();
    auto encoded = vb.Finish().ValueOrDie();
    BinaryBuilder builder;
    for (int64_t i = 0; i < num_rows; ++i) {
      builder
          .Append(encoded.metadata.data(), static_cast<int32_t>(encoded.metadata.size()))
          .ok();
    }
    std::shared_ptr<Array> result;
    builder.Finish(&result).ok();
    return result;
  }
};

TEST_F(VariantShredErrorTest, ReconstructBothNonNullPrimitiveSchema) {
  // Reconstruction should error if both value and typed_value are non-null
  // for a primitive schema (this is an invalid shredded state).
  auto metadata = BuildMetadataColumn(1);
  auto schema = VariantShreddingSchema::Primitive(int64());

  // Build a value column with a valid value
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    vb.Int(42).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> value_col;
  value_builder.Finish(&value_col).ok();

  // Build a typed_value column that is also non-null
  Int64Builder typed_builder;
  typed_builder.Append(99).ok();
  std::shared_ptr<Array> typed_col;
  typed_builder.Finish(&typed_col).ok();

  // Should return Status::Invalid (both non-null is invalid for primitives)
  auto result = ReconstructVariantColumn(metadata, value_col, typed_col, schema);
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsInvalid());
}

TEST_F(VariantShredErrorTest, ShredUnsupportedTargetType) {
  // Shredding with an unsupported target type should return NotImplemented
  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    vb.Int(42).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();
  auto metadata = BuildMetadataColumn(1);

  // duration() is not a valid shredding target
  auto schema = VariantShreddingSchema::Primitive(duration(TimeUnit::MICRO));
  auto result = ShredVariantColumn(metadata, values, schema);
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsNotImplemented());
}

TEST_F(VariantShredErrorTest, ShredInvalidMetadataArrayType) {
  // metadata_array must be BINARY, LARGE_BINARY, or BINARY_VIEW
  Int64Builder int_builder;
  int_builder.Append(42).ok();
  std::shared_ptr<Array> bad_metadata;
  int_builder.Finish(&bad_metadata).ok();

  BinaryBuilder value_builder;
  {
    VariantBuilder vb;
    vb.Int(1).ok();
    auto encoded = vb.Finish().ValueOrDie();
    value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();

  auto schema = VariantShreddingSchema::Primitive(int64());
  auto result = ShredVariantColumn(bad_metadata, values, schema);
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsInvalid());
}

TEST_F(VariantShredErrorTest, ReconstructInvalidValueArrayType) {
  // value_array must be BINARY, LARGE_BINARY, or BINARY_VIEW for reconstruction
  auto metadata = BuildMetadataColumn(1);

  Int64Builder int_builder;
  int_builder.Append(99).ok();
  std::shared_ptr<Array> bad_value;
  int_builder.Finish(&bad_value).ok();

  Int64Builder typed_builder;
  typed_builder.AppendNull().ok();
  std::shared_ptr<Array> typed_col;
  typed_builder.Finish(&typed_col).ok();

  auto schema = VariantShreddingSchema::Primitive(int64());
  auto result = ReconstructVariantColumn(metadata, bad_value, typed_col, schema);
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsInvalid());
}

TEST_F(VariantShredErrorTest, ReconstructArrayTypedValueNotList) {
  // For array schemas, typed_value must be a LIST or LARGE_LIST
  auto metadata = BuildMetadataColumn(1);

  BinaryBuilder value_builder;
  value_builder.AppendNull().ok();
  std::shared_ptr<Array> value_col;
  value_builder.Finish(&value_col).ok();

  // Pass an Int64Array instead of a ListArray
  Int64Builder typed_builder;
  typed_builder.Append(42).ok();
  std::shared_ptr<Array> typed_col;
  typed_builder.Finish(&typed_col).ok();

  auto schema = VariantShreddingSchema::Array(VariantShreddingSchema::Primitive(int64()));
  auto result = ReconstructVariantColumn(metadata, value_col, typed_col, schema);
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsInvalid());
}

TEST_F(VariantShredErrorTest, ReconstructObjectTypedValueNotStruct) {
  // For object schemas, typed_value must be a StructArray
  auto metadata = BuildMetadataColumn(1);

  BinaryBuilder value_builder;
  value_builder.AppendNull().ok();
  std::shared_ptr<Array> value_col;
  value_builder.Finish(&value_col).ok();

  // Pass an Int64Array instead of a StructArray
  Int64Builder typed_builder;
  typed_builder.Append(42).ok();
  std::shared_ptr<Array> typed_col;
  typed_builder.Finish(&typed_col).ok();

  auto schema = VariantShreddingSchema::Object({
      {"name", VariantShreddingSchema::Primitive(utf8())},
  });
  auto result = ReconstructVariantColumn(metadata, value_col, typed_col, schema);
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsInvalid());
}

TEST_F(VariantShredErrorTest, ReconstructObjectFieldCountMismatch) {
  // typed_value struct has fewer fields than the schema expects
  auto metadata = BuildMetadataColumn(1);

  BinaryBuilder value_builder;
  value_builder.AppendNull().ok();
  std::shared_ptr<Array> value_col;
  value_builder.Finish(&value_col).ok();

  // Build a struct with only 1 field, but schema expects 2
  BinaryBuilder inner_value_builder;
  inner_value_builder.AppendNull().ok();
  std::shared_ptr<Array> inner_value;
  inner_value_builder.Finish(&inner_value).ok();

  Int64Builder inner_typed_builder;
  inner_typed_builder.AppendNull().ok();
  std::shared_ptr<Array> inner_typed;
  inner_typed_builder.Finish(&inner_typed).ok();

  auto inner_fields = std::vector<std::shared_ptr<Field>>{
      field("value", binary(), true),
      field("typed_value", int64(), true),
  };
  ASSERT_OK_AND_ASSIGN(auto single_field_struct,
                       StructArray::Make({inner_value, inner_typed}, inner_fields));

  // Wrap into outer struct with only 1 field ("name")
  auto outer_fields = std::vector<std::shared_ptr<Field>>{
      field("name", single_field_struct->type(), false),
  };
  ASSERT_OK_AND_ASSIGN(auto typed_col,
                       StructArray::Make({single_field_struct}, outer_fields));

  // Schema expects 2 fields: name + age
  auto schema = VariantShreddingSchema::Object({
      {"name", VariantShreddingSchema::Primitive(utf8())},
      {"age", VariantShreddingSchema::Primitive(int64())},
  });
  auto result = ReconstructVariantColumn(metadata, value_col, typed_col, schema);
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsInvalid());
}

// ===========================================================================
// StringView / BinaryView shredding tests
// ===========================================================================

TEST_F(VariantShredRoundTripTest, StringViewShredRoundTrip) {
  // String variant values shredded into StringView typed column
  auto values = BuildVariantColumn({
      [](VariantBuilder& b) { return b.String("hello"); },
      [](VariantBuilder& b) { return b.Int(42); },  // doesn't match
      [](VariantBuilder& b) { return b.String("world"); },
  });
  auto metadata = BuildMetadataColumn(3);
  auto schema = VariantShreddingSchema::Primitive(utf8_view());

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  // Rows 0,2 match (strings); Row 1 doesn't (int)
  ASSERT_TRUE(value_col->IsNull(0));
  ASSERT_TRUE(value_col->IsValid(1));
  ASSERT_TRUE(value_col->IsNull(2));

  ASSERT_TRUE(typed_col->IsValid(0));
  ASSERT_TRUE(typed_col->IsNull(1));
  ASSERT_TRUE(typed_col->IsValid(2));

  // Verify typed column is StringViewArray
  ASSERT_EQ(typed_col->type_id(), Type::STRING_VIEW);
  auto& sv_arr = static_cast<const StringViewArray&>(*typed_col);
  ASSERT_EQ(sv_arr.GetView(0), "hello");
  ASSERT_EQ(sv_arr.GetView(2), "world");

  // Round-trip via reconstruction
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 3);
  // Verify reconstructed strings have correct short-string header byte
  auto recon_bytes0 = GetBinaryView(*reconstructed, 0);
  // Short string "hello" (5 chars): header = (5 << 2) | 0x01 = 0x15
  ASSERT_EQ(recon_bytes0.size(), 6);
  ASSERT_EQ(static_cast<uint8_t>(recon_bytes0[0]), 0x15);
}

TEST_F(VariantShredRoundTripTest, BinaryViewShredRoundTrip) {
  // Binary variant values shredded into BinaryView typed column
  auto values = BuildVariantColumn({
      [](VariantBuilder& b) { return b.Binary(std::string_view("\x01\x02\x03", 3)); },
      [](VariantBuilder& b) { return b.String("text"); },  // doesn't match binary
      [](VariantBuilder& b) { return b.Binary(std::string_view("\xAA\xBB", 2)); },
  });
  auto metadata = BuildMetadataColumn(3);
  auto schema = VariantShreddingSchema::Primitive(binary_view());

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  // Rows 0,2 match (binary); Row 1 doesn't (string)
  ASSERT_TRUE(value_col->IsNull(0));
  ASSERT_TRUE(value_col->IsValid(1));
  ASSERT_TRUE(value_col->IsNull(2));

  ASSERT_TRUE(typed_col->IsValid(0));
  ASSERT_TRUE(typed_col->IsNull(1));
  ASSERT_TRUE(typed_col->IsValid(2));

  // Verify typed column is BinaryViewArray
  ASSERT_EQ(typed_col->type_id(), Type::BINARY_VIEW);
  auto& bv_arr = static_cast<const BinaryViewArray&>(*typed_col);
  auto v0 = bv_arr.GetView(0);
  ASSERT_EQ(v0.size(), 3);
  ASSERT_EQ(v0[0], '\x01');
  ASSERT_EQ(v0[1], '\x02');
  ASSERT_EQ(v0[2], '\x03');

  // Round-trip via reconstruction
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 3);
}

TEST_F(VariantShredRoundTripTest, ShortStringToStringView) {
  // Short strings (≤63 bytes) should be compatible with StringView target
  auto values = BuildVariantColumn({
      [](VariantBuilder& b) { return b.String("hi"); },  // short string
  });
  auto metadata = BuildMetadataColumn(1);
  auto schema = VariantShreddingSchema::Primitive(utf8_view());

  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));

  auto typed_col = shredded->field(2);
  ASSERT_EQ(typed_col->type_id(), Type::STRING_VIEW);
  ASSERT_TRUE(typed_col->IsValid(0));
  auto& sv_arr = static_cast<const StringViewArray&>(*typed_col);
  ASSERT_EQ(sv_arr.GetView(0), "hi");
}

// ===========================================================================
// LargeList reconstruction tests
// ===========================================================================

TEST_F(VariantShredRoundTripTest, LargeListReconstructRoundTrip) {
  // Shred an array, then construct a LargeListArray with equivalent data
  // and verify reconstruction works with 64-bit offsets.
  auto values = BuildVariantColumn({
      [](VariantBuilder& b) {
        auto s = b.Offset();
        std::vector<int64_t> offsets;
        offsets.push_back(b.NextElement(s));
        b.Int(10).ok();
        offsets.push_back(b.NextElement(s));
        b.Int(20).ok();
        return b.FinishArray(s, offsets);
      },
  });
  auto metadata = BuildMetadataColumn(1);
  auto schema = VariantShreddingSchema::Array(VariantShreddingSchema::Primitive(int64()));

  // First shred normally (produces ListArray with struct elements)
  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));
  auto value_col = shredded->field(1);  // residual (null for arrays)
  auto typed_col = shredded->field(2);  // ListArray<struct{value, typed_value}>

  ASSERT_EQ(typed_col->type_id(), Type::LIST);

  // Reconstruct from the normal ListArray (sanity check)
  ASSERT_OK_AND_ASSIGN(auto recon1,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(recon1->length(), 1);

  // Now build a LargeListArray with the same element struct data.
  // The elements are struct{value: binary, typed_value: int64}.
  const auto* list_arr = static_cast<const ListArray*>(typed_col.get());
  auto elem_struct = list_arr->values();  // StructArray

  // Construct a LargeList with same struct elements using 64-bit offsets.
  auto large_offsets = Buffer::FromVector(std::vector<int64_t>{0, 2});
  auto large_list_arr = std::make_shared<LargeListArray>(
      large_list(field("element", elem_struct->type(), false)), 1, large_offsets,
      elem_struct);
  ASSERT_EQ(large_list_arr->type_id(), Type::LARGE_LIST);

  // Reconstruct from LargeListArray
  ASSERT_OK_AND_ASSIGN(
      auto recon2, ReconstructVariantColumn(metadata, value_col, large_list_arr, schema));
  ASSERT_EQ(recon2->length(), 1);

  // Verify both reconstructions produce identical output
  auto bytes1 = GetBinaryView(*recon1, 0);
  auto bytes2 = GetBinaryView(*recon2, 0);
  ASSERT_EQ(bytes1, bytes2);
}

TEST_F(VariantShredRoundTripTest, ReconstructArrayTypedValueLargeListAccepted) {
  // Verify that LargeList is accepted alongside List for array reconstruction.
  // This tests the validation path.
  auto metadata = BuildMetadataColumn(0);
  BinaryBuilder vb;
  std::shared_ptr<Array> value_col;
  vb.Finish(&value_col).ok();

  // Empty LargeList of binary
  auto large_list_builder = std::make_shared<LargeListBuilder>(
      default_memory_pool(), std::make_shared<BinaryBuilder>());
  std::shared_ptr<Array> typed_col;
  large_list_builder->Finish(&typed_col).ok();

  auto schema = VariantShreddingSchema::Array(VariantShreddingSchema::Primitive(int64()));

  // Should succeed (empty arrays, no actual reconstruction needed)
  auto result = ReconstructVariantColumn(metadata, value_col, typed_col, schema);
  ASSERT_TRUE(result.ok());
}

TEST_F(VariantShredRoundTripTest, ReconstructArrayFixedSizeListAccepted) {
  // Verify that FixedSizeList is accepted for array reconstruction.
  // Build a FixedSizeList(2) of binary variant bytes and reconstruct.
  auto metadata = BuildMetadataColumn(1);

  // value_col is null (array went to typed_value)
  BinaryBuilder value_builder;
  value_builder.AppendNull().ok();
  std::shared_ptr<Array> value_col;
  value_builder.Finish(&value_col).ok();

  // Build element binary values: Int(10), Int(20)
  BinaryBuilder elem_builder;
  {
    VariantBuilder vb;
    vb.Int(10).ok();
    auto encoded = vb.Finish().ValueOrDie();
    elem_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  {
    VariantBuilder vb;
    vb.Int(20).ok();
    auto encoded = vb.Finish().ValueOrDie();
    elem_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> elem_arr;
  elem_builder.Finish(&elem_arr).ok();

  // Build FixedSizeList(2) containing the 2 elements
  auto typed_col = std::make_shared<FixedSizeListArray>(
      fixed_size_list(field("item", binary()), 2), 1, elem_arr);

  auto schema = VariantShreddingSchema::Array(VariantShreddingSchema::Primitive(int64()));

  // Should succeed
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 1);
  ASSERT_TRUE(reconstructed->IsValid(0));

  // Verify reconstructed is a variant array with 2 elements
  auto recon_bytes = GetBinaryView(*reconstructed, 0);
  auto* data = reinterpret_cast<const uint8_t*>(recon_bytes.data());
  auto len = static_cast<int64_t>(recon_bytes.size());
  ASSERT_GE(len, 1);
  ASSERT_EQ(GetBasicType(data[0]), BasicType::kArray);
  ASSERT_OK_AND_ASSIGN(auto elem_count, GetArrayElementCount(data, len));
  ASSERT_EQ(elem_count, 2);
}

TEST_F(VariantShredRoundTripTest, ReconstructArrayListViewAccepted) {
  // Verify that ListView is accepted for array reconstruction.
  auto metadata = BuildMetadataColumn(1);

  // value_col is null
  BinaryBuilder value_builder;
  value_builder.AppendNull().ok();
  std::shared_ptr<Array> value_col;
  value_builder.Finish(&value_col).ok();

  // Build element binary values: Int(5), Int(6), Int(7)
  BinaryBuilder elem_builder;
  for (int val : {5, 6, 7}) {
    VariantBuilder vb;
    vb.Int(val).ok();
    auto encoded = vb.Finish().ValueOrDie();
    elem_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
        .ok();
  }
  std::shared_ptr<Array> elem_arr;
  elem_builder.Finish(&elem_arr).ok();

  // Build a ListView array: 1 row pointing to elements [0, 3) (all 3 elements)
  // ListView needs offsets buffer + sizes buffer
  auto offsets_buf = Buffer::FromVector<int32_t>({0});
  auto sizes_buf = Buffer::FromVector<int32_t>({3});
  auto list_view_type = list_view(field("item", binary()));
  auto typed_col = std::make_shared<ListViewArray>(list_view_type, 1, offsets_buf,
                                                   sizes_buf, elem_arr);

  auto schema = VariantShreddingSchema::Array(VariantShreddingSchema::Primitive(int64()));

  // Should succeed
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 1);
  ASSERT_TRUE(reconstructed->IsValid(0));

  // Verify reconstructed is a variant array with 3 elements
  auto recon_bytes = GetBinaryView(*reconstructed, 0);
  auto* data = reinterpret_cast<const uint8_t*>(recon_bytes.data());
  auto len = static_cast<int64_t>(recon_bytes.size());
  ASSERT_GE(len, 1);
  ASSERT_EQ(GetBasicType(data[0]), BasicType::kArray);
  ASSERT_OK_AND_ASSIGN(auto elem_count, GetArrayElementCount(data, len));
  ASSERT_EQ(elem_count, 3);
}

TEST_F(VariantShredRoundTripTest, StringViewMetadataArrayInput) {
  // Verify that STRING_VIEW metadata arrays are accepted by ShredVariantColumn.
  // Arrow's BinaryViewArray/StringViewArray are valid metadata containers.
  VariantBuilder vb;
  vb.Int(42).ok();
  auto encoded = vb.Finish().ValueOrDie();

  // Build a BinaryView metadata array (STRING_VIEW has the same binary layout
  // and is accepted by GetBinaryValue)
  BinaryViewBuilder meta_builder;
  meta_builder
      .Append(encoded.metadata.data(), static_cast<int64_t>(encoded.metadata.size()))
      .ok();
  std::shared_ptr<Array> metadata;
  meta_builder.Finish(&metadata).ok();
  ASSERT_EQ(metadata->type_id(), Type::BINARY_VIEW);

  // Build value array as regular binary
  BinaryBuilder value_builder;
  value_builder.Append(encoded.value.data(), static_cast<int32_t>(encoded.value.size()))
      .ok();
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();

  auto schema = VariantShreddingSchema::Primitive(int64());

  // Shredding should work with BinaryView metadata
  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));
  auto typed_col = shredded->field(2);
  ASSERT_EQ(typed_col->null_count(), 0);
  auto& int_arr = static_cast<const Int64Array&>(*typed_col);
  ASSERT_EQ(int_arr.Value(0), 42);
}

TEST_F(VariantShredRoundTripTest, BinaryViewMetadataReconstructionRoundTrip) {
  // Verify that BINARY_VIEW metadata arrays work in reconstruction path.
  VariantBuilder vb;
  vb.Int(99).ok();
  auto encoded = vb.Finish().ValueOrDie();

  // Build metadata as BinaryView
  BinaryViewBuilder meta_builder;
  meta_builder
      .Append(encoded.metadata.data(), static_cast<int64_t>(encoded.metadata.size()))
      .ok();
  std::shared_ptr<Array> metadata;
  meta_builder.Finish(&metadata).ok();
  ASSERT_EQ(metadata->type_id(), Type::BINARY_VIEW);

  // value_col is null (value goes to typed)
  BinaryBuilder value_builder;
  value_builder.AppendNull().ok();
  std::shared_ptr<Array> value_col;
  value_builder.Finish(&value_col).ok();

  // typed_value has the int64
  Int64Builder typed_builder;
  typed_builder.Append(99).ok();
  std::shared_ptr<Array> typed_col;
  typed_builder.Finish(&typed_col).ok();

  auto schema = VariantShreddingSchema::Primitive(int64());

  // Reconstruction should succeed with BinaryView metadata
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 1);
  ASSERT_TRUE(reconstructed->IsValid(0));
}

TEST_F(VariantShredRoundTripTest, ObjectShredDifferentMetadataDictionaries) {
  // Test object shredding with rows that have different metadata dictionaries.
  // This exercises the cached_meta_bytes comparison in ReconstructVariantColumnObject.
  auto schema = VariantShreddingSchema::Object({
      {"x", VariantShreddingSchema::Primitive(int64())},
  });

  // Row 0: object {"x": 10} with metadata containing "x"
  VariantBuilder vb1;
  auto start1 = vb1.Offset();
  std::vector<VariantBuilder::FieldEntry> fields1;
  fields1.push_back(vb1.NextField(start1, "x"));
  vb1.Int(10).ok();
  vb1.FinishObject(start1, fields1).ok();
  auto encoded1 = vb1.Finish().ValueOrDie();

  // Row 1: object {"x": 20, "y": 30} with metadata containing "x" AND "y"
  VariantBuilder vb2;
  auto start2 = vb2.Offset();
  std::vector<VariantBuilder::FieldEntry> fields2;
  fields2.push_back(vb2.NextField(start2, "x"));
  vb2.Int(20).ok();
  fields2.push_back(vb2.NextField(start2, "y"));
  vb2.Int(30).ok();
  vb2.FinishObject(start2, fields2).ok();
  auto encoded2 = vb2.Finish().ValueOrDie();

  // Build metadata array (different metadata per row)
  BinaryBuilder meta_builder;
  meta_builder
      .Append(encoded1.metadata.data(), static_cast<int32_t>(encoded1.metadata.size()))
      .ok();
  meta_builder
      .Append(encoded2.metadata.data(), static_cast<int32_t>(encoded2.metadata.size()))
      .ok();
  std::shared_ptr<Array> metadata;
  meta_builder.Finish(&metadata).ok();

  // Build value array
  BinaryBuilder value_builder;
  value_builder.Append(encoded1.value.data(), static_cast<int32_t>(encoded1.value.size()))
      .ok();
  value_builder.Append(encoded2.value.data(), static_cast<int32_t>(encoded2.value.size()))
      .ok();
  std::shared_ptr<Array> values;
  value_builder.Finish(&values).ok();

  // Shred
  ASSERT_OK_AND_ASSIGN(auto shredded, ShredVariantColumn(metadata, values, schema));
  auto value_col = shredded->field(1);
  auto typed_col = shredded->field(2);

  // Row 0: "x" is shredded, no residual (single field)
  // Row 1: "x" is shredded, "y" goes to residual
  ASSERT_TRUE(value_col->IsNull(0));   // no residual for row 0
  ASSERT_TRUE(value_col->IsValid(1));  // residual with "y" for row 1

  // Reconstruct — must handle different metadata dictionaries correctly
  ASSERT_OK_AND_ASSIGN(auto reconstructed,
                       ReconstructVariantColumn(metadata, value_col, typed_col, schema));
  ASSERT_EQ(reconstructed->length(), 2);
  ASSERT_TRUE(reconstructed->IsValid(0));
  ASSERT_TRUE(reconstructed->IsValid(1));

  // Verify row 1 reconstructed has both fields by checking it's a valid object
  auto recon_bytes = GetBinaryView(*reconstructed, 1);
  auto* data = reinterpret_cast<const uint8_t*>(recon_bytes.data());
  auto len = static_cast<int64_t>(recon_bytes.size());
  ASSERT_GE(len, 1);
  ASSERT_EQ(GetBasicType(data[0]), BasicType::kObject);
  ASSERT_OK_AND_ASSIGN(auto field_count, GetObjectFieldCount(data, len));
  ASSERT_EQ(field_count, 2);  // Both "x" and "y" reconstructed
}

// ===========================================================================
// NullBuffer output from ReconstructVariantColumn
// ===========================================================================

class VariantReconstructNullBitmapTest : public ::testing::Test {};

TEST_F(VariantReconstructNullBitmapTest, PrimitiveSchemaProducesNullBitmap) {
  // Build: row 0 = Int64(42), row 1 = both null (SQL NULL), row 2 = Int64(99)
  // Shred them so row 1 results in both value=null and typed_value=null.
  auto schema = VariantShreddingSchema::Primitive(int64());

  // Build variant values
  VariantBuilder builder;
  ASSERT_OK(builder.Int(42));
  ASSERT_OK_AND_ASSIGN(auto enc0, builder.Finish());
  builder.Reset();
  ASSERT_OK(builder.Int(99));
  ASSERT_OK_AND_ASSIGN(auto enc2, builder.Finish());

  // Construct the shredded representation manually:
  // metadata: 3 rows (same metadata for all)
  // value: [null, null, null] (all values go to typed_value)
  // typed_value: [42, null, 99]
  BinaryBuilder meta_builder;
  ASSERT_OK(meta_builder.Append(enc0.metadata.data(), enc0.metadata.size()));
  ASSERT_OK(meta_builder.Append(enc0.metadata.data(), enc0.metadata.size()));
  ASSERT_OK(meta_builder.Append(enc0.metadata.data(), enc0.metadata.size()));
  std::shared_ptr<Array> meta_arr;
  ASSERT_OK(meta_builder.Finish(&meta_arr));

  BinaryBuilder value_builder;
  ASSERT_OK(value_builder.AppendNull());
  ASSERT_OK(value_builder.AppendNull());
  ASSERT_OK(value_builder.AppendNull());
  std::shared_ptr<Array> value_arr;
  ASSERT_OK(value_builder.Finish(&value_arr));

  Int64Builder typed_builder;
  ASSERT_OK(typed_builder.Append(42));
  ASSERT_OK(typed_builder.AppendNull());  // row 1: SQL NULL
  ASSERT_OK(typed_builder.Append(99));
  std::shared_ptr<Array> typed_arr;
  ASSERT_OK(typed_builder.Finish(&typed_arr));

  // Reconstruct WITH null bitmap output
  std::shared_ptr<Buffer> null_bitmap;
  ASSERT_OK_AND_ASSIGN(
      auto result,
      ReconstructVariantColumn(meta_arr, value_arr, typed_arr, schema, &null_bitmap));

  // Verify results
  ASSERT_EQ(result->length(), 3);
  ASSERT_NE(null_bitmap, nullptr);

  // Row 0: typed_value present → valid (bit=1)
  ASSERT_TRUE(bit_util::GetBit(null_bitmap->data(), 0));
  // Row 1: both null → SQL NULL (bit=0)
  ASSERT_FALSE(bit_util::GetBit(null_bitmap->data(), 1));
  // Row 2: typed_value present → valid (bit=1)
  ASSERT_TRUE(bit_util::GetBit(null_bitmap->data(), 2));
}

TEST_F(VariantReconstructNullBitmapTest, NullBitmapNotRequestedByDefault) {
  auto schema = VariantShreddingSchema::Primitive(int64());

  VariantBuilder builder;
  ASSERT_OK(builder.Int(42));
  ASSERT_OK_AND_ASSIGN(auto enc, builder.Finish());

  BinaryBuilder meta_builder;
  ASSERT_OK(meta_builder.Append(enc.metadata.data(), enc.metadata.size()));
  std::shared_ptr<Array> meta_arr;
  ASSERT_OK(meta_builder.Finish(&meta_arr));

  BinaryBuilder value_builder;
  ASSERT_OK(value_builder.AppendNull());
  std::shared_ptr<Array> value_arr;
  ASSERT_OK(value_builder.Finish(&value_arr));

  Int64Builder typed_builder;
  ASSERT_OK(typed_builder.Append(42));
  std::shared_ptr<Array> typed_arr;
  ASSERT_OK(typed_builder.Finish(&typed_arr));

  // Call WITHOUT null bitmap (default parameter)
  ASSERT_OK_AND_ASSIGN(auto result,
                       ReconstructVariantColumn(meta_arr, value_arr, typed_arr, schema));
  ASSERT_EQ(result->length(), 1);
}

}  // namespace arrow::extension::variant
