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

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer.h"
#include "arrow/extension/variant.h"
#include "arrow/extension/variant_internal_util.h"
#include "arrow/memory_pool.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/decimal.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging_internal.h"

namespace arrow::extension::variant {

// Forward declaration
static Result<std::shared_ptr<StructArray>> ShredVariantColumnObject(
    const std::shared_ptr<Array>& metadata_array,
    const std::shared_ptr<Array>& value_array, const VariantShreddingSchema& schema);

static Result<std::shared_ptr<StructArray>> ShredVariantColumnArray(
    const std::shared_ptr<Array>& metadata_array,
    const std::shared_ptr<Array>& value_array, const VariantShreddingSchema& schema);

static Result<std::shared_ptr<Array>> ReconstructVariantColumnObject(
    const std::shared_ptr<Array>& metadata_array,
    const std::shared_ptr<Array>& value_array,
    const std::shared_ptr<Array>& typed_value_array,
    const VariantShreddingSchema& schema);

static Result<std::shared_ptr<Array>> ReconstructVariantColumnArray(
    const std::shared_ptr<Array>& metadata_array,
    const std::shared_ptr<Array>& value_array,
    const std::shared_ptr<Array>& typed_value_array,
    const VariantShreddingSchema& schema);

// ---------------------------------------------------------------------------
// VariantShreddingSchema implementation
// ---------------------------------------------------------------------------

VariantShreddingSchema VariantShreddingSchema::Primitive(std::shared_ptr<DataType> type) {
  VariantShreddingSchema schema;
  schema.kind_ = Kind::kPrimitive;
  schema.type_ = std::move(type);
  return schema;
}

VariantShreddingSchema VariantShreddingSchema::Object(
    std::vector<std::pair<std::string, VariantShreddingSchema>> fields) {
  VariantShreddingSchema schema;
  schema.kind_ = Kind::kObject;
  schema.fields_ = std::move(fields);
  return schema;
}

VariantShreddingSchema VariantShreddingSchema::Array(
    VariantShreddingSchema element_schema) {
  VariantShreddingSchema schema;
  schema.kind_ = Kind::kArray;
  schema.element_schema_ =
      std::make_shared<VariantShreddingSchema>(std::move(element_schema));
  return schema;
}

std::shared_ptr<DataType> VariantShreddingSchema::ToArrowType() const {
  switch (kind_) {
    case Kind::kPrimitive:
      return type_;

    case Kind::kObject: {
      std::vector<std::shared_ptr<Field>> arrow_fields;
      arrow_fields.reserve(fields_.size());
      for (const auto& [name, sub_schema] : fields_) {
        auto typed_value_type = sub_schema.ToArrowType();
        auto field_struct = struct_({
            field("value", binary(), /*nullable=*/true),
            field("typed_value", typed_value_type, /*nullable=*/true),
        });
        arrow_fields.push_back(field(name, field_struct, /*nullable=*/false));
      }
      return struct_(std::move(arrow_fields));
    }

    case Kind::kArray: {
      auto elem_typed_type = element_schema_->ToArrowType();
      auto element_struct = struct_({
          field("value", binary(), /*nullable=*/true),
          field("typed_value", elem_typed_type, /*nullable=*/true),
      });
      return list(field("element", element_struct, /*nullable=*/false));
    }
  }
  // All enum values are handled above; this is unreachable.
  DCHECK(false) << "Unknown VariantShreddingSchema kind";
  return nullptr;
}

// ---------------------------------------------------------------------------
// Type compatibility check
// ---------------------------------------------------------------------------

bool IsVariantCompatibleWithType(const uint8_t* variant_data, int64_t variant_length,
                                 const DataType& target_type) {
  if (variant_length < 1) return false;

  uint8_t header = variant_data[0];
  auto basic_type = GetBasicType(header);

  if (basic_type == BasicType::kShortString) {
    // Short strings are semantically UTF-8 text (same as kString), not binary data.
    // They should NOT match BINARY/LARGE_BINARY targets — only string targets.
    // Rust's shredding makes the same distinction: strings → Utf8/LargeUtf8/Utf8View,
    // binary → Binary/LargeBinary/BinaryView.
    return target_type.id() == Type::STRING || target_type.id() == Type::LARGE_STRING ||
           target_type.id() == Type::STRING_VIEW;
  }

  if (basic_type == BasicType::kObject || basic_type == BasicType::kArray) {
    return false;
  }

  if (basic_type == BasicType::kPrimitive) {
    auto prim_type = static_cast<PrimitiveType>((header >> 2) & 0x3F);
    switch (prim_type) {
      case PrimitiveType::kNull:
        // Per Rust/spec semantics: Variant::Null is NOT shredded into typed
        // columns. It is stored as-is in the value column. This distinguishes
        // "variant-typed null" (value = 0x00 byte) from "SQL NULL / missing"
        // (both value and typed_value are null).
        return false;
      case PrimitiveType::kTrue:
      case PrimitiveType::kFalse:
        return target_type.id() == Type::BOOL;
      case PrimitiveType::kInt8:
        return target_type.id() == Type::INT8 || target_type.id() == Type::INT16 ||
               target_type.id() == Type::INT32 || target_type.id() == Type::INT64;
      case PrimitiveType::kInt16:
        return target_type.id() == Type::INT16 || target_type.id() == Type::INT32 ||
               target_type.id() == Type::INT64;
      case PrimitiveType::kInt32:
        return target_type.id() == Type::INT32 || target_type.id() == Type::INT64;
      case PrimitiveType::kInt64:
        return target_type.id() == Type::INT64;
      case PrimitiveType::kFloat:
        // Note: Float→Double widening means shred(Float)→reconstruct produces Double.
        // This is a lossy round-trip for the type tag (value precision is preserved).
        return target_type.id() == Type::FLOAT || target_type.id() == Type::DOUBLE;
      case PrimitiveType::kDouble:
        return target_type.id() == Type::DOUBLE;
      case PrimitiveType::kDecimal4:
      case PrimitiveType::kDecimal8:
      case PrimitiveType::kDecimal16: {
        if (target_type.id() != Type::DECIMAL128 &&
            target_type.id() != Type::DECIMAL256) {
          return false;
        }
        // Verify scale matches: read scale byte from variant data (byte after header).
        int32_t min_size = (prim_type == PrimitiveType::kDecimal4)    ? 6
                           : (prim_type == PrimitiveType::kDecimal8)  ? 10
                           : (prim_type == PrimitiveType::kDecimal16) ? 18
                                                                      : 0;
        if (variant_length < min_size) return false;
        auto variant_scale = static_cast<int32_t>(variant_data[1]);
        if (target_type.id() == Type::DECIMAL128) {
          return variant_scale == static_cast<const Decimal128Type&>(target_type).scale();
        }
        return true;  // DECIMAL256 — accept any scale for now
        // TODO: This is asymmetric with DECIMAL128 which validates scale.
        // Consider adding scale matching for DECIMAL256 once usage is
        // established. Currently pragmatic since DECIMAL256 is rarely used.
      }
      case PrimitiveType::kDate:
        return target_type.id() == Type::DATE32;
      case PrimitiveType::kTimestampMicros: {
        if (target_type.id() != Type::TIMESTAMP) return false;
        auto& ts = static_cast<const TimestampType&>(target_type);
        return ts.unit() == TimeUnit::MICRO && !ts.timezone().empty();
      }
      case PrimitiveType::kTimestampMicrosNTZ: {
        if (target_type.id() != Type::TIMESTAMP) return false;
        auto& ts = static_cast<const TimestampType&>(target_type);
        return ts.unit() == TimeUnit::MICRO && ts.timezone().empty();
      }
      case PrimitiveType::kTimestampNanos: {
        if (target_type.id() != Type::TIMESTAMP) return false;
        auto& ts = static_cast<const TimestampType&>(target_type);
        return ts.unit() == TimeUnit::NANO && !ts.timezone().empty();
      }
      case PrimitiveType::kTimestampNanosNTZ: {
        if (target_type.id() != Type::TIMESTAMP) return false;
        auto& ts = static_cast<const TimestampType&>(target_type);
        return ts.unit() == TimeUnit::NANO && ts.timezone().empty();
      }
      case PrimitiveType::kTimeNTZ: {
        if (target_type.id() != Type::TIME64) return false;
        // The variant spec's kTimeNTZ stores microseconds since midnight.
        // Only accept time64(MICRO) targets — a time64(NANO) target would
        // cause misinterpretation of the shredded values in typed_value.
        auto& t = static_cast<const Time64Type&>(target_type);
        return t.unit() == TimeUnit::MICRO;
      }
      case PrimitiveType::kString:
        return target_type.id() == Type::STRING ||
               target_type.id() == Type::LARGE_STRING ||
               target_type.id() == Type::STRING_VIEW;
      case PrimitiveType::kBinary:
        return target_type.id() == Type::BINARY ||
               target_type.id() == Type::LARGE_BINARY ||
               target_type.id() == Type::BINARY_VIEW;
      case PrimitiveType::kUUID:
        return target_type.id() == Type::FIXED_SIZE_BINARY &&
               static_cast<const FixedSizeBinaryType&>(target_type).byte_width() ==
                   kUUIDByteLength;
      default:
        // Unknown or future primitive types are not compatible with any target.
        return false;
    }
  }

  return false;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

// Supports BINARY, LARGE_BINARY, BINARY_VIEW, and STRING_VIEW arrays.
// Rust supports all of these via GenericByteViewArray / GenericByteArray.
std::string_view GetBinaryValue(const Array& array, int64_t i) {
  if (array.type_id() == Type::BINARY) {
    auto& bin = static_cast<const BinaryArray&>(array);
    auto view = bin.GetView(i);
    return {reinterpret_cast<const char*>(view.data()), static_cast<size_t>(view.size())};
  }
  if (array.type_id() == Type::LARGE_BINARY) {
    auto& bin = static_cast<const LargeBinaryArray&>(array);
    auto view = bin.GetView(i);
    return {reinterpret_cast<const char*>(view.data()), static_cast<size_t>(view.size())};
  }
  if (array.type_id() == Type::BINARY_VIEW || array.type_id() == Type::STRING_VIEW) {
    auto& bin = static_cast<const BinaryViewArray&>(array);
    return bin.GetView(i);
  }
  // Callers validate input types at public entry points (ShredVariantColumn,
  // ReconstructVariantColumn). Reaching here indicates a programming error.
  DCHECK(false) << "GetBinaryValue: unsupported array type " << array.type()->ToString();
  return {};
}

/// Alias the shared internal ReadLE utilities for use in this file.
/// ReadUnsignedLE64 handles 1-8 byte reads and returns int64_t, which
/// matches the needs of shredding's extraction functions.
using internal::ReadUnsignedLE64;

/// Convenience alias matching the local calling convention.
/// All callers in this file use ReadLE(buf, nbytes) → int64_t.
inline int64_t ReadLE(const uint8_t* buf, int32_t nbytes) {
  return ReadUnsignedLE64(buf, nbytes);
}

/// Extract a native int64 value from variant-encoded bytes (handles all int sizes).
/// Returns true if extraction succeeded, false if type is not an integer.
bool ExtractInt64(const uint8_t* data, int64_t length, int64_t* out) {
  if (length < 1) return false;
  uint8_t header = data[0];
  if (GetBasicType(header) != BasicType::kPrimitive) return false;
  auto prim = static_cast<PrimitiveType>((header >> 2) & 0x3F);
  switch (prim) {
    case PrimitiveType::kInt8:
      if (length < 2) return false;
      *out = static_cast<int64_t>(static_cast<int8_t>(data[1]));
      return true;
    case PrimitiveType::kInt16:
      if (length < 3) return false;
      // ReadLE returns zero-extended int64_t; narrow to int16_t for sign-extension,
      // then widen back to int64_t explicitly.
      *out = static_cast<int64_t>(static_cast<int16_t>(ReadLE(data + 1, 2)));
      return true;
    case PrimitiveType::kInt32:
      if (length < 5) return false;
      // ReadLE returns zero-extended int64_t; narrow to int32_t for sign-extension,
      // then widen back to int64_t explicitly.
      *out = static_cast<int64_t>(static_cast<int32_t>(ReadLE(data + 1, 4)));
      return true;
    case PrimitiveType::kInt64:
      if (length < 9) return false;
      *out = ReadLE(data + 1, 8);
      return true;
    default:
      return false;
  }
}

/// Extract a boolean from variant-encoded bytes.
bool ExtractBool(const uint8_t* data, int64_t length, bool* out) {
  if (length < 1) return false;
  uint8_t header = data[0];
  if (GetBasicType(header) != BasicType::kPrimitive) return false;
  auto prim = static_cast<PrimitiveType>((header >> 2) & 0x3F);
  if (prim == PrimitiveType::kTrue) {
    *out = true;
    return true;
  }
  if (prim == PrimitiveType::kFalse) {
    *out = false;
    return true;
  }
  return false;
}

/// Extract a double from variant-encoded bytes (handles float→double widening and
/// native double). Named "ExtractDoubleOrFloat" to clarify that it accepts both
/// kFloat (widened to double) and kDouble variant types.
bool ExtractDoubleOrFloat(const uint8_t* data, int64_t length, double* out) {
  if (length < 1) return false;
  uint8_t header = data[0];
  if (GetBasicType(header) != BasicType::kPrimitive) return false;
  auto prim = static_cast<PrimitiveType>((header >> 2) & 0x3F);
  if (prim == PrimitiveType::kDouble && length >= 9) {
    uint64_t bits;
    std::memcpy(&bits, data + 1, 8);
    bits = bit_util::FromLittleEndian(bits);
    std::memcpy(out, &bits, 8);
    return true;
  }
  if (prim == PrimitiveType::kFloat && length >= 5) {
    uint32_t bits;
    std::memcpy(&bits, data + 1, 4);
    bits = bit_util::FromLittleEndian(bits);
    float f;
    std::memcpy(&f, &bits, 4);
    *out = static_cast<double>(f);
    return true;
  }
  return false;
}

/// Extract a string_view from variant-encoded bytes (handles short and long strings).
bool ExtractString(const uint8_t* data, int64_t length, std::string_view* out) {
  if (length < 1) return false;
  uint8_t header = data[0];
  auto basic_type = GetBasicType(header);
  if (basic_type == BasicType::kShortString) {
    int32_t str_len = (header >> 2) & 0x3F;
    if (length < 1 + str_len) return false;
    *out = std::string_view(reinterpret_cast<const char*>(data + 1), str_len);
    return true;
  }
  if (basic_type == BasicType::kPrimitive) {
    auto prim = static_cast<PrimitiveType>((header >> 2) & 0x3F);
    if (prim == PrimitiveType::kString && length >= 5) {
      uint32_t str_len;
      std::memcpy(&str_len, data + 1, 4);
      str_len = bit_util::FromLittleEndian(str_len);
      if (length < 5 + static_cast<int64_t>(str_len)) return false;
      *out = std::string_view(reinterpret_cast<const char*>(data + 5), str_len);
      return true;
    }
  }
  return false;
}

/// Check if the variant value is Variant Null.
bool IsVariantNull(const uint8_t* data, int64_t length) {
  if (length < 1) return false;
  uint8_t header = data[0];
  return GetBasicType(header) == BasicType::kPrimitive &&
         static_cast<PrimitiveType>((header >> 2) & 0x3F) == PrimitiveType::kNull;
}

/// Extract a float from variant-encoded bytes.
bool ExtractFloat(const uint8_t* data, int64_t length, float* out) {
  if (length < 1) return false;
  uint8_t header = data[0];
  if (GetBasicType(header) != BasicType::kPrimitive) return false;
  auto prim = static_cast<PrimitiveType>((header >> 2) & 0x3F);
  if (prim == PrimitiveType::kFloat && length >= 5) {
    uint32_t bits;
    std::memcpy(&bits, data + 1, 4);
    bits = bit_util::FromLittleEndian(bits);
    std::memcpy(out, &bits, 4);
    return true;
  }
  return false;
}

/// Extract a date32 (days since epoch) from variant-encoded bytes.
bool ExtractDate32(const uint8_t* data, int64_t length, int32_t* out) {
  if (length < 1) return false;
  uint8_t header = data[0];
  if (GetBasicType(header) != BasicType::kPrimitive) return false;
  auto prim = static_cast<PrimitiveType>((header >> 2) & 0x3F);
  if (prim == PrimitiveType::kDate && length >= 5) {
    *out = static_cast<int32_t>(ReadLE(data + 1, 4));
    return true;
  }
  return false;
}

/// Extract a timestamp (micros or nanos) from variant-encoded bytes.
bool ExtractTimestamp(const uint8_t* data, int64_t length, int64_t* out) {
  if (length < 1) return false;
  uint8_t header = data[0];
  if (GetBasicType(header) != BasicType::kPrimitive) return false;
  auto prim = static_cast<PrimitiveType>((header >> 2) & 0x3F);
  if ((prim == PrimitiveType::kTimestampMicros ||
       prim == PrimitiveType::kTimestampMicrosNTZ ||
       prim == PrimitiveType::kTimestampNanos ||
       prim == PrimitiveType::kTimestampNanosNTZ) &&
      length >= 9) {
    *out = static_cast<int64_t>(ReadLE(data + 1, 8));
    return true;
  }
  return false;
}

/// Extract binary data from variant-encoded bytes.
bool ExtractBinary(const uint8_t* data, int64_t length, const uint8_t** out_data,
                   int32_t* out_size) {
  if (length < 1) return false;
  uint8_t header = data[0];
  if (GetBasicType(header) != BasicType::kPrimitive) return false;
  auto prim = static_cast<PrimitiveType>((header >> 2) & 0x3F);
  if (prim == PrimitiveType::kBinary && length >= 5) {
    uint32_t bin_len;
    std::memcpy(&bin_len, data + 1, 4);
    bin_len = bit_util::FromLittleEndian(bin_len);
    if (length < 5 + static_cast<int64_t>(bin_len)) return false;
    *out_data = data + 5;
    *out_size = static_cast<int32_t>(bin_len);
    return true;
  }
  return false;
}

/// Extract an int32 from variant-encoded bytes (handles int8, int16, int32).
bool ExtractInt32(const uint8_t* data, int64_t length, int32_t* out) {
  if (length < 1) return false;
  uint8_t header = data[0];
  if (GetBasicType(header) != BasicType::kPrimitive) return false;
  auto prim = static_cast<PrimitiveType>((header >> 2) & 0x3F);
  switch (prim) {
    case PrimitiveType::kInt8:
      if (length < 2) return false;
      *out = static_cast<int32_t>(static_cast<int8_t>(data[1]));
      return true;
    case PrimitiveType::kInt16:
      if (length < 3) return false;
      *out = static_cast<int32_t>(static_cast<int16_t>(ReadLE(data + 1, 2)));
      return true;
    case PrimitiveType::kInt32:
      if (length < 5) return false;
      *out = static_cast<int32_t>(ReadLE(data + 1, 4));
      return true;
    default:
      return false;
  }
}

/// Extract an int8 from variant-encoded bytes (handles only int8).
bool ExtractInt8(const uint8_t* data, int64_t length, int8_t* out) {
  if (length < 1) return false;
  uint8_t header = data[0];
  if (GetBasicType(header) != BasicType::kPrimitive) return false;
  auto prim = static_cast<PrimitiveType>((header >> 2) & 0x3F);
  if (prim == PrimitiveType::kInt8 && length >= 2) {
    *out = static_cast<int8_t>(data[1]);
    return true;
  }
  return false;
}

/// Extract an int16 from variant-encoded bytes (handles int8, int16).
bool ExtractInt16(const uint8_t* data, int64_t length, int16_t* out) {
  if (length < 1) return false;
  uint8_t header = data[0];
  if (GetBasicType(header) != BasicType::kPrimitive) return false;
  auto prim = static_cast<PrimitiveType>((header >> 2) & 0x3F);
  switch (prim) {
    case PrimitiveType::kInt8:
      if (length < 2) return false;
      *out = static_cast<int16_t>(static_cast<int8_t>(data[1]));
      return true;
    case PrimitiveType::kInt16:
      if (length < 3) return false;
      *out = static_cast<int16_t>(ReadLE(data + 1, 2));
      return true;
    default:
      return false;
  }
}

/// Extract time64 (microseconds since midnight) from variant-encoded bytes.
bool ExtractTime64(const uint8_t* data, int64_t length, int64_t* out) {
  if (length < 1) return false;
  uint8_t header = data[0];
  if (GetBasicType(header) != BasicType::kPrimitive) return false;
  auto prim = static_cast<PrimitiveType>((header >> 2) & 0x3F);
  if (prim == PrimitiveType::kTimeNTZ && length >= 9) {
    *out = static_cast<int64_t>(ReadLE(data + 1, 8));
    return true;
  }
  return false;
}

/// Extract UUID (16 big-endian bytes) from variant-encoded bytes.
bool ExtractUUID(const uint8_t* data, int64_t length, uint8_t* out16) {
  if (length < 1) return false;
  uint8_t header = data[0];
  if (GetBasicType(header) != BasicType::kPrimitive) return false;
  auto prim = static_cast<PrimitiveType>((header >> 2) & 0x3F);
  if (prim == PrimitiveType::kUUID && length >= kUUIDByteLength + 1) {
    std::memcpy(out16, data + 1, kUUIDByteLength);
    return true;
  }
  return false;
}

/// Extract a Decimal128 (scale + unscaled value) from variant-encoded bytes.
/// Handles decimal4 (4-byte), decimal8 (8-byte), and decimal16 (16-byte).
/// The output is a 128-bit unscaled integer value suitable for Arrow's Decimal128Type.
bool ExtractDecimal128(const uint8_t* data, int64_t length, int64_t* out_low,
                       int64_t* out_high, uint8_t* out_scale) {
  if (length < 1) return false;
  uint8_t header = data[0];
  if (GetBasicType(header) != BasicType::kPrimitive) return false;
  auto prim = static_cast<PrimitiveType>((header >> 2) & 0x3F);
  switch (prim) {
    case PrimitiveType::kDecimal4:
      if (length < 6) return false;  // 1 header + 1 scale + 4 value
      *out_scale = data[1];
      {
        int32_t val;
        // memcpy + FromLittleEndian is safe here: we copy the full variable width
        // (4 bytes into int32), then swap. This differs from ReadLE's byte-shift
        // approach which handles partial-width reads.
        std::memcpy(&val, data + 2, 4);
        val = bit_util::FromLittleEndian(val);
        *out_low = static_cast<int64_t>(val);
        *out_high = (val < 0) ? -1 : 0;  // sign-extend
      }
      return true;
    case PrimitiveType::kDecimal8:
      if (length < 10) return false;  // 1 header + 1 scale + 8 value
      *out_scale = data[1];
      {
        int64_t val;
        // memcpy + FromLittleEndian is safe here: we copy the full variable width
        // (8 bytes into int64), then swap. This differs from ReadLE's byte-shift
        // approach which handles partial-width reads.
        std::memcpy(&val, data + 2, 8);
        val = bit_util::FromLittleEndian(val);
        *out_low = val;
        *out_high = (val < 0) ? -1 : 0;  // sign-extend
      }
      return true;
    case PrimitiveType::kDecimal16:
      if (length < 18) return false;  // 1 header + 1 scale + 16 value
      *out_scale = data[1];
      {
        std::memcpy(out_low, data + 2, 8);
        std::memcpy(out_high, data + 10, 8);
        *out_low = bit_util::FromLittleEndian(*out_low);
        *out_high = bit_util::FromLittleEndian(*out_high);
      }
      return true;
    default:
      return false;
  }
}

}  // namespace

// ---------------------------------------------------------------------------
// ShredVariantColumnObject — object field-level shredding
// ---------------------------------------------------------------------------

namespace {

/// Shred a single object variant value, routing named fields to sub-builders.
/// Produces: for each schema field, the field value (or null if missing).
///           Residual = object with remaining fields not in schema.
///
/// For fields with Primitive sub-schemas, the output construction phase
/// (in ShredVariantColumnObject) performs recursive native extraction by
/// calling ShredVariantColumn on the per-field BinaryArray. This matches
/// Rust's VariantToShreddedObjectVariantRowBuilder behavior: compatible
/// field values go to typed_value, incompatible remain in value.
///
/// For fields with Object or Array sub-schemas, field values are stored
/// as variant binary in the "value" sub-column (recursive nested shredding
/// is a potential follow-up optimization).
// TODO GH-45948 follow-up: Recursive shredding for nested Object/Array
// sub-schemas (not just Primitive).
struct ObjectFieldShredder {
  ObjectFieldShredder(const VariantShreddingSchema& schema, int64_t num_rows)
      : schema(schema), num_rows(num_rows) {}

  const VariantShreddingSchema& schema;
  int64_t num_rows;

  // One BinaryBuilder per schema field (stores variant bytes of matching fields).
  // For Primitive sub-schemas, the output construction phase re-shreds these
  // into {value, typed_value} using ShredVariantColumn. For other sub-schemas,
  // these bytes go directly to the "value" sub-column.
  std::vector<BinaryBuilder> field_value_builders;

  // Residual: object bytes for fields not in schema
  BinaryBuilder residual_builder;

  Status Init() {
    field_value_builders.resize(schema.fields().size());
    for (auto& b : field_value_builders) {
      ARROW_RETURN_NOT_OK(b.Reserve(num_rows));
    }
    ARROW_RETURN_NOT_OK(residual_builder.Reserve(num_rows));
    return Status::OK();
  }

  Status AppendNull() {
    // Object is missing/null → all field builders get null, residual gets null
    for (auto& b : field_value_builders) {
      ARROW_RETURN_NOT_OK(b.AppendNull());
    }
    ARROW_RETURN_NOT_OK(residual_builder.AppendNull());
    return Status::OK();
  }

  Status AppendNonObject(std::string_view variant_bytes) {
    // Value is not an object → residual gets the whole value, fields get null
    for (auto& b : field_value_builders) {
      ARROW_RETURN_NOT_OK(b.AppendNull());
    }
    ARROW_RETURN_NOT_OK(residual_builder.Append(
        reinterpret_cast<const uint8_t*>(variant_bytes.data()), variant_bytes.size()));
    return Status::OK();
  }

  Status AppendObject(const VariantMetadata& meta, const uint8_t* obj_data,
                      int64_t obj_length) {
    // Determine which fields from the schema are present in this object
    const auto& schema_fields = schema.fields();

    // Parse the object header ONCE — all subsequent field access is O(1).
    ARROW_ASSIGN_OR_RAISE(auto obj_view,
                          VariantObjectView::Make(meta, obj_data, obj_length));
    auto field_count = obj_view.num_fields();

    // Single pass over object fields: build a name→(index, offset, size) map.
    // This eliminates the previous O(s × k) inner marking loop by allowing
    // O(1) positional index lookup when marking shredded fields.
    // PERF TODO: This allocates an unordered_map per row. For column-scan
    // workloads with millions of rows, consider lifting the map to the
    // ObjectFieldShredder struct and clearing/reusing it across rows.
    // Duplicate keys (spec-invalid): last occurrence wins in map, earlier
    // occurrences remain in residual. Matches last-value-wins semantics.
    struct FieldInfo {
      int32_t index;
      int64_t offset;
      int64_t size;
      std::string_view name;
    };
    std::unordered_map<std::string_view, FieldInfo> object_field_map;
    object_field_map.reserve(field_count);
    for (int32_t fi = 0; fi < field_count; ++fi) {
      ARROW_ASSIGN_OR_RAISE(auto fname, obj_view.field_name(fi));
      ARROW_ASSIGN_OR_RAISE(auto fvalue, obj_view.field_value(fi));
      auto foff = fvalue.data() - obj_data;
      auto fsz = fvalue.size_bytes();
      object_field_map[fname] = FieldInfo{fi, foff, fsz, fname};
    }

    // Track which object fields are shredded vs residual
    std::vector<bool> is_shredded(field_count, false);

    // For each schema field, look it up in the pre-built map.
    // Total complexity: O(k) for map construction + O(s) for lookups = O(s + k).
    for (size_t sf = 0; sf < schema_fields.size(); ++sf) {
      auto it = object_field_map.find(schema_fields[sf].first);
      if (it != object_field_map.end()) {
        const auto& info = it->second;
        ARROW_RETURN_NOT_OK(field_value_builders[sf].Append(
            obj_data + info.offset, static_cast<int32_t>(info.size)));
        is_shredded[info.index] = true;
      } else {
        // Not found — null for this field
        ARROW_RETURN_NOT_OK(field_value_builders[sf].AppendNull());
      }
    }

    // Build residual object with non-shredded fields
    bool has_residual = false;
    for (int32_t fi = 0; fi < field_count; ++fi) {
      if (!is_shredded[fi]) {
        has_residual = true;
        break;
      }
    }

    if (has_residual) {
      // Build a residual variant object with non-shredded fields.
      // Uses the pre-built object_field_map to avoid re-parsing the object header.
      VariantBuilder vb(meta);
      auto start = vb.Offset();
      std::vector<VariantBuilder::FieldEntry> residual_fields;
      for (const auto& [fname, info] : object_field_map) {
        if (!is_shredded[info.index]) {
          residual_fields.push_back(vb.NextField(start, fname));
          vb.UnsafeAppendEncoded(obj_data + info.offset, info.size);
        }
      }
      ARROW_RETURN_NOT_OK(vb.FinishObject(start, residual_fields));
      ARROW_ASSIGN_OR_RAISE(auto residual_bytes, vb.BuildWithoutMeta());
      ARROW_RETURN_NOT_OK(residual_builder.Append(
          residual_bytes.data(), static_cast<int32_t>(residual_bytes.size())));
    } else {
      // All fields were shredded → residual is null
      ARROW_RETURN_NOT_OK(residual_builder.AppendNull());
    }

    return Status::OK();
  }
};

}  // namespace

Result<std::shared_ptr<StructArray>> ShredVariantColumnObject(
    const std::shared_ptr<Array>& metadata_array,
    const std::shared_ptr<Array>& value_array, const VariantShreddingSchema& schema) {
  const int64_t num_rows = value_array->length();
  const auto& schema_fields = schema.fields();

  ObjectFieldShredder shredder(schema, num_rows);
  ARROW_RETURN_NOT_OK(shredder.Init());

  for (int64_t i = 0; i < num_rows; ++i) {
    if (value_array->IsNull(i)) {
      ARROW_RETURN_NOT_OK(shredder.AppendNull());
      continue;
    }

    auto bytes = GetBinaryValue(*value_array, i);
    auto* data = reinterpret_cast<const uint8_t*>(bytes.data());
    auto len = static_cast<int64_t>(bytes.size());

    if (len < 1) {
      ARROW_RETURN_NOT_OK(shredder.AppendNull());
      continue;
    }

    auto basic_type = GetBasicType(data[0]);
    if (basic_type != BasicType::kObject) {
      // Not an object → goes entirely to residual value
      ARROW_RETURN_NOT_OK(shredder.AppendNonObject(bytes));
      continue;
    }

    // Decode metadata for this row
    auto meta_bytes = GetBinaryValue(*metadata_array, i);
    ARROW_ASSIGN_OR_RAISE(
        auto meta, DecodeMetadata(reinterpret_cast<const uint8_t*>(meta_bytes.data()),
                                  static_cast<int64_t>(meta_bytes.size())));

    ARROW_RETURN_NOT_OK(shredder.AppendObject(meta, data, len));
  }

  // Build output: typed_value is a struct with one field per schema field.
  // Each field is a struct {value: binary(nullable), typed_value: <type>(nullable)}.
  // For fields with Primitive sub-schemas, we perform native extraction:
  // compatible values go to typed_value, incompatible remain in value.
  // This matches Rust's recursive shredding behavior.
  std::vector<std::shared_ptr<Array>> typed_value_columns;
  std::vector<std::shared_ptr<Field>> typed_value_fields;

  for (size_t sf = 0; sf < schema_fields.size(); ++sf) {
    std::shared_ptr<Array> field_arr;
    ARROW_RETURN_NOT_OK(shredder.field_value_builders[sf].Finish(&field_arr));

    const auto& sub_schema = schema_fields[sf].second;

    if (sub_schema.kind() == VariantShreddingSchema::Kind::kPrimitive) {
      // Recursive native extraction: shred the field values through the
      // primitive path. This produces a struct {metadata, value, typed_value}
      // from which we extract just {value, typed_value} for the sub-field.
      // We use the top-level metadata_array since field values reference
      // the same metadata dictionary.
      ARROW_ASSIGN_OR_RAISE(auto field_shredded,
                            ShredVariantColumn(metadata_array, field_arr, sub_schema));
      auto field_value_col = field_shredded->field(1);  // "value" (nullable)
      auto field_typed_col = field_shredded->field(2);  // "typed_value" (nullable)

      // Determine typed_value field type — for TIMESTAMP/TIME64, the builder
      // produces Int64Array, so the field declares int64().
      auto typed_field_type = sub_schema.type();
      if (typed_field_type->id() == Type::TIMESTAMP ||
          typed_field_type->id() == Type::TIME64) {
        typed_field_type = int64();
      }

      auto inner_fields = std::vector<std::shared_ptr<Field>>{
          field("value", binary(), true),
          field("typed_value", typed_field_type, true),
      };
      ARROW_ASSIGN_OR_RAISE(
          auto field_struct,
          StructArray::Make({field_value_col, field_typed_col}, inner_fields));
      typed_value_columns.push_back(field_struct);
      typed_value_fields.push_back(
          field(schema_fields[sf].first, field_struct->type(), false));
    } else {
      // Non-primitive sub-schemas (Object, Array): store field values as
      // variant binary in the "value" sub-column. Recursive shredding of
      // nested objects/arrays is a potential follow-up optimization.
      //
      // NOTE: We use NullArray here for the typed_value sub-column. The field
      // metadata declares the logical type (from sub_schema.ToArrowType()),
      // but the actual array is type null(). This is semantically acceptable
      // because the field is always null (no typed extraction for non-primitive
      // sub-schemas), so no consumer will attempt to access typed data. The
      // declared field type serves only as schema documentation for readers
      // inspecting the shredded output structure.
      auto null_arr = std::make_shared<NullArray>(num_rows);
      auto inner_fields = std::vector<std::shared_ptr<Field>>{
          field("value", binary(), true),
          field("typed_value", sub_schema.ToArrowType(), true),
      };
      ARROW_ASSIGN_OR_RAISE(auto field_struct,
                            StructArray::Make({field_arr, null_arr}, inner_fields));
      typed_value_columns.push_back(field_struct);
      typed_value_fields.push_back(
          field(schema_fields[sf].first, field_struct->type(), false));
    }
  }

  std::shared_ptr<Array> typed_value_struct;
  if (!typed_value_fields.empty()) {
    ARROW_ASSIGN_OR_RAISE(typed_value_struct,
                          StructArray::Make(typed_value_columns, typed_value_fields));
  } else {
    typed_value_struct = std::make_shared<NullArray>(num_rows);
  }

  std::shared_ptr<Array> residual_result;
  ARROW_RETURN_NOT_OK(shredder.residual_builder.Finish(&residual_result));

  auto output_fields = std::vector<std::shared_ptr<Field>>{
      field("metadata", metadata_array->type(), false),
      field("value", binary(), true),
      field("typed_value", typed_value_struct->type(), true),
  };

  return StructArray::Make({metadata_array, residual_result, typed_value_struct},
                           output_fields);
}

// ---------------------------------------------------------------------------
// ShredVariantColumnArray — array element-wise shredding
// ---------------------------------------------------------------------------

Result<std::shared_ptr<StructArray>> ShredVariantColumnArray(
    const std::shared_ptr<Array>& metadata_array,
    const std::shared_ptr<Array>& value_array, const VariantShreddingSchema& schema) {
  const int64_t num_rows = value_array->length();

  // For array shredding, typed_value is a ListArray where each element
  // is a struct {value: binary, typed_value: <elem_type>}.
  // Per the spec: if the variant is an array → typed_value is the list, value is null.
  //              if the variant is NOT an array → typed_value is null, value has the
  //              bytes.
  //
  // Rust parity: elements are recursively shredded according to the element schema,
  // producing native typed columns for compatible elements. Incompatible elements
  // remain in the per-element value column as variant binary. This enables
  // statistics-based predicate pushdown on array element values.

  BinaryBuilder residual_value_builder;
  ARROW_RETURN_NOT_OK(residual_value_builder.Reserve(num_rows));

  // Phase 1: Extract array element bytes into a flat BinaryArray.
  // Track list offsets and validity manually (rather than using ListBuilder)
  // to avoid double-finish issues with the internal value builder.
  BinaryBuilder elem_value_builder;
  BinaryBuilder elem_metadata_builder;
  std::vector<int32_t> list_offsets;
  list_offsets.reserve(num_rows + 1);
  list_offsets.push_back(0);
  std::vector<bool> list_validity(num_rows, false);

  for (int64_t i = 0; i < num_rows; ++i) {
    if (value_array->IsNull(i)) {
      ARROW_RETURN_NOT_OK(residual_value_builder.AppendNull());
      list_offsets.push_back(list_offsets.back());
      // list_validity[i] remains false (null)
      continue;
    }

    auto bytes = GetBinaryValue(*value_array, i);
    auto* data = reinterpret_cast<const uint8_t*>(bytes.data());
    auto len = static_cast<int64_t>(bytes.size());

    if (len < 1 || GetBasicType(data[0]) != BasicType::kArray) {
      // Not an array → residual value, typed_value null
      ARROW_RETURN_NOT_OK(residual_value_builder.Append(
          reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
      list_offsets.push_back(list_offsets.back());
      // list_validity[i] remains false (null)
      continue;
    }

    // Is an array → extract elements
    ARROW_RETURN_NOT_OK(residual_value_builder.AppendNull());
    list_validity[i] = true;

    // Get the row's metadata for element replication
    auto meta_bytes = GetBinaryValue(*metadata_array, i);

    VariantMetadata empty_meta;
    ARROW_ASSIGN_OR_RAISE(auto arr_view, VariantArrayView::Make(empty_meta, data, len));
    auto elem_count = arr_view.num_elements();
    for (int32_t ei = 0; ei < elem_count; ++ei) {
      ARROW_ASSIGN_OR_RAISE(auto elem, arr_view.get(ei));
      ARROW_RETURN_NOT_OK(elem_value_builder.Append(
          elem.data(), static_cast<int32_t>(elem.size_bytes())));
      ARROW_RETURN_NOT_OK(elem_metadata_builder.Append(
          reinterpret_cast<const uint8_t*>(meta_bytes.data()), meta_bytes.size()));
    }
    list_offsets.push_back(static_cast<int32_t>(list_offsets.back() + elem_count));
  }

  // Phase 2: Recursively shred the flattened element array through the element schema.
  std::shared_ptr<Array> elem_values_arr;
  ARROW_RETURN_NOT_OK(elem_value_builder.Finish(&elem_values_arr));

  std::shared_ptr<Array> elem_metadata_arr;
  ARROW_RETURN_NOT_OK(elem_metadata_builder.Finish(&elem_metadata_arr));

  ARROW_ASSIGN_OR_RAISE(
      auto elem_shredded,
      ShredVariantColumn(elem_metadata_arr, elem_values_arr, schema.element_schema()));

  // Extract the {value, typed_value} columns from the recursively shredded result.
  auto elem_value_col = elem_shredded->field(1);  // per-element residual value
  auto elem_typed_col = elem_shredded->field(2);  // per-element typed_value

  // Determine typed_value field type for the element struct.
  auto elem_typed_field_type = schema.element_schema().type();
  if (schema.element_schema().kind() == VariantShreddingSchema::Kind::kPrimitive) {
    if (elem_typed_field_type && (elem_typed_field_type->id() == Type::TIMESTAMP ||
                                  elem_typed_field_type->id() == Type::TIME64)) {
      elem_typed_field_type = int64();
    }
  } else {
    // For Object/Array element schemas, use the shredded output's actual type
    elem_typed_field_type = elem_typed_col->type();
  }

  // Build the element struct array: {value: binary, typed_value: <elem_type>}
  auto elem_struct_fields = std::vector<std::shared_ptr<Field>>{
      field("value", binary(), true),
      field("typed_value", elem_typed_field_type, true),
  };
  ARROW_ASSIGN_OR_RAISE(
      auto elem_struct_arr,
      StructArray::Make({elem_value_col, elem_typed_col}, elem_struct_fields));

  // Phase 3: Build the ListArray from manually tracked offsets and the element struct.
  auto offsets_buf = Buffer::FromVector(std::move(list_offsets));

  // Build null bitmap for the list
  int64_t null_count = 0;
  std::shared_ptr<Buffer> null_bitmap;
  for (int64_t i = 0; i < num_rows; ++i) {
    if (!list_validity[i]) ++null_count;
  }
  if (null_count > 0) {
    ARROW_ASSIGN_OR_RAISE(null_bitmap, AllocateBitmap(num_rows));
    for (int64_t i = 0; i < num_rows; ++i) {
      if (list_validity[i]) {
        bit_util::SetBit(null_bitmap->mutable_data(), i);
      } else {
        bit_util::ClearBit(null_bitmap->mutable_data(), i);
      }
    }
  }

  auto typed_value_list = std::make_shared<ListArray>(
      list(field("element", elem_struct_arr->type(), false)), num_rows, offsets_buf,
      elem_struct_arr, null_bitmap, null_count);

  std::shared_ptr<Array> residual_result;
  ARROW_RETURN_NOT_OK(residual_value_builder.Finish(&residual_result));

  auto output_fields = std::vector<std::shared_ptr<Field>>{
      field("metadata", metadata_array->type(), false),
      field("value", binary(), true),
      field("typed_value", typed_value_list->type(), true),
  };

  return StructArray::Make({metadata_array, residual_result,
                            std::static_pointer_cast<Array>(typed_value_list)},
                           output_fields);
}

// ---------------------------------------------------------------------------
// Template helpers for primitive shredding
// ---------------------------------------------------------------------------

namespace {

/// Generic primitive shredding loop. For each row:
///  - If input is null → both builders get null
///  - If Variant::Null → value column gets bytes, typed gets null
///  - If extraction succeeds → typed gets value, residual gets null
///  - Otherwise → value column gets bytes, typed gets null
///
/// This eliminates ~360 lines of per-type copy-paste that previously existed
/// as individual switch cases with identical structure.
template <typename BuilderT, typename NativeT, typename ExtractFn>
Status ShredPrimitiveLoop(const Array& value_array, int64_t num_rows,
                          BinaryBuilder& residual, std::shared_ptr<Array>* out,
                          ExtractFn&& extract) {
  BuilderT typed_builder;
  ARROW_RETURN_NOT_OK(typed_builder.Reserve(num_rows));
  for (int64_t i = 0; i < num_rows; ++i) {
    if (value_array.IsNull(i)) {
      ARROW_RETURN_NOT_OK(residual.AppendNull());
      ARROW_RETURN_NOT_OK(typed_builder.AppendNull());
      continue;
    }
    auto bytes = GetBinaryValue(value_array, i);
    auto* data = reinterpret_cast<const uint8_t*>(bytes.data());
    auto len = static_cast<int64_t>(bytes.size());
    // Default-initialized; only read when extract() returns true.
    NativeT native_val{};
    if (IsVariantNull(data, len)) {
      // Variant::Null goes to value column — distinguishes from SQL NULL
      ARROW_RETURN_NOT_OK(
          residual.Append(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
      ARROW_RETURN_NOT_OK(typed_builder.AppendNull());
    } else if (extract(data, len, &native_val)) {
      ARROW_RETURN_NOT_OK(residual.AppendNull());
      ARROW_RETURN_NOT_OK(typed_builder.Append(native_val));
    } else {
      ARROW_RETURN_NOT_OK(
          residual.Append(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
      ARROW_RETURN_NOT_OK(typed_builder.AppendNull());
    }
  }
  return typed_builder.Finish(out);
}

/// Specialization for BINARY/LARGE_BINARY extraction (uses pointer+size output).
template <typename BuilderT>
Status ShredBinaryLoop(const Array& value_array, int64_t num_rows,
                       BinaryBuilder& residual, std::shared_ptr<Array>* out) {
  BuilderT typed_builder;
  ARROW_RETURN_NOT_OK(typed_builder.Reserve(num_rows));
  for (int64_t i = 0; i < num_rows; ++i) {
    if (value_array.IsNull(i)) {
      ARROW_RETURN_NOT_OK(residual.AppendNull());
      ARROW_RETURN_NOT_OK(typed_builder.AppendNull());
      continue;
    }
    auto bytes = GetBinaryValue(value_array, i);
    auto* data = reinterpret_cast<const uint8_t*>(bytes.data());
    auto len = static_cast<int64_t>(bytes.size());
    const uint8_t* bin_data;
    int32_t bin_size;
    if (IsVariantNull(data, len)) {
      ARROW_RETURN_NOT_OK(
          residual.Append(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
      ARROW_RETURN_NOT_OK(typed_builder.AppendNull());
    } else if (ExtractBinary(data, len, &bin_data, &bin_size)) {
      ARROW_RETURN_NOT_OK(residual.AppendNull());
      ARROW_RETURN_NOT_OK(typed_builder.Append(bin_data, bin_size));
    } else {
      ARROW_RETURN_NOT_OK(
          residual.Append(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
      ARROW_RETURN_NOT_OK(typed_builder.AppendNull());
    }
  }
  return typed_builder.Finish(out);
}

}  // namespace

// ---------------------------------------------------------------------------
// ShredVariantColumn — dispatch by schema kind
// ---------------------------------------------------------------------------

Result<std::shared_ptr<StructArray>> ShredVariantColumn(
    const std::shared_ptr<Array>& metadata_array,
    const std::shared_ptr<Array>& value_array, const VariantShreddingSchema& schema) {
  // Validate input array types — GetBinaryValue silently returns empty for
  // unsupported types, which would cause subtle data corruption.
  if (metadata_array->type_id() != Type::BINARY &&
      metadata_array->type_id() != Type::LARGE_BINARY &&
      metadata_array->type_id() != Type::BINARY_VIEW) {
    return Status::Invalid(
        "ShredVariantColumn: metadata_array must be BINARY, LARGE_BINARY, or "
        "BINARY_VIEW, got ",
        metadata_array->type()->ToString());
  }
  if (value_array->type_id() != Type::BINARY &&
      value_array->type_id() != Type::LARGE_BINARY &&
      value_array->type_id() != Type::BINARY_VIEW) {
    return Status::Invalid(
        "ShredVariantColumn: value_array must be BINARY, LARGE_BINARY, or BINARY_VIEW, "
        "got ",
        value_array->type()->ToString());
  }
  if (metadata_array->length() != value_array->length()) {
    return Status::Invalid(
        "ShredVariantColumn: metadata_array and value_array length mismatch (",
        metadata_array->length(), " vs ", value_array->length(), ")");
  }

  if (schema.kind() == VariantShreddingSchema::Kind::kArray) {
    return ShredVariantColumnArray(metadata_array, value_array, schema);
  }

  if (schema.kind() == VariantShreddingSchema::Kind::kObject) {
    return ShredVariantColumnObject(metadata_array, value_array, schema);
  }

  // Primitive shredding
  const int64_t num_rows = value_array->length();
  const auto& target_type = *schema.type();

  // Residual value builder (variant bytes for non-matching rows)
  BinaryBuilder residual_value_builder;
  ARROW_RETURN_NOT_OK(residual_value_builder.Reserve(num_rows));

  // NOTE (Rust divergence): Rust's shredding uses arrow::compute::cast() which
  // allows cross-type conversions (e.g., Int32→Float64, Float32→Int32). C++
  // only shreds values whose variant type matches the target column type
  // directly (with safe widening within the same numeric family, e.g.,
  // Int8→Int64). This is spec-compliant but less aggressive for predicate
  // pushdown. A future cast-based approach could be added as a separate mode.
  //
  // NOTE (Rust divergence — additional types): Rust additionally supports
  // Uint8/16/32/64, Float16, Decimal32, Decimal64, Decimal256, and
  // TimestampSecond/Millisecond as shredding targets. These require the
  // cast-based mode (variant spec only encodes signed ints, float32/64,
  // and timestamp micros/nanos natively). Adding them is straightforward
  // once the CastOptions mode is implemented.
  std::shared_ptr<Array> typed_result;

  switch (target_type.id()) {
    case Type::INT64: {
      // Note: No IsVariantCompatibleWithType() gatekeeper here because
      // ExtractInt64() already accepts only Int8/Int16/Int32/Int64 variants,
      // and all int→int64 widening is unconditionally valid per the spec.
      // This differs from TIMESTAMP/DECIMAL which need explicit type matching.
      auto st = ShredPrimitiveLoop<Int64Builder, int64_t>(
          *value_array, num_rows, residual_value_builder, &typed_result,
          [&](const uint8_t* data, int64_t len, int64_t* out) {
            return ExtractInt64(data, len, out);
          });
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::DOUBLE: {
      auto st = ShredPrimitiveLoop<DoubleBuilder, double>(
          *value_array, num_rows, residual_value_builder, &typed_result,
          [&](const uint8_t* data, int64_t len, double* out) {
            return ExtractDoubleOrFloat(data, len, out);
          });
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::BOOL: {
      auto st = ShredPrimitiveLoop<BooleanBuilder, bool>(
          *value_array, num_rows, residual_value_builder, &typed_result,
          [&](const uint8_t* data, int64_t len, bool* out) {
            return ExtractBool(data, len, out);
          });
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::STRING: {
      auto st = ShredPrimitiveLoop<StringBuilder, std::string_view>(
          *value_array, num_rows, residual_value_builder, &typed_result,
          [&](const uint8_t* data, int64_t len, std::string_view* out) {
            return ExtractString(data, len, out);
          });
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::LARGE_STRING: {
      auto st = ShredPrimitiveLoop<LargeStringBuilder, std::string_view>(
          *value_array, num_rows, residual_value_builder, &typed_result,
          [&](const uint8_t* data, int64_t len, std::string_view* out) {
            return ExtractString(data, len, out);
          });
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::FLOAT: {
      auto st = ShredPrimitiveLoop<FloatBuilder, float>(
          *value_array, num_rows, residual_value_builder, &typed_result,
          [&](const uint8_t* data, int64_t len, float* out) {
            return ExtractFloat(data, len, out);
          });
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::DATE32: {
      auto st = ShredPrimitiveLoop<Date32Builder, int32_t>(
          *value_array, num_rows, residual_value_builder, &typed_result,
          [&](const uint8_t* data, int64_t len, int32_t* out) {
            return ExtractDate32(data, len, out);
          });
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::TIMESTAMP: {
      // Use Int64 storage for timestamps (Arrow stores timestamps as int64).
      // IsVariantCompatibleWithType enforces TimeUnit and timezone matching,
      // so only correctly-matching timestamp variants reach the typed column.
      auto st = ShredPrimitiveLoop<Int64Builder, int64_t>(
          *value_array, num_rows, residual_value_builder, &typed_result,
          [&](const uint8_t* data, int64_t len, int64_t* out) {
            return IsVariantCompatibleWithType(data, len, target_type) &&
                   ExtractTimestamp(data, len, out);
          });
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::INT8: {
      auto st = ShredPrimitiveLoop<Int8Builder, int8_t>(
          *value_array, num_rows, residual_value_builder, &typed_result,
          [&](const uint8_t* data, int64_t len, int8_t* out) {
            return ExtractInt8(data, len, out);
          });
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::INT16: {
      auto st = ShredPrimitiveLoop<Int16Builder, int16_t>(
          *value_array, num_rows, residual_value_builder, &typed_result,
          [&](const uint8_t* data, int64_t len, int16_t* out) {
            return ExtractInt16(data, len, out);
          });
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::INT32: {
      auto st = ShredPrimitiveLoop<Int32Builder, int32_t>(
          *value_array, num_rows, residual_value_builder, &typed_result,
          [&](const uint8_t* data, int64_t len, int32_t* out) {
            return ExtractInt32(data, len, out);
          });
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::BINARY: {
      auto st = ShredBinaryLoop<BinaryBuilder>(*value_array, num_rows,
                                               residual_value_builder, &typed_result);
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::LARGE_BINARY: {
      auto st = ShredBinaryLoop<LargeBinaryBuilder>(
          *value_array, num_rows, residual_value_builder, &typed_result);
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::STRING_VIEW: {
      auto st = ShredPrimitiveLoop<StringViewBuilder, std::string_view>(
          *value_array, num_rows, residual_value_builder, &typed_result,
          [&](const uint8_t* data, int64_t len, std::string_view* out) {
            return ExtractString(data, len, out);
          });
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::BINARY_VIEW: {
      // Note: ShredBinaryLoop calls typed_builder.Append(bin_data, bin_size) where
      // bin_size is int32_t. BinaryViewBuilder::Append accepts int64_t — the implicit
      // widening from int32_t is safe and produces correct results.
      auto st = ShredBinaryLoop<BinaryViewBuilder>(*value_array, num_rows,
                                                   residual_value_builder, &typed_result);
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::TIME64: {
      auto st = ShredPrimitiveLoop<Int64Builder, int64_t>(
          *value_array, num_rows, residual_value_builder, &typed_result,
          [&](const uint8_t* data, int64_t len, int64_t* out) {
            return ExtractTime64(data, len, out);
          });
      ARROW_RETURN_NOT_OK(st);
      break;
    }

    case Type::FIXED_SIZE_BINARY: {
      // UUID = FixedSizeBinary(16)
      auto& fsb_type = static_cast<const FixedSizeBinaryType&>(target_type);
      if (fsb_type.byte_width() != 16) {
        return Status::NotImplemented(
            "ShredVariantColumn: only FixedSizeBinary(16) for UUID is supported");
      }
      FixedSizeBinaryBuilder typed_builder(fixed_size_binary(kUUIDByteLength));
      ARROW_RETURN_NOT_OK(typed_builder.Reserve(num_rows));
      for (int64_t i = 0; i < num_rows; ++i) {
        if (value_array->IsNull(i)) {
          ARROW_RETURN_NOT_OK(residual_value_builder.AppendNull());
          ARROW_RETURN_NOT_OK(typed_builder.AppendNull());
          continue;
        }
        auto bytes = GetBinaryValue(*value_array, i);
        auto* data = reinterpret_cast<const uint8_t*>(bytes.data());
        auto len = static_cast<int64_t>(bytes.size());
        uint8_t uuid_bytes[kUUIDByteLength];
        if (IsVariantNull(data, len)) {
          ARROW_RETURN_NOT_OK(residual_value_builder.Append(
              reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
          ARROW_RETURN_NOT_OK(typed_builder.AppendNull());
        } else if (ExtractUUID(data, len, uuid_bytes)) {
          ARROW_RETURN_NOT_OK(residual_value_builder.AppendNull());
          ARROW_RETURN_NOT_OK(typed_builder.Append(uuid_bytes));
        } else {
          ARROW_RETURN_NOT_OK(residual_value_builder.Append(
              reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
          ARROW_RETURN_NOT_OK(typed_builder.AppendNull());
        }
      }
      ARROW_RETURN_NOT_OK(typed_builder.Finish(&typed_result));
      break;
    }

    case Type::DECIMAL128: {
      // Decimal128 shredding — extracts scale from variant, stores unscaled value.
      // IsVariantCompatibleWithType checks both type and scale compatibility.
      // Note: Rust also supports Decimal32/Decimal64 as separate shredding targets
      // (via VariantDecimal4/VariantDecimal8 types). C++ Arrow only has Decimal128/256,
      // so we consolidate all decimal widths into Decimal128 storage.
      Decimal128Builder typed_builder(schema.type());
      ARROW_RETURN_NOT_OK(typed_builder.Reserve(num_rows));
      for (int64_t i = 0; i < num_rows; ++i) {
        if (value_array->IsNull(i)) {
          ARROW_RETURN_NOT_OK(residual_value_builder.AppendNull());
          ARROW_RETURN_NOT_OK(typed_builder.AppendNull());
          continue;
        }
        auto bytes = GetBinaryValue(*value_array, i);
        auto* data = reinterpret_cast<const uint8_t*>(bytes.data());
        auto len = static_cast<int64_t>(bytes.size());
        int64_t low = 0, high = 0;
        uint8_t scale = 0;
        if (IsVariantNull(data, len)) {
          ARROW_RETURN_NOT_OK(residual_value_builder.Append(
              reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
          ARROW_RETURN_NOT_OK(typed_builder.AppendNull());
        } else if (IsVariantCompatibleWithType(data, len, target_type) &&
                   ExtractDecimal128(data, len, &low, &high, &scale)) {
          Decimal128 dec_val(high, static_cast<uint64_t>(low));
          ARROW_RETURN_NOT_OK(typed_builder.Append(dec_val));
          ARROW_RETURN_NOT_OK(residual_value_builder.AppendNull());
        } else {
          ARROW_RETURN_NOT_OK(residual_value_builder.Append(
              reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
          ARROW_RETURN_NOT_OK(typed_builder.AppendNull());
        }
      }
      ARROW_RETURN_NOT_OK(typed_builder.Finish(&typed_result));
      break;
    }

      // TODO GH-45948 follow-up: Add FixedSizeList and ListView as array
      // shredding targets (Rust supports all list-like types via GenericListArray).
      // LargeList is supported in reconstruction but not as a distinct shredding
      // output (shredding always produces ListArray with 32-bit offsets).
      // TODO GH-45948 follow-up (Rust parity — cast mode): Add support for:
      //   - Uint8/16/32/64 (variant spec only encodes signed ints; requires cast)
      //   - Float16 (variant spec encodes Float32/64; requires cast/truncation)
      //   - Decimal32, Decimal64 (Rust has VariantDecimal4/8 dedicated types)
      //   - TimestampSecond, TimestampMillisecond (variant spec only has micros/nanos;
      //     requires unit conversion similar to Rust's CastOptions approach)
      //   These all require a CastOptions-based extraction mode analogous to Rust's
      //   `shred_variant_with_options()` which uses arrow::compute::cast().

    default:
      return Status::NotImplemented("ShredVariantColumn: unsupported target type ",
                                    target_type.ToString());
  }

  std::shared_ptr<Array> residual_result;
  ARROW_RETURN_NOT_OK(residual_value_builder.Finish(&residual_result));

  // Determine the output field type for typed_value. For most types this is
  // schema.type() directly. For TIMESTAMP and TIME64, the builder produces
  // Int64Array (physical storage), so declare the field as int64() to match.
  // The reconstruction path uses schema.type()->id() to re-encode correctly.
  auto typed_field_type = schema.type();
  if (typed_field_type->id() == Type::TIMESTAMP ||
      typed_field_type->id() == Type::TIME64) {
    typed_field_type = int64();
  }

  auto output_fields = std::vector<std::shared_ptr<Field>>{
      field("metadata", metadata_array->type(), /*nullable=*/false),
      field("value", binary(), /*nullable=*/true),
      field("typed_value", typed_field_type, /*nullable=*/true),
  };

  return StructArray::Make({metadata_array, residual_result, typed_result},
                           output_fields);
}

// ---------------------------------------------------------------------------
// ReconstructVariantColumnArray
// ---------------------------------------------------------------------------

static Result<std::shared_ptr<Array>> ReconstructVariantColumnArray(
    const std::shared_ptr<Array>& metadata_array,
    const std::shared_ptr<Array>& value_array,
    const std::shared_ptr<Array>& typed_value_array,
    const VariantShreddingSchema& schema) {
  const int64_t num_rows = metadata_array->length();

  // Validate typed_value_array is a list-like array with binary elements.
  // Accept LIST, LARGE_LIST, FIXED_SIZE_LIST, LIST_VIEW, and LARGE_LIST_VIEW
  // to support all Parquet list-like representations (Rust parity).
  if (typed_value_array->type_id() != Type::LIST &&
      typed_value_array->type_id() != Type::LARGE_LIST &&
      typed_value_array->type_id() != Type::FIXED_SIZE_LIST &&
      typed_value_array->type_id() != Type::LIST_VIEW &&
      typed_value_array->type_id() != Type::LARGE_LIST_VIEW) {
    return Status::Invalid(
        "ReconstructVariantColumnArray: typed_value_array must be LIST, LARGE_LIST, "
        "FIXED_SIZE_LIST, LIST_VIEW, or LARGE_LIST_VIEW, got ",
        typed_value_array->type()->ToString());
  }

  BinaryBuilder output_builder;
  ARROW_RETURN_NOT_OK(output_builder.Reserve(num_rows));

  // Generic lambda that handles any list-like type with value_offset(i) + values().
  // Works for LIST, LARGE_LIST, FIXED_SIZE_LIST, LIST_VIEW, and LARGE_LIST_VIEW.
  // Elements can be either raw BINARY (legacy format) or STRUCT{value, typed_value}
  // (recursively shredded format). We detect which format at runtime.
  auto reconstruct_rows_offset_based = [&](auto* list_arr) -> Status {
    auto value_type_id = list_arr->value_type()->id();

    if (value_type_id == Type::BINARY) {
      // Legacy format: elements are raw binary variant bytes.
      DCHECK_NE(list_arr->values(), nullptr);
      const auto* elem_arr = static_cast<const BinaryArray*>(list_arr->values().get());

      for (int64_t i = 0; i < num_rows; ++i) {
        bool value_present = value_array && value_array->IsValid(i);
        bool typed_present = typed_value_array && typed_value_array->IsValid(i);

        if (!value_present && !typed_present) {
          uint8_t null_byte = 0x00;
          ARROW_RETURN_NOT_OK(output_builder.Append(&null_byte, 1));
        } else if (value_present && !typed_present) {
          auto bytes = GetBinaryValue(*value_array, i);
          ARROW_RETURN_NOT_OK(output_builder.Append(
              reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
        } else if (!value_present && typed_present) {
          VariantBuilder vb;
          auto start = vb.Offset();
          std::vector<int64_t> offsets;

          auto list_start = list_arr->value_offset(i);
          auto list_length = list_arr->value_length(i);
          for (decltype(list_length) ei = 0; ei < list_length; ++ei) {
            offsets.push_back(vb.NextElement(start));
            auto elem_view = elem_arr->GetView(static_cast<int64_t>(list_start + ei));
            vb.UnsafeAppendEncoded(reinterpret_cast<const uint8_t*>(elem_view.data()),
                                   static_cast<int64_t>(elem_view.size()));
          }

          ARROW_RETURN_NOT_OK(vb.FinishArray(start, offsets));
          ARROW_ASSIGN_OR_RAISE(auto arr_bytes, vb.BuildWithoutMeta());
          ARROW_RETURN_NOT_OK(output_builder.Append(
              arr_bytes.data(), static_cast<int32_t>(arr_bytes.size())));
        } else {
          return Status::Invalid(
              "ReconstructVariantColumn: both value and typed_value non-null "
              "at row ",
              i, " (not valid for array schemas)");
        }
      }
    } else if (value_type_id == Type::STRUCT) {
      // Recursively shredded format: elements are struct{value, typed_value}.
      // Reconstruct elements first at the column level, then use the
      // reconstructed per-element binary to build variant arrays.
      DCHECK_NE(list_arr->values(), nullptr);
      const auto* elem_struct = static_cast<const StructArray*>(list_arr->values().get());

      if (elem_struct->num_fields() != 2) {
        return Status::Invalid(
            "ReconstructVariantColumnArray: element struct must have 2 fields "
            "(value, typed_value), got ",
            elem_struct->num_fields());
      }
      auto elem_value_col = elem_struct->field(0);
      auto elem_typed_col = elem_struct->field(1);

      // Build a metadata array for elements by replicating row metadata.
      int64_t total_elements = elem_struct->length();
      BinaryBuilder elem_meta_builder;
      ARROW_RETURN_NOT_OK(elem_meta_builder.Reserve(total_elements));
      for (int64_t i = 0; i < num_rows; ++i) {
        if (!typed_value_array->IsValid(i)) continue;
        auto meta_bytes = GetBinaryValue(*metadata_array, i);
        auto list_length = list_arr->value_length(i);
        for (decltype(list_length) ei = 0; ei < list_length; ++ei) {
          ARROW_RETURN_NOT_OK(elem_meta_builder.Append(
              reinterpret_cast<const uint8_t*>(meta_bytes.data()), meta_bytes.size()));
        }
      }
      std::shared_ptr<Array> elem_meta_arr;
      ARROW_RETURN_NOT_OK(elem_meta_builder.Finish(&elem_meta_arr));

      if (elem_meta_arr->length() != total_elements) {
        return Status::Invalid(
            "ReconstructVariantColumnArray: element metadata count mismatch (",
            elem_meta_arr->length(), " vs ", total_elements, " elements)");
      }

      // Recursively reconstruct all elements at once (column-level operation)
      ARROW_ASSIGN_OR_RAISE(
          auto reconstructed_elements,
          ReconstructVariantColumn(elem_meta_arr, elem_value_col, elem_typed_col,
                                   schema.element_schema()));

      // Build variant arrays from the reconstructed element bytes
      for (int64_t i = 0; i < num_rows; ++i) {
        bool value_present = value_array && value_array->IsValid(i);
        bool typed_present = typed_value_array && typed_value_array->IsValid(i);

        if (!value_present && !typed_present) {
          uint8_t null_byte = 0x00;
          ARROW_RETURN_NOT_OK(output_builder.Append(&null_byte, 1));
        } else if (value_present && !typed_present) {
          auto bytes = GetBinaryValue(*value_array, i);
          ARROW_RETURN_NOT_OK(output_builder.Append(
              reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
        } else if (!value_present && typed_present) {
          VariantBuilder vb;
          auto start = vb.Offset();
          std::vector<int64_t> offsets;

          auto list_start = list_arr->value_offset(i);
          auto list_length = list_arr->value_length(i);
          for (decltype(list_length) ei = 0; ei < list_length; ++ei) {
            offsets.push_back(vb.NextElement(start));
            auto elem_bytes = GetBinaryValue(*reconstructed_elements,
                                             static_cast<int64_t>(list_start + ei));
            vb.UnsafeAppendEncoded(reinterpret_cast<const uint8_t*>(elem_bytes.data()),
                                   static_cast<int64_t>(elem_bytes.size()));
          }

          ARROW_RETURN_NOT_OK(vb.FinishArray(start, offsets));
          ARROW_ASSIGN_OR_RAISE(auto arr_bytes, vb.BuildWithoutMeta());
          ARROW_RETURN_NOT_OK(output_builder.Append(
              arr_bytes.data(), static_cast<int32_t>(arr_bytes.size())));
        } else {
          return Status::Invalid(
              "ReconstructVariantColumn: both value and typed_value non-null "
              "at row ",
              i, " (not valid for array schemas)");
        }
      }
    } else {
      return Status::Invalid(
          "ReconstructVariantColumnArray: list elements must be BINARY or "
          "STRUCT{value, typed_value}, got ",
          list_arr->value_type()->ToString());
    }
    return Status::OK();
  };

  switch (typed_value_array->type_id()) {
    case Type::LIST:
      ARROW_RETURN_NOT_OK(reconstruct_rows_offset_based(
          static_cast<const ListArray*>(typed_value_array.get())));
      break;
    case Type::LARGE_LIST:
      ARROW_RETURN_NOT_OK(reconstruct_rows_offset_based(
          static_cast<const LargeListArray*>(typed_value_array.get())));
      break;
    case Type::FIXED_SIZE_LIST:
      ARROW_RETURN_NOT_OK(reconstruct_rows_offset_based(
          static_cast<const FixedSizeListArray*>(typed_value_array.get())));
      break;
    case Type::LIST_VIEW:
      ARROW_RETURN_NOT_OK(reconstruct_rows_offset_based(
          static_cast<const ListViewArray*>(typed_value_array.get())));
      break;
    case Type::LARGE_LIST_VIEW:
      ARROW_RETURN_NOT_OK(reconstruct_rows_offset_based(
          static_cast<const LargeListViewArray*>(typed_value_array.get())));
      break;
    default:
      return Status::Invalid(
          "ReconstructVariantColumnArray: unexpected typed_value type ",
          typed_value_array->type()->ToString());
  }

  std::shared_ptr<Array> result;
  ARROW_RETURN_NOT_OK(output_builder.Finish(&result));
  return result;
}

// ---------------------------------------------------------------------------
// ReconstructVariantColumnObject
// ---------------------------------------------------------------------------

static Result<std::shared_ptr<Array>> ReconstructVariantColumnObject(
    const std::shared_ptr<Array>& metadata_array,
    const std::shared_ptr<Array>& value_array,
    const std::shared_ptr<Array>& typed_value_array,
    const VariantShreddingSchema& schema) {
  const int64_t num_rows = metadata_array->length();
  const auto& schema_fields = schema.fields();

  // Validate typed_value_array is the expected StructArray.
  if (typed_value_array->type_id() != Type::STRUCT) {
    return Status::Invalid(
        "ReconstructVariantColumnObject: typed_value_array must be STRUCT, got ",
        typed_value_array->type()->ToString());
  }

  BinaryBuilder output_builder;
  ARROW_RETURN_NOT_OK(output_builder.Reserve(num_rows));

  // typed_value should be a StructArray with one field per schema field
  const auto* typed_struct = static_cast<const StructArray*>(typed_value_array.get());

  // Validate that the typed_value struct has the expected number of fields.
  if (typed_struct->num_fields() != static_cast<int>(schema_fields.size())) {
    return Status::Invalid("ReconstructVariantColumnObject: typed_value struct has ",
                           typed_struct->num_fields(), " fields but schema expects ",
                           schema_fields.size());
  }

  // Pre-compute per-field reconstructed variant arrays for fields with
  // primitive sub-schemas. This avoids O(n²) by calling ReconstructVariantColumn
  // once per field (column-level) rather than once per row.
  std::vector<std::shared_ptr<Array>> field_reconstructed_arrays(schema_fields.size());
  for (size_t sf = 0; sf < schema_fields.size(); ++sf) {
    const auto& sub_schema = schema_fields[sf].second;
    if (sub_schema.kind() == VariantShreddingSchema::Kind::kPrimitive) {
      auto field_struct_col = typed_struct->field(static_cast<int>(sf));
      auto* field_struct = static_cast<const StructArray*>(field_struct_col.get());
      auto field_value_col = field_struct->field(0);  // "value" sub-column
      auto field_typed_col = field_struct->field(1);  // "typed_value" sub-column

      ARROW_ASSIGN_OR_RAISE(field_reconstructed_arrays[sf],
                            ReconstructVariantColumn(metadata_array, field_value_col,
                                                     field_typed_col, sub_schema));
    }
  }

  // Cache for metadata reuse: in columnar data, all rows typically share the
  // same metadata dictionary. By comparing metadata bytes across rows, we avoid
  // redundant DecodeMetadata calls and VariantBuilder dictionary copies.
  // Lifetime safety: cached_meta_bytes is a string_view into metadata_array's
  // buffer, which is kept alive by the shared_ptr parameter for this function's
  // entire duration. Arrow arrays never relocate their backing buffers.
  std::string_view cached_meta_bytes;
  VariantMetadata cached_meta;
  // Cached builder: reused when consecutive rows share metadata. After
  // BuildWithoutMeta(), the builder's buffer is cleared but its dictionary
  // (dict_, dict_keys_) is preserved — exactly what we need for the next row.
  std::unique_ptr<VariantBuilder> cached_builder;

  for (int64_t i = 0; i < num_rows; ++i) {
    bool value_present = value_array && value_array->IsValid(i);
    bool typed_present = typed_value_array && typed_value_array->IsValid(i);

    if (!value_present && !typed_present) {
      // Both null → Variant null (see comment in primitive reconstruction
      // about SQL NULL vs variant-null ambiguity)
      uint8_t null_byte = 0x00;
      ARROW_RETURN_NOT_OK(output_builder.Append(&null_byte, 1));
      continue;
    }

    if (value_present && !typed_present) {
      // Non-object value stored in residual
      auto bytes = GetBinaryValue(*value_array, i);
      ARROW_RETURN_NOT_OK(output_builder.Append(
          reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
      continue;
    }

    // typed_present: reconstruct object from shredded fields + residual
    auto meta_bytes = GetBinaryValue(*metadata_array, i);

    // Reuse cached metadata and builder if bytes are identical (common case:
    // all rows share the same metadata dictionary). This avoids O(n × k)
    // dictionary copies from VariantBuilder construction per row.
    if (meta_bytes != cached_meta_bytes) {
      ARROW_ASSIGN_OR_RAISE(
          cached_meta, DecodeMetadata(reinterpret_cast<const uint8_t*>(meta_bytes.data()),
                                      static_cast<int64_t>(meta_bytes.size())));
      cached_meta_bytes = meta_bytes;
      cached_builder = std::make_unique<VariantBuilder>(cached_meta);
      cached_builder->SetAllowDuplicates(true);
    }

    // Reuse the cached builder: BuildWithoutMeta() clears the buffer but
    // preserves the dictionary, so NextField() can resolve keys without
    // redundant hash map insertions.
    VariantBuilder& vb = *cached_builder;
    auto start = vb.Offset();
    std::vector<VariantBuilder::FieldEntry> fields;

    // Add shredded fields from typed_value struct
    for (size_t sf = 0; sf < schema_fields.size(); ++sf) {
      const auto& sub_schema = schema_fields[sf].second;

      if (sub_schema.kind() == VariantShreddingSchema::Kind::kPrimitive &&
          field_reconstructed_arrays[sf]) {
        // Primitive sub-schema: use the pre-computed reconstructed array.
        // Check the original sub-field struct to determine if the field was
        // present (value or typed_value non-null) or absent (both null).
        auto field_struct_col = typed_struct->field(static_cast<int>(sf));
        auto* field_struct = static_cast<const StructArray*>(field_struct_col.get());
        auto field_value_col = field_struct->field(0);
        auto field_typed_col = field_struct->field(1);
        bool field_present = field_value_col->IsValid(i) || field_typed_col->IsValid(i);

        if (field_present) {
          auto& recon_arr = field_reconstructed_arrays[sf];
          auto recon_bytes = GetBinaryValue(*recon_arr, i);
          if (!recon_bytes.empty()) {
            fields.push_back(vb.NextField(start, schema_fields[sf].first));
            vb.UnsafeAppendEncoded(reinterpret_cast<const uint8_t*>(recon_bytes.data()),
                                   static_cast<int64_t>(recon_bytes.size()));
          }
        }
      } else {
        // Non-primitive or no reconstruction available: read from value sub-column
        auto field_struct_col = typed_struct->field(static_cast<int>(sf));
        auto* field_struct = static_cast<const StructArray*>(field_struct_col.get());
        auto field_value_col = field_struct->field(0);  // "value" sub-column

        if (field_value_col->IsValid(i)) {
          auto field_bytes = GetBinaryValue(*field_value_col, i);
          fields.push_back(vb.NextField(start, schema_fields[sf].first));
          vb.UnsafeAppendEncoded(reinterpret_cast<const uint8_t*>(field_bytes.data()),
                                 static_cast<int64_t>(field_bytes.size()));
        }
      }
      // Both null → field is missing from this object, skip it
    }

    // Add residual fields (if value is present alongside typed_value)
    if (value_present) {
      auto residual_bytes = GetBinaryValue(*value_array, i);
      auto* residual_data = reinterpret_cast<const uint8_t*>(residual_bytes.data());
      auto residual_len = static_cast<int64_t>(residual_bytes.size());

      if (residual_len > 0 && GetBasicType(residual_data[0]) == BasicType::kObject) {
        ARROW_ASSIGN_OR_RAISE(
            auto residual_obj,
            VariantObjectView::Make(cached_meta, residual_data, residual_len));
        for (int32_t fi = 0; fi < residual_obj.num_fields(); ++fi) {
          ARROW_ASSIGN_OR_RAISE(auto fname, residual_obj.field_name(fi));
          ARROW_ASSIGN_OR_RAISE(auto fvalue, residual_obj.field_value(fi));
          fields.push_back(vb.NextField(start, fname));
          vb.UnsafeAppendEncoded(fvalue.data(), fvalue.size_bytes());
        }
      }
    }

    ARROW_RETURN_NOT_OK(vb.FinishObject(start, fields));
    ARROW_ASSIGN_OR_RAISE(auto obj_bytes, vb.BuildWithoutMeta());
    ARROW_RETURN_NOT_OK(
        output_builder.Append(obj_bytes.data(), static_cast<int32_t>(obj_bytes.size())));
  }

  std::shared_ptr<Array> result;
  ARROW_RETURN_NOT_OK(output_builder.Finish(&result));
  return result;
}

// ---------------------------------------------------------------------------
// ReconstructVariantColumn — dispatch by schema kind
// ---------------------------------------------------------------------------

Result<std::shared_ptr<Array>> ReconstructVariantColumn(
    const std::shared_ptr<Array>& metadata_array,
    const std::shared_ptr<Array>& value_array,
    const std::shared_ptr<Array>& typed_value_array, const VariantShreddingSchema& schema,
    std::shared_ptr<Buffer>* out_null_bitmap) {
  // Validate array lengths are consistent.
  if (metadata_array->length() != value_array->length() ||
      metadata_array->length() != typed_value_array->length()) {
    return Status::Invalid("ReconstructVariantColumn: array length mismatch (metadata=",
                           metadata_array->length(), ", value=", value_array->length(),
                           ", typed_value=", typed_value_array->length(), ")");
  }
  // Validate input array types — GetBinaryValue silently returns empty for
  // unsupported types, which would cause subtle data corruption.
  if (metadata_array->type_id() != Type::BINARY &&
      metadata_array->type_id() != Type::LARGE_BINARY &&
      metadata_array->type_id() != Type::BINARY_VIEW) {
    return Status::Invalid(
        "ReconstructVariantColumn: metadata_array must be BINARY, LARGE_BINARY, or "
        "BINARY_VIEW, got ",
        metadata_array->type()->ToString());
  }
  if (value_array->type_id() != Type::BINARY &&
      value_array->type_id() != Type::LARGE_BINARY &&
      value_array->type_id() != Type::BINARY_VIEW) {
    return Status::Invalid(
        "ReconstructVariantColumn: value_array must be BINARY, LARGE_BINARY, or "
        "BINARY_VIEW, got ",
        value_array->type()->ToString());
  }

  if (schema.kind() == VariantShreddingSchema::Kind::kArray) {
    auto result = ReconstructVariantColumnArray(metadata_array, value_array,
                                                typed_value_array, schema);
    if (result.ok() && out_null_bitmap) {
      // Compute null bitmap: bit=0 where both value and typed_value are null
      const int64_t num_rows = metadata_array->length();
      ARROW_ASSIGN_OR_RAISE(auto bitmap, AllocateBitmap(num_rows, default_memory_pool()));
      uint8_t* bitmap_data = bitmap->mutable_data();
      for (int64_t i = 0; i < num_rows; ++i) {
        bool valid = value_array->IsValid(i) || typed_value_array->IsValid(i);
        bit_util::SetBitTo(bitmap_data, i, valid);
      }
      *out_null_bitmap = std::move(bitmap);
    }
    return result;
  }

  if (schema.kind() == VariantShreddingSchema::Kind::kObject) {
    auto result = ReconstructVariantColumnObject(metadata_array, value_array,
                                                 typed_value_array, schema);
    if (result.ok() && out_null_bitmap) {
      const int64_t num_rows = metadata_array->length();
      ARROW_ASSIGN_OR_RAISE(auto bitmap, AllocateBitmap(num_rows, default_memory_pool()));
      uint8_t* bitmap_data = bitmap->mutable_data();
      for (int64_t i = 0; i < num_rows; ++i) {
        bool valid = value_array->IsValid(i) || typed_value_array->IsValid(i);
        bit_util::SetBitTo(bitmap_data, i, valid);
      }
      *out_null_bitmap = std::move(bitmap);
    }
    return result;
  }

  // Primitive reconstruction
  const int64_t num_rows = metadata_array->length();
  BinaryBuilder output_builder;
  ARROW_RETURN_NOT_OK(output_builder.Reserve(num_rows));

  // We need to re-encode native typed_value back to variant bytes.
  // Reused across all rows — safe because primitive encoding never adds
  // dictionary keys (only objects do). The dictionary stays empty.
  VariantBuilder vb;

  for (int64_t i = 0; i < num_rows; ++i) {
    bool value_present = value_array && value_array->IsValid(i);
    bool typed_present = typed_value_array && typed_value_array->IsValid(i);

    if (!value_present && !typed_present) {
      // Both null → Variant null (single byte: 0x00)
      // NOTE: This is ambiguous — it could represent either:
      //   1. "SQL NULL / missing" (the row itself is absent), or
      //   2. "variant-typed null" (a Variant::Null that was stored in value)
      // The Rust implementation disambiguates via a separate NullBuffer on the
      // VariantArray struct. Since we don't receive a validity bitmap here,
      // we conservatively emit Variant::Null (0x00). Callers that need to
      // distinguish SQL NULL from variant-null should check the struct-level
      // validity bitmap before calling ReconstructVariantColumn.
      // TODO GH-45948 follow-up (Rust parity — NullBuffer): Accept an optional
      // validity bitmap to propagate struct-level nulls into the output, matching
      // Rust's `unshred_variant()` which returns a separate NullBuffer.
      uint8_t null_byte = 0x00;
      ARROW_RETURN_NOT_OK(output_builder.Append(&null_byte, 1));
    } else if (value_present && !typed_present) {
      // Value present, typed null → use residual value as-is
      auto bytes = GetBinaryValue(*value_array, i);
      ARROW_RETURN_NOT_OK(output_builder.Append(
          reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
    } else if (!value_present && typed_present) {
      // Typed present, value null → re-encode native value as variant.
      // Dispatch on the *schema* type, not the physical array type, because
      // some types (e.g., TIMESTAMP, TIME64) are stored as Int64 arrays
      // but need to be re-encoded with their specific variant type.
      switch (schema.type()->id()) {
        case Type::INT64: {
          auto& arr = static_cast<const Int64Array&>(*typed_value_array);
          // Note: vb.Int() auto-sizes to smallest representation. This means
          // Shred(Int64(42))→Reconstruct() produces Int8(42). The *value* is
          // preserved but the encoding width may narrow. This matches Rust.
          ARROW_RETURN_NOT_OK(vb.Int(arr.Value(i)));
          break;
        }
        case Type::DOUBLE: {
          auto& arr = static_cast<const DoubleArray&>(*typed_value_array);
          ARROW_RETURN_NOT_OK(vb.Double(arr.Value(i)));
          break;
        }
        case Type::FLOAT: {
          auto& arr = static_cast<const FloatArray&>(*typed_value_array);
          ARROW_RETURN_NOT_OK(vb.Float(arr.Value(i)));
          break;
        }
        case Type::BOOL: {
          auto& arr = static_cast<const BooleanArray&>(*typed_value_array);
          ARROW_RETURN_NOT_OK(vb.Bool(arr.Value(i)));
          break;
        }
        case Type::STRING: {
          auto& arr = static_cast<const StringArray&>(*typed_value_array);
          ARROW_RETURN_NOT_OK(vb.String(arr.GetView(i)));
          break;
        }
        case Type::LARGE_STRING: {
          auto& arr = static_cast<const LargeStringArray&>(*typed_value_array);
          auto view = arr.GetView(i);
          ARROW_RETURN_NOT_OK(vb.String(std::string_view(view.data(), view.size())));
          break;
        }
        case Type::DATE32: {
          auto& arr = static_cast<const Date32Array&>(*typed_value_array);
          ARROW_RETURN_NOT_OK(vb.Date(arr.Value(i)));
          break;
        }
        case Type::TIMESTAMP: {
          auto& arr = static_cast<const Int64Array&>(*typed_value_array);
          // Determine the correct timestamp variant from the schema type.
          // The schema carries TimeUnit and timezone, which tells us which
          // variant encoding method to use for faithful round-trip.
          auto& ts_type = static_cast<const TimestampType&>(*schema.type());
          bool has_tz = !ts_type.timezone().empty();
          if (ts_type.unit() == TimeUnit::NANO) {
            if (has_tz) {
              ARROW_RETURN_NOT_OK(vb.TimestampNanos(arr.Value(i)));
            } else {
              ARROW_RETURN_NOT_OK(vb.TimestampNanosNTZ(arr.Value(i)));
            }
          } else {
            // MICRO unit (guaranteed by IsVariantCompatibleWithType which rejects
            // SECOND/MILLI/other units during shredding)
            if (has_tz) {
              ARROW_RETURN_NOT_OK(vb.TimestampMicros(arr.Value(i)));
            } else {
              ARROW_RETURN_NOT_OK(vb.TimestampMicrosNTZ(arr.Value(i)));
            }
          }
          break;
        }
        case Type::BINARY: {
          auto& arr = static_cast<const BinaryArray&>(*typed_value_array);
          auto view = arr.GetView(i);
          ARROW_RETURN_NOT_OK(vb.Binary(
              std::string_view(reinterpret_cast<const char*>(view.data()), view.size())));
          break;
        }
        case Type::LARGE_BINARY: {
          auto& arr = static_cast<const LargeBinaryArray&>(*typed_value_array);
          auto view = arr.GetView(i);
          ARROW_RETURN_NOT_OK(vb.Binary(
              std::string_view(reinterpret_cast<const char*>(view.data()), view.size())));
          break;
        }
        case Type::STRING_VIEW: {
          auto& arr = static_cast<const StringViewArray&>(*typed_value_array);
          ARROW_RETURN_NOT_OK(vb.String(arr.GetView(i)));
          break;
        }
        case Type::BINARY_VIEW: {
          auto& arr = static_cast<const BinaryViewArray&>(*typed_value_array);
          auto view = arr.GetView(i);
          ARROW_RETURN_NOT_OK(vb.Binary(view));
          break;
        }
        case Type::INT8: {
          auto& arr = static_cast<const Int8Array&>(*typed_value_array);
          ARROW_RETURN_NOT_OK(vb.Int8(arr.Value(i)));
          break;
        }
        case Type::INT16: {
          auto& arr = static_cast<const Int16Array&>(*typed_value_array);
          ARROW_RETURN_NOT_OK(vb.Int16(arr.Value(i)));
          break;
        }
        case Type::INT32: {
          auto& arr = static_cast<const Int32Array&>(*typed_value_array);
          ARROW_RETURN_NOT_OK(vb.Int32(arr.Value(i)));
          break;
        }
        case Type::TIME64: {
          auto& arr = static_cast<const Int64Array&>(*typed_value_array);
          ARROW_RETURN_NOT_OK(vb.TimeNTZ(arr.Value(i)));
          break;
        }
        case Type::FIXED_SIZE_BINARY: {
          auto& arr = static_cast<const FixedSizeBinaryArray&>(*typed_value_array);
          ARROW_RETURN_NOT_OK(vb.UUID(arr.GetValue(i)));
          break;
        }
        case Type::DECIMAL128: {
          auto& arr = static_cast<const Decimal128Array&>(*typed_value_array);
          auto& dec_type = static_cast<const Decimal128Type&>(*typed_value_array->type());
          Decimal128 val(arr.GetValue(i));
          uint8_t scale = static_cast<uint8_t>(dec_type.scale());
          // Preserve the smallest encoding width that can represent the value.
          // This ensures round-trip byte identity: a Decimal4 variant stays Decimal4
          // after shred→reconstruct rather than being widened to Decimal16.
          //
          // Use high_bits()/low_bits() accessors which are endian-safe (they
          // return numeric values, not raw bytes). This avoids the ToBytes()+
          // FromLittleEndian() pattern which is incorrect on big-endian.
          int64_t high_word = val.high_bits();
          int64_t low_word = static_cast<int64_t>(val.low_bits());
          // Check if the value fits in 4 bytes (int32 range)
          int32_t as_int32 = static_cast<int32_t>(low_word);
          if (high_word == (as_int32 < 0 ? -1 : 0) &&
              low_word == static_cast<int64_t>(as_int32)) {
            // Fits in Decimal4 (4-byte unscaled value)
            uint8_t d4_bytes[4];
            int32_t val32 = bit_util::ToLittleEndian(as_int32);
            std::memcpy(d4_bytes, &val32, 4);
            ARROW_RETURN_NOT_OK(vb.Decimal4(scale, d4_bytes));
          } else if (high_word == (low_word < 0 ? -1 : 0)) {
            // Fits in Decimal8 (8-byte unscaled value)
            uint8_t d8_bytes[8];
            int64_t val64 = bit_util::ToLittleEndian(low_word);
            std::memcpy(d8_bytes, &val64, 8);
            ARROW_RETURN_NOT_OK(vb.Decimal8(scale, d8_bytes));
          } else {
            // Full Decimal16 — write low and high words in little-endian
            uint8_t d16_bytes[16];
            int64_t le_low = bit_util::ToLittleEndian(low_word);
            int64_t le_high = bit_util::ToLittleEndian(high_word);
            std::memcpy(d16_bytes, &le_low, 8);
            std::memcpy(d16_bytes + 8, &le_high, 8);
            ARROW_RETURN_NOT_OK(vb.Decimal16(scale, d16_bytes));
          }
          break;
        }
        // TODO GH-45948 follow-up: Add FixedSizeList and ListView
        // reconstruction (LargeList is already supported above via the
        // array reconstruction path).
        // TODO GH-45948 follow-up (Rust parity — cast mode): Add Uint8/16/32/64,
        // Float16, Decimal32/64, TimestampSecond/Millisecond reconstruction.
        // See shredding switch for the full list.
        default:
          return Status::NotImplemented(
              "ReconstructVariantColumn: unsupported typed_value type ",
              typed_value_array->type()->ToString());
      }
      // Use BuildWithoutMeta() to avoid reconstructing the metadata dictionary
      // on every row — primitives don't reference dictionary keys, so the
      // metadata is irrelevant here. This avoids O(n) allocations.
      ARROW_ASSIGN_OR_RAISE(auto value_bytes, vb.BuildWithoutMeta());
      ARROW_RETURN_NOT_OK(output_builder.Append(
          value_bytes.data(), static_cast<int32_t>(value_bytes.size())));
    } else {
      // Both present → partial object (not supported for primitive schemas)
      return Status::Invalid(
          "ReconstructVariantColumn: both value and typed_value are non-null "
          "at row ",
          i, " (partial objects not supported for primitive schemas)");
    }
  }

  std::shared_ptr<Array> result;
  ARROW_RETURN_NOT_OK(output_builder.Finish(&result));

  // Compute null bitmap if requested
  if (out_null_bitmap) {
    ARROW_ASSIGN_OR_RAISE(auto bitmap, AllocateBitmap(num_rows, default_memory_pool()));
    uint8_t* bitmap_data = bitmap->mutable_data();
    for (int64_t i = 0; i < num_rows; ++i) {
      bool valid = (value_array && value_array->IsValid(i)) ||
                   (typed_value_array && typed_value_array->IsValid(i));
      bit_util::SetBitTo(bitmap_data, i, valid);
    }
    *out_null_bitmap = std::move(bitmap);
  }

  return result;
}

}  // namespace arrow::extension::variant
