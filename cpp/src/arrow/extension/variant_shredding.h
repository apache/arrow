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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_nested.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow::extension::variant {

// ---------------------------------------------------------------------------
// Shredding Schema
// ---------------------------------------------------------------------------

/// \brief Defines which fields/types should be extracted from a Variant
///        into typed Parquet columns.
///
/// A shredding schema is a tree that mirrors the structure of the Variant
/// values to be shredded:
/// - Primitive: shred the entire value as a specific Arrow type
/// - Object: shred named fields, each with its own sub-schema
/// - Array: shred array elements with a uniform element schema
///
/// This is the C++ equivalent of Rust's ShreddedSchemaBuilder.
class ARROW_EXPORT VariantShreddingSchema {
 public:
  enum class Kind { kPrimitive, kObject, kArray };

  /// \brief Create a primitive shredding schema.
  ///
  /// When applied, if the variant value's type is compatible with
  /// the target DataType, it is cast and placed in typed_value.
  /// Otherwise it remains in the value column as binary variant.
  ///
  /// \param[in] type The target Arrow DataType for the typed_value column
  static VariantShreddingSchema Primitive(std::shared_ptr<DataType> type);

  /// \brief Create an object shredding schema.
  ///
  /// When applied to an object variant, named fields are extracted
  /// according to their individual sub-schemas. Remaining fields
  /// go into the residual value column.
  ///
  /// \param[in] fields Vector of (field_name, sub_schema) pairs
  static VariantShreddingSchema Object(
      std::vector<std::pair<std::string, VariantShreddingSchema>> fields);

  /// \brief Create an array shredding schema.
  ///
  /// When applied to an array variant, each element is shredded
  /// according to the element_schema.
  ///
  /// \param[in] element_schema Schema for array elements
  static VariantShreddingSchema Array(VariantShreddingSchema element_schema);

  /// \brief Get the kind of this schema node.
  Kind kind() const { return kind_; }

  /// \brief Get the target type (only valid for Primitive kind).
  const std::shared_ptr<DataType>& type() const { return type_; }

  /// \brief Get the object fields (only valid for Object kind).
  const std::vector<std::pair<std::string, VariantShreddingSchema>>& fields() const {
    return fields_;
  }

  /// \brief Get the element schema (only valid for Array kind).
  const VariantShreddingSchema& element_schema() const { return *element_schema_; }

  /// \brief Convert this schema to the Arrow DataType for the typed_value column.
  ///
  /// - Primitive → the target DataType directly
  /// - Object → struct type with fields named per the schema
  /// - Array → list type with element struct {value?, typed_value?}
  ///
  /// NOTE: This returns the *logical* type. The actual shredded output for
  /// TIMESTAMP and TIME64 schemas uses int64() as the physical field type
  /// (since Arrow builds timestamps via Int64Builder). Callers comparing
  /// ToArrowType() output against shredded array field types should account
  /// for this TIMESTAMP/TIME64 → int64() mapping.
  std::shared_ptr<DataType> ToArrowType() const;

 private:
  Kind kind_ = Kind::kPrimitive;
  std::shared_ptr<DataType> type_;                                      // Primitive
  std::vector<std::pair<std::string, VariantShreddingSchema>> fields_;  // Object
  std::shared_ptr<VariantShreddingSchema> element_schema_;              // Array
};

// ---------------------------------------------------------------------------
// Shredding Operations
// ---------------------------------------------------------------------------

/// \brief Determine if a Variant primitive type is compatible with a target
///        Arrow DataType for shredding purposes.
///
/// Compatibility means the variant value can be losslessly represented
/// in the target typed column without falling back to the binary value column.
///
/// \param[in] variant_data Pointer to the variant value buffer
/// \param[in] variant_length Length of the variant value buffer
/// \param[in] target_type The target Arrow DataType
/// \return true if the variant value type is compatible with target_type
ARROW_EXPORT bool IsVariantCompatibleWithType(const uint8_t* variant_data,
                                              int64_t variant_length,
                                              const DataType& target_type);

// ---------------------------------------------------------------------------
// Column-level Shredding / Reconstruction
// ---------------------------------------------------------------------------

/// \brief Shred a column of variant values according to a shredding schema.
///
/// Takes an unshredded VariantArray (a StructArray with {metadata, value})
/// and produces a shredded StructArray with {metadata, value?, typed_value?}
/// where matching values are routed to typed_value and non-matching values
/// remain in value.
///
/// Type compatibility is strict: a variant value is only shredded if its
/// encoded type matches the target type directly (with safe widening within
/// the same family, e.g., Int8→Int64). Non-matching values always fall to
/// the value column safely — no errors are produced for type mismatches.
///
/// This is the C++ equivalent of Rust's `shred_variant()`.
///
/// Known Rust parity gaps (planned follow-ups):
/// - Recursive object/array sub-field shredding: Rust recursively shreds
///   nested object and array sub-fields. C++ handles primitive sub-fields
///   natively and recursively shreds array elements. Object/Array sub-schemas
///   in object fields store values as variant binary (recursive nested
///   shredding for those is a potential follow-up).
/// - CastOptions: Rust supports cross-type coercion (e.g., Int32->Float64 via
///   arrow::compute::cast); this function uses strict matching only.
/// - Additional targets: Rust supports FixedSizeList and ListView as
///   shredding output targets. Reconstruction accepts all list-like types.
///
/// \param[in] metadata_array The shared metadata column (binary array)
/// \param[in] value_array The unshredded value column (binary array)
/// \param[in] schema The shredding schema defining the typed_value target
/// \return A struct with three fields: {metadata, value(nullable), typed_value(nullable)}
ARROW_EXPORT Result<std::shared_ptr<StructArray>> ShredVariantColumn(
    const std::shared_ptr<Array>& metadata_array,
    const std::shared_ptr<Array>& value_array, const VariantShreddingSchema& schema);

/// \brief Reconstruct unshredded variant values from shredded columns.
///
/// Takes a shredded representation {metadata, value?, typed_value?} and
/// produces a fully-materialized value column where all typed_value entries
/// have been re-encoded as variant binary.
///
/// This is the C++ equivalent of Rust's `unshred_variant()`.
///
/// \param[in] metadata_array The shared metadata column (binary array)
/// \param[in] value_array The residual value column (nullable binary array)
/// \param[in] typed_value_array The shredded typed column (nullable)
/// \param[in] schema The shredding schema (needed to interpret typed_value)
/// \param[out] out_null_bitmap Optional. If non-null, receives a validity bitmap
///             where bit i is 0 when both value[i] and typed_value[i] are null
///             (indicating SQL NULL / missing row). This disambiguates SQL NULL
///             from variant-null (0x00 byte). When null (default), all rows produce
///             output (variant-null for both-null rows) matching prior behavior.
/// \return A binary array of fully-reconstructed variant values.
ARROW_EXPORT Result<std::shared_ptr<Array>> ReconstructVariantColumn(
    const std::shared_ptr<Array>& metadata_array,
    const std::shared_ptr<Array>& value_array,
    const std::shared_ptr<Array>& typed_value_array, const VariantShreddingSchema& schema,
    std::shared_ptr<Buffer>* out_null_bitmap = nullptr);

}  // namespace arrow::extension::variant
