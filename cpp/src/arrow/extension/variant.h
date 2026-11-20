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

/// \file variant.h
/// \brief Public C++ API for Variant binary encoding/decoding.
///
/// Provides zero-copy view classes for reading variant values (VariantView,
/// VariantObjectView, VariantArrayView) and a visitor interface for full
/// tree traversal. Implements the Variant Encoding Spec:
/// https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
///
/// Design principles:
///   - Parse once, query many (view classes pre-parse headers at construction)
///   - Zero-copy (string_view into source buffers, no heap allocation for reads)
///   - Type safety at boundaries (views validate at construction, not on access)
///   - O(log n) field lookup always (no threshold heuristics)

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow::extension::variant {

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Variant encoding spec version 1.
constexpr uint8_t kVariantVersion = 1;

/// Maximum nesting depth for recursive value decoding.
/// Prevents stack overflow on deeply nested (possibly malicious) input.
constexpr int32_t kMaxNestingDepth = 128;

/// UUID values are always 16 bytes (128-bit, big-endian per RFC 4122).
constexpr int32_t kUUIDByteLength = 16;

// ---------------------------------------------------------------------------
// Enumerations
// ---------------------------------------------------------------------------

/// \brief Basic type codes from bits 0-1 of the value header byte.
///
/// See:
/// https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#encoding-types
enum class BasicType : uint8_t {
  kPrimitive = 0,
  kShortString = 1,
  kObject = 2,
  kArray = 3,
};

/// \brief Primitive type codes from bits 2-7 when basic_type == kPrimitive.
///
/// See:
/// https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#encoding-types
enum class PrimitiveType : uint8_t {
  kNull = 0,
  kTrue = 1,
  kFalse = 2,
  kInt8 = 3,
  kInt16 = 4,
  kInt32 = 5,
  kInt64 = 6,
  kDouble = 7,
  kDecimal4 = 8,
  kDecimal8 = 9,
  kDecimal16 = 10,
  kDate = 11,
  kTimestampMicros = 12,
  kTimestampMicrosNTZ = 13,
  kFloat = 14,
  kBinary = 15,
  kString = 16,
  kTimeNTZ = 17,
  kTimestampNanos = 18,
  kTimestampNanosNTZ = 19,
  kUUID = 20,
};

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

/// \brief Parsed variant metadata (string dictionary).
///
/// The metadata buffer contains a header byte followed by a dictionary of
/// interned strings used as object field names. String views reference the
/// raw buffer and are valid only as long as the underlying buffer is alive.
///
/// \note This is NOT a schema — it contains key names only, not value types.
struct ARROW_EXPORT VariantMetadata {
  /// Spec version (must be kVariantVersion).
  uint8_t version = 0;

  /// Whether the dictionary strings are sorted lexicographically.
  bool is_sorted = false;

  /// Number of bytes used for each offset (1, 2, 3, or 4).
  int32_t offset_size = 0;

  /// Dictionary of interned strings. Views into the raw metadata buffer.
  std::vector<std::string_view> strings;
};

/// \brief Decode a variant metadata buffer.
///
/// Parses the header byte and string dictionary from the raw metadata
/// buffer. The returned VariantMetadata contains string_views that
/// reference the input buffer directly (zero-copy).
///
/// \param[in] data Pointer to the metadata buffer (must not be null)
/// \param[in] length Length of the metadata buffer in bytes
/// \return Parsed VariantMetadata on success, Status::Invalid on
///         malformed input
///
/// \note The input buffer must outlive the returned VariantMetadata.
ARROW_EXPORT Result<VariantMetadata> DecodeMetadata(const uint8_t* data, int64_t length);

/// \brief Find the dictionary ID for a given key name.
///
/// Uses binary search if the metadata is sorted, otherwise linear scan.
///
/// \param[in] metadata Parsed metadata
/// \param[in] key The key to search for
/// \return The dictionary ID if found, or -1 if not present
ARROW_EXPORT int32_t FindMetadataKey(const VariantMetadata& metadata,
                                     std::string_view key);

// ---------------------------------------------------------------------------
// Header utilities
// ---------------------------------------------------------------------------

/// \brief Extract the basic type from a value header byte.
inline BasicType GetBasicType(uint8_t header) {
  return static_cast<BasicType>(header & 0x03);
}

/// \brief Extract the primitive type from a value header byte.
/// Only valid when GetBasicType(header) == BasicType::kPrimitive.
inline PrimitiveType GetPrimitiveType(uint8_t header) {
  return static_cast<PrimitiveType>((header >> 2) & 0x3F);
}

/// \brief Get the byte size of a primitive value (excluding header).
/// Returns -1 for variable-length types (Binary, String).
ARROW_EXPORT int32_t PrimitiveValueSize(PrimitiveType primitive_type);

/// \brief Compute the total byte size of a variant value (header + data).
///
/// Determines how many bytes a variant value occupies without full decoding.
/// \param[in] data Pointer to the start of a variant value
/// \param[in] length Maximum bytes available
/// \return Total byte count, or Status::Invalid if truncated
ARROW_EXPORT Result<int64_t> ValueSize(const uint8_t* data, int64_t length);

/// \brief Recursively validate a variant value and all nested children.
///
/// Performs deep structural validation of the entire value tree:
///   - All headers are well-formed
///   - All field IDs reference valid dictionary entries
///   - All offsets are within bounds and monotonically non-decreasing
///   - All nested values are recursively valid
///   - Nesting depth does not exceed kMaxNestingDepth
///
/// Use this for untrusted input where you want a single pass/fail before
/// operating on the data. For trusted data (e.g., builder output), the
/// per-access validation in view classes is sufficient.
///
/// \param[in] metadata Parsed metadata (for validating field ID references)
/// \param[in] data Pointer to the variant value buffer
/// \param[in] length Length of the variant value buffer in bytes
/// \return Status::OK if valid, Status::Invalid with description of first error
ARROW_EXPORT Status ValidateVariant(const VariantMetadata& metadata, const uint8_t* data,
                                    int64_t length);

// ---------------------------------------------------------------------------
// Forward declarations
// ---------------------------------------------------------------------------

class VariantObjectView;
class VariantArrayView;

// ---------------------------------------------------------------------------
// VariantView — non-owning view over a single variant value
// ---------------------------------------------------------------------------

/// \brief A non-owning, zero-copy view over a single variant value.
///
/// Construction validates the header byte is readable. Subsequent typed
/// accessors validate type compatibility and return errors on mismatch.
///
/// Stack-allocated, ~32 bytes. No heap allocation.
///
/// \note The metadata and data buffers must outlive this view.
class ARROW_EXPORT VariantView {
 public:
  /// \brief Construct a view over a variant value.
  ///
  /// Validates that the buffer has at least one byte and computes the
  /// value's total size. Returns Invalid on empty/null buffers or if
  /// the value is truncated.
  ///
  /// \param[in] metadata Parsed metadata (for resolving object field names)
  /// \param[in] data Pointer to the value buffer
  /// \param[in] length Length of the value buffer in bytes
  static Result<VariantView> Make(const VariantMetadata& metadata, const uint8_t* data,
                                  int64_t length);

  /// \brief The basic type of this value.
  BasicType type() const { return type_; }

  /// \brief Whether this value is null (PrimitiveType::kNull).
  bool is_null() const;

  /// \brief Total byte size of this value (header + payload).
  int64_t size_bytes() const { return size_; }

  /// \brief Raw pointer to the value bytes.
  const uint8_t* data() const { return data_; }

  /// @name Primitive accessors
  /// Each returns the value if the type matches, or Status::Invalid.
  /// @{
  Result<bool> as_bool() const;
  Result<int8_t> as_int8() const;
  Result<int16_t> as_int16() const;
  Result<int32_t> as_int32() const;
  Result<int64_t> as_int64() const;
  Result<float> as_float() const;
  Result<double> as_double() const;
  Result<std::string_view> as_string() const;
  Result<std::string_view> as_binary() const;
  Result<int32_t> as_date() const;
  Result<int64_t> as_timestamp_micros() const;
  Result<int64_t> as_timestamp_micros_ntz() const;
  Result<int64_t> as_timestamp_nanos() const;
  Result<int64_t> as_timestamp_nanos_ntz() const;
  Result<int64_t> as_time_ntz() const;
  /// @}

  /// @name Widening numeric accessors (Rust parity)
  /// These coerce narrower integer types to a wider target type.
  /// e.g., as_int64_coerced() succeeds on Int8, Int16, Int32, or Int64 values.
  /// @{

  /// \brief Read any integer variant as int64, widening if necessary.
  /// Succeeds for Int8, Int16, Int32, and Int64 encoded values.
  Result<int64_t> as_int64_coerced() const;

  /// \brief Read any integer variant as int32, widening if necessary.
  /// Succeeds for Int8 and Int16 and Int32 encoded values.
  /// Returns Invalid for Int64 (would narrow).
  Result<int32_t> as_int32_coerced() const;

  /// \brief Read any numeric variant as double, coercing if necessary.
  /// Succeeds for Int8, Int16, Int32, Int64, Float, and Double.
  /// Note: Int64 -> double may lose precision for large values.
  Result<double> as_double_coerced() const;
  /// @}

  /// \brief Access UUID value (16 bytes, big-endian).
  /// Returns pointer to the 16 UUID bytes within the source buffer.
  Result<const uint8_t*> as_uuid() const;

  /// \brief Access Decimal4 (scale + 4 bytes unscaled value).
  /// \param[out] scale Set to the decimal scale
  /// \return Pointer to the 4 unscaled value bytes
  Result<const uint8_t*> as_decimal4(int32_t* scale) const;

  /// \brief Access Decimal8 (scale + 8 bytes unscaled value).
  Result<const uint8_t*> as_decimal8(int32_t* scale) const;

  /// \brief Access Decimal16 (scale + 16 bytes unscaled value).
  Result<const uint8_t*> as_decimal16(int32_t* scale) const;
  /// @}

  /// @name Container accessors
  /// @{

  /// \brief Interpret this value as an object.
  /// Returns Invalid if the basic type is not kObject.
  Result<VariantObjectView> as_object() const;

  /// \brief Interpret this value as an array.
  /// Returns Invalid if the basic type is not kArray.
  Result<VariantArrayView> as_array() const;
  /// @}

  /// @name Visitor traversal
  /// @{

  /// \brief Full recursive traversal via visitor pattern.
  ///
  /// Visits every node in the value tree, calling appropriate visitor
  /// methods. For bulk processing of entire variant values (e.g., JSON
  /// serialization, schema inference).
  ///
  /// \param[in] visitor Callback interface for decoded values
  /// \return Status::OK on success, or first error from visitor/decode
  Status Visit(class VariantVisitor* visitor) const;
  /// @}

 private:
  VariantView(const VariantMetadata* metadata, const uint8_t* data, int64_t size,
              BasicType type);

  const VariantMetadata* metadata_;
  const uint8_t* data_;
  int64_t size_;
  BasicType type_;
};

// ---------------------------------------------------------------------------
// VariantObjectView — pre-parsed object for O(log n) field lookup
// ---------------------------------------------------------------------------

/// \brief A non-owning view over a variant object value with pre-parsed header.
///
/// Construction validates the object header structure (field counts, array
/// bounds). After construction, field lookup is O(log n) via binary search
/// with no redundant parsing.
///
/// Stack-allocated, ~72 bytes. No heap allocation.
///
/// \note The metadata and data buffers must outlive this view.
class ARROW_EXPORT VariantObjectView {
 public:
  /// \brief Construct an object view, pre-parsing the header.
  ///
  /// Validates:
  ///   1. Basic type is kObject
  ///   2. num_fields is readable
  ///   3. Field ID array fits in buffer
  ///   4. Offset array fits in buffer
  ///   5. Last offset (total data size) is within buffer
  ///
  /// After Make() succeeds, all field accessors are bounds-safe.
  static Result<VariantObjectView> Make(const VariantMetadata& metadata,
                                        const uint8_t* data, int64_t length);

  /// \brief Number of fields in this object.
  int32_t num_fields() const { return num_fields_; }

  /// \brief Get the name of the i-th field (0-indexed).
  /// \return The field name, or Invalid if index is out of range or
  ///         field ID references an invalid dictionary entry.
  Result<std::string_view> field_name(int32_t index) const;

  /// \brief Get the value of the i-th field (0-indexed).
  /// \return A VariantView over the field's value.
  Result<VariantView> field_value(int32_t index) const;

  /// \brief Lookup a field by name using binary search.
  ///
  /// Per spec, field IDs are sorted by lexicographic order of their
  /// corresponding key names. This enables O(log n) lookup for all
  /// object sizes.
  ///
  /// \param[in] name The field name to search for
  /// \return The field's value if found, or std::nullopt if not present.
  ///
  /// \note Returns std::nullopt for both "field not found" AND "malformed data"
  ///       (e.g., field ID exceeds dictionary, or field value bytes are truncated).
  ///       For untrusted data where error reporting is needed, use field_name(i)
  ///       and field_value(i) directly which return Result<T>.
  std::optional<VariantView> get(std::string_view name) const;

  /// \brief Check if a field exists by name.
  bool contains(std::string_view name) const;

  /// \brief Location of a field's value bytes within the object buffer.
  struct FieldLocation {
    int64_t offset;  ///< Byte offset from object start to field value
    int64_t size;    ///< Byte size of the field value
  };

  /// \brief Locate a field's raw bytes by name without constructing a view.
  ///
  /// Useful for zero-copy extraction when you need the offset+size for
  /// raw byte operations (e.g., shredding's UnsafeAppendEncoded).
  ///
  /// \param[in] name The field name to search for
  /// \return The field location, or std::nullopt if not present.
  std::optional<FieldLocation> locate(std::string_view name) const;

  /// @name Iteration support
  /// @{

  /// \brief Iterator over (name, value) pairs.
  ///
  /// \warning Iteration uses ValueOrDie() internally. If the object contains
  ///          malformed field data (corrupt value bytes), dereferencing the
  ///          iterator will abort. Use field_name(i)/field_value(i) for
  ///          error-safe access on untrusted data.
  class Iterator {
   public:
    using value_type = std::pair<std::string_view, VariantView>;

    Iterator(const VariantObjectView* obj, int32_t index);

    value_type operator*() const;
    Iterator& operator++();
    bool operator!=(const Iterator& other) const;

   private:
    const VariantObjectView* obj_;
    int32_t index_;
  };

  Iterator begin() const;
  Iterator end() const;
  /// @}

 private:
  VariantObjectView(const VariantMetadata* metadata, const uint8_t* data, int64_t length,
                    int32_t num_fields, int8_t field_id_size, int8_t field_offset_size,
                    int64_t id_start, int64_t offset_start, int64_t data_start);

  /// \brief Read the field ID at position i.
  uint32_t field_id_at(int32_t i) const;

  /// \brief Read the value offset at position i.
  int64_t value_offset_at(int32_t i) const;

  const VariantMetadata* metadata_;
  const uint8_t* data_;
  int64_t length_;
  int32_t num_fields_;
  int8_t field_id_size_;
  int8_t field_offset_size_;
  int64_t id_start_;
  int64_t offset_start_;
  int64_t data_start_;
};

// ---------------------------------------------------------------------------
// VariantArrayView — pre-parsed array for O(1) element access
// ---------------------------------------------------------------------------

/// \brief A non-owning view over a variant array value with pre-parsed header.
///
/// Construction validates the array header structure. After construction,
/// element access is O(1) via the pre-computed offset table.
///
/// Stack-allocated, ~56 bytes. No heap allocation.
class ARROW_EXPORT VariantArrayView {
 public:
  /// \brief Construct an array view, pre-parsing the header.
  ///
  /// Validates:
  ///   1. Basic type is kArray
  ///   2. num_elements is readable
  ///   3. Offset array fits in buffer
  ///   4. Offsets are monotonically non-decreasing
  ///   5. Last offset is within buffer
  static Result<VariantArrayView> Make(const VariantMetadata& metadata,
                                       const uint8_t* data, int64_t length);

  /// \brief Number of elements in this array.
  int32_t num_elements() const { return num_elements_; }

  /// \brief Get the i-th element (0-indexed, O(1) access).
  /// \return A VariantView over the element, or Invalid if index is out of range.
  Result<VariantView> get(int32_t index) const;

  /// @name Iteration support
  /// @{

  /// \warning Iteration uses ValueOrDie() internally. If the array contains
  ///          malformed element data, dereferencing the iterator will abort.
  ///          Use get(i) for error-safe access on untrusted data.
  class Iterator {
   public:
    using value_type = VariantView;

    Iterator(const VariantArrayView* arr, int32_t index);

    value_type operator*() const;
    Iterator& operator++();
    bool operator!=(const Iterator& other) const;

   private:
    const VariantArrayView* arr_;
    int32_t index_;
  };

  Iterator begin() const;
  Iterator end() const;
  /// @}

 private:
  VariantArrayView(const VariantMetadata* metadata, const uint8_t* data, int64_t length,
                   int32_t num_elements, int8_t offset_size, int64_t offset_start,
                   int64_t data_start);

  /// \brief Read the element offset at position i.
  int64_t element_offset_at(int32_t i) const;

  const VariantMetadata* metadata_;
  const uint8_t* data_;
  int64_t length_;
  int32_t num_elements_;
  int8_t offset_size_;
  int64_t offset_start_;
  int64_t data_start_;
};

// ---------------------------------------------------------------------------
// Visitor interface (for full tree traversal)
// ---------------------------------------------------------------------------

/// \brief Visitor interface for variant value traversal (SAX-style).
///
/// Implement this interface to receive callbacks during recursive variant
/// value traversal. Use VariantView::Visit() to drive the traversal.
///
/// For point-queries (reading a specific field), use the view classes
/// directly instead. The visitor is for bulk operations that need to
/// process every node in the tree.
///
/// \note String values are raw bytes without UTF-8 validation.
class ARROW_EXPORT VariantVisitor {
 public:
  virtual ~VariantVisitor() = default;

  /// @name Primitive value callbacks
  /// @{
  virtual Status Null() = 0;
  virtual Status Bool(bool value) = 0;
  virtual Status Int8(int8_t value) = 0;
  virtual Status Int16(int16_t value) = 0;
  virtual Status Int32(int32_t value) = 0;
  virtual Status Int64(int64_t value) = 0;
  virtual Status Float(float value) = 0;
  virtual Status Double(double value) = 0;
  virtual Status Decimal4(const uint8_t* bytes, int32_t scale) = 0;
  virtual Status Decimal8(const uint8_t* bytes, int32_t scale) = 0;
  virtual Status Decimal16(const uint8_t* bytes, int32_t scale) = 0;
  virtual Status Date(int32_t days_since_epoch) = 0;
  virtual Status TimestampMicros(int64_t micros_since_epoch) = 0;
  virtual Status TimestampMicrosNTZ(int64_t micros_since_epoch) = 0;
  virtual Status String(std::string_view value) = 0;
  virtual Status Binary(std::string_view value) = 0;
  virtual Status TimeNTZ(int64_t micros_since_midnight) = 0;
  virtual Status TimestampNanos(int64_t nanos_since_epoch) = 0;
  virtual Status TimestampNanosNTZ(int64_t nanos_since_epoch) = 0;
  virtual Status UUID(const uint8_t* bytes) = 0;
  /// @}

  /// @name Container callbacks
  /// @{
  virtual Status StartObject(int32_t num_fields) = 0;
  virtual Status FieldName(std::string_view name) = 0;
  virtual Status EndObject() = 0;
  virtual Status StartArray(int32_t num_elements) = 0;
  virtual Status EndArray() = 0;
  /// @}
};

}  // namespace arrow::extension::variant
