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

#include "arrow/extension_type.h"
#include "arrow/type.h"

namespace arrow::extension {

/// \brief Which bound(s) of an arrow.range interval are inclusive.
///
/// Null (infinite) bounds are always exclusive regardless of this value.
enum class RangeClosed {
  /// Lower bound is inclusive, upper bound is exclusive: [lower, upper)
  Left,
  /// Lower bound is exclusive, upper bound is inclusive: (lower, upper]
  Right,
  /// Both bounds are inclusive: [lower, upper]
  Both,
  /// Both bounds are exclusive: (lower, upper)
  Neither,
};

/// \brief RangeType represents a bounded set (mathematical interval) over an
/// orderable Arrow type T.
///
/// Storage is a Struct with exactly two fields "lower" and "upper" of the same
/// orderable type T. Each field may independently be nullable or not: a nullable
/// bound can hold null to represent an unbounded (infinite) endpoint on that
/// side, while a non-nullable bound is always finite.
///   - "lower": T  (null, when nullable = unbounded below, i.e. -infinity)
///   - "upper": T  (null, when nullable = unbounded above, i.e. +infinity)
///
/// The outer struct's validity bit marks a null/absent range.
///
/// The "closed" parameter controls which finite bounds are inclusive.
/// Null (infinite) bounds are always treated as exclusive.
class ARROW_EXPORT RangeType : public ExtensionType {
 public:
  /// \brief Construct a RangeType.
  ///
  /// \param[in] storage_type A two-field Struct type with nullable fields
  ///   "lower" and "upper" of the same orderable Arrow type T.
  /// \param[in] closed Which bound(s) are inclusive.
  explicit RangeType(std::shared_ptr<DataType> storage_type, RangeClosed closed)
      : ExtensionType(std::move(storage_type)), closed_(closed) {}

  std::string extension_name() const override { return "arrow.range"; }
  std::string ToString(bool show_metadata = false) const override;
  bool ExtensionEquals(const ExtensionType& other) const override;
  std::string Serialize() const override;
  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized_data) const override;

  /// \brief Create a RangeArray from ArrayData.
  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override;

  /// \brief Factory function.
  ///
  /// Constructs the two-field struct storage type internally.
  /// \param[in] value_type The orderable Arrow subtype T for lower and upper.
  /// \param[in] closed Which bound(s) are inclusive.
  /// \param[in] allow_unbounded Whether each side may be unbounded (infinite).
  ///   When true, the "lower" and "upper" fields are nullable and a null bound
  ///   denotes an infinite endpoint; when false, both bounds are non-nullable
  ///   and the range is always finite. Defaults to true.
  static Result<std::shared_ptr<DataType>> Make(std::shared_ptr<DataType> value_type,
                                                RangeClosed closed = RangeClosed::Left,
                                                bool allow_unbounded = true);

  /// \brief Return the bound-inclusivity parameter.
  RangeClosed closed() const { return closed_; }

  /// \brief Return the Arrow subtype T (the type of "lower" and "upper" fields).
  std::shared_ptr<DataType> value_type() const;

 private:
  RangeClosed closed_;
};

/// \brief Array class for arrow.range extension arrays.
class ARROW_EXPORT RangeArray : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

/// \brief Create a RangeType with the given value subtype and closed parameter.
///
/// This is a convenience wrapper around RangeType::Make that aborts on error.
/// For recoverable error handling prefer RangeType::Make.
ARROW_EXPORT std::shared_ptr<DataType> range(std::shared_ptr<DataType> value_type,
                                             RangeClosed closed = RangeClosed::Left,
                                             bool allow_unbounded = true);

/// \brief RangeIncType represents a bounded set (mathematical interval) over an
/// orderable Arrow type T whose bound inclusivity is stored **per value**.
///
/// Unlike RangeType, which carries a single type-level "closed" parameter, this
/// type records the inclusivity of each row's bounds in two boolean storage
/// fields. This is required for continuous ranges (e.g. PostgreSQL's
/// ``numrange``, ``tsrange``, ``tstzrange``) which cannot be canonicalized to a
/// uniform closedness. It mirrors PostgreSQL's internal range representation.
///
/// Storage is a Struct with exactly four fields, in order:
///   - "lower": T     (null, when nullable = unbounded below, i.e. -infinity)
///   - "upper": T     (null, when nullable = unbounded above, i.e. +infinity)
///   - "lower_inc": boolean, non-nullable: is the lower bound inclusive?
///   - "upper_inc": boolean, non-nullable: is the upper bound inclusive?
///
/// "lower" and "upper" share the same orderable type T and may independently be
/// nullable to represent an unbounded (infinite) endpoint. A null (infinite)
/// bound is always treated as exclusive, regardless of its "*_inc" flag.
///
/// The outer struct's validity bit marks a null/absent range.
///
/// There is no type-level "closed" parameter, so the extension metadata carries
/// no parameters (serialized as the empty JSON object ``{}``).
class ARROW_EXPORT RangeIncType : public ExtensionType {
 public:
  /// \brief Construct a RangeIncType.
  ///
  /// \param[in] storage_type A four-field Struct type with fields "lower",
  ///   "upper" (same orderable type T) and non-nullable boolean "lower_inc",
  ///   "upper_inc".
  explicit RangeIncType(std::shared_ptr<DataType> storage_type)
      : ExtensionType(std::move(storage_type)) {}

  std::string extension_name() const override { return "arrow.range_inc"; }
  std::string ToString(bool show_metadata = false) const override;
  bool ExtensionEquals(const ExtensionType& other) const override;
  std::string Serialize() const override;
  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized_data) const override;

  /// \brief Create a RangeIncArray from ArrayData.
  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override;

  /// \brief Factory function.
  ///
  /// Constructs the four-field struct storage type internally.
  /// \param[in] value_type The orderable Arrow subtype T for lower and upper.
  /// \param[in] allow_unbounded Whether each side may be unbounded (infinite).
  ///   When true, the "lower" and "upper" fields are nullable and a null bound
  ///   denotes an infinite endpoint; when false, both bounds are non-nullable
  ///   and the range is always finite. The "lower_inc" and "upper_inc" fields
  ///   are always non-nullable. Defaults to true.
  static Result<std::shared_ptr<DataType>> Make(std::shared_ptr<DataType> value_type,
                                                bool allow_unbounded = true);

  /// \brief Return the Arrow subtype T (the type of "lower" and "upper" fields).
  std::shared_ptr<DataType> value_type() const;
};

/// \brief Array class for arrow.range_inc extension arrays.
class ARROW_EXPORT RangeIncArray : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

/// \brief Create a RangeIncType with the given value subtype.
///
/// This is a convenience wrapper around RangeIncType::Make that aborts on error.
/// For recoverable error handling prefer RangeIncType::Make.
ARROW_EXPORT std::shared_ptr<DataType> range_inc(std::shared_ptr<DataType> value_type,
                                                 bool allow_unbounded = true);

}  // namespace arrow::extension
