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

// This API is EXPERIMENTAL.

#pragma once

#include <vector>

#include "arrow/buffer.h"
#include "arrow/engine/visibility.h"
#include "arrow/type_fwd.h"
#include "arrow/util/optional.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace engine {

// arrow::ExtensionTypes are provided to wrap uuid, fixed_char, varchar, interval_year,
// and interval_day which are first-class types in substrait but do not appear in
// the arrow type system.
//
// Note that these are not automatically registered with arrow::RegisterExtensionType(),
// which means among other things that serialization of these types to IPC would fail.
//
// FIXME we'll also need arrow::ExtensionTypes to wrap substrait::ExtensionTypeVariations
// and substrait::ExtensionTypes.

/// fixed_size_binary(16) for storing Universally Unique IDentifiers
ARROW_ENGINE_EXPORT
std::shared_ptr<DataType> uuid();

/// fixed_size_binary(length) constrained to contain only valid UTF-8
ARROW_ENGINE_EXPORT
std::shared_ptr<DataType> fixed_char(int32_t length);

/// utf8() constrained to be shorter than `length`
ARROW_ENGINE_EXPORT
std::shared_ptr<DataType> varchar(int32_t length);

/// fixed_size_list(int32(), 2) storing a number of [years, months]
ARROW_ENGINE_EXPORT
std::shared_ptr<DataType> interval_year();

/// fixed_size_list(int32(), 2) storing a number of [days, seconds]
ARROW_ENGINE_EXPORT
std::shared_ptr<DataType> interval_day();

/// Return true if t is Uuid, otherwise false
ARROW_ENGINE_EXPORT
bool UnwrapUuid(const DataType&);

/// Return FixedChar length if t is FixedChar, otherwise nullopt
ARROW_ENGINE_EXPORT
util::optional<int32_t> UnwrapFixedChar(const DataType&);

/// Return Varchar (max) length if t is VarChar, otherwise nullopt
ARROW_ENGINE_EXPORT
util::optional<int32_t> UnwrapVarChar(const DataType& t);

/// Return true if t is IntervalYear, otherwise false
ARROW_ENGINE_EXPORT
bool UnwrapIntervalYear(const DataType&);

/// Return true if t is IntervalDay, otherwise false
ARROW_ENGINE_EXPORT
bool UnwrapIntervalDay(const DataType&);

// We need to be able to append to ExtensionSets as we serialize-
//   we first serialize Plan.relations, then finalize by hydrating
//   Plan.extensions etc with the accumulated variations etc
//
// We need to be able to draw from ExtensionSets as we deserialize-
//   type variation references in protobuf are just ints and we need to
//   be able to look them up and efficiently hydrate with the corresponding
//   DataType.

/// A mapping from arrow types and functions to the (uri, name) which identifies
/// the corresponding substrait extension.
class ARROW_ENGINE_EXPORT ExtensionIdRegistry {
 public:
  struct Id {
    util::string_view uri, name;

    bool empty() const { return uri.empty() && name.empty(); }
  };

  struct TypeRecord {
    Id id;
    const std::shared_ptr<DataType>& type;
    bool is_variation;
  };
  virtual util::optional<TypeRecord> GetType(const DataType&) const = 0;
  virtual util::optional<TypeRecord> GetType(Id, bool is_variation) const = 0;
  virtual Status RegisterType(Id, std::shared_ptr<DataType>, bool is_variation) = 0;
};

constexpr util::string_view kArrowExtTypesUri =
    "https://github.com/apache/arrow/blob/master/format/substrait/"
    "extension_types.yaml";

ARROW_ENGINE_EXPORT ExtensionIdRegistry* default_extension_id_registry();

/// A subset of an ExtensionIdRegistry with extensions identifiable by an integer.
class ARROW_ENGINE_EXPORT ExtensionSet {
 public:
  using Id = ExtensionIdRegistry::Id;

  /// Construct an empty ExtensionSet to be populated during serialization.
  explicit ExtensionSet(ExtensionIdRegistry* = default_extension_id_registry());
  ARROW_DEFAULT_MOVE_AND_ASSIGN(ExtensionSet);

  /// Construct an ExtensionSet with explicit extension ids for efficient referencing
  /// during deserialization. Note that input vectors need not be densely packed; an empty
  /// (default constructed) Id may be used as a placeholder to indicate an unused
  /// _anchor/_reference. This factory will be used to wrap the extensions declared in a
  /// substrait::Plan before deserializing the plan's relations.
  static Result<ExtensionSet> Make(
      std::vector<std::string> uris, std::vector<Id> type_ids,
      std::vector<bool> type_is_variation,
      ExtensionIdRegistry* = default_extension_id_registry());

  ~ExtensionSet();

  // index in these vectors == value of _anchor/_reference fields
  /// FIXME this assumes that _anchor/_references won't be huge, which is not guaranteed.
  /// Could it be?
  const std::vector<std::string>& uris() const { return uris_; }

  const DataTypeVector& types() const { return types_; }
  const std::vector<Id>& type_ids() const { return type_ids_; }
  bool type_is_variation(uint32_t i) const { return type_is_variation_[i]; }

  /// Encode a type as a substrait::ExtensionType or a substrait::ExtensionTypeVariation.
  /// Returns an integer usable for Type.user_defined_type_reference. This method will be
  /// invoked during serialization when non substrait-core types are encountered.
  uint32_t EncodeType(Id, const std::shared_ptr<DataType>&, bool is_variation);

  /// Encode a type, looking it up first in this set's ExtensionIdRegistry.
  /// If no type is found, an error will be raised.
  Result<uint32_t> EncodeType(const DataType& type);

  // FIXME need type_internal.h::{AddExtensionSetToPlan, GetExtensionSetFromPlan} or so

 private:
  ExtensionIdRegistry* registry_;
  DataTypeVector types_;
  std::vector<Id> type_ids_;
  std::vector<std::string> uris_;
  std::vector<bool> type_is_variation_;

  // pimpl pattern to hide lookup details
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace engine
}  // namespace arrow
