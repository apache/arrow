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

    bool operator==(Id other) const { return uri == other.uri && name == other.name; }
    bool operator!=(Id other) const { return !(*this == other); }
  };

  virtual util::optional<Id> GetTypeVariation(const std::shared_ptr<DataType>&) const = 0;
  virtual std::shared_ptr<DataType> GetTypeVariation(Id) const = 0;
  virtual Status RegisterTypeVariation(Id, const std::shared_ptr<DataType>&) = 0;

  virtual util::optional<Id> GetType(const std::shared_ptr<DataType>&) const = 0;
  virtual std::shared_ptr<DataType> GetType(Id) const = 0;
  virtual Status RegisterType(Id, const std::shared_ptr<DataType>&) = 0;
};

ARROW_ENGINE_EXPORT ExtensionIdRegistry* default_extension_id_registry();

/// A subset of an ExtensionIdRegistry with extensions identifiable by an integer.
class ARROW_ENGINE_EXPORT ExtensionSet {
 public:
  using Id = ExtensionIdRegistry::Id;

  /// Construct an empty ExtensionSet to be populated during serialization.
  ExtensionSet();
  ARROW_DEFAULT_MOVE_AND_ASSIGN(ExtensionSet);

  /// Construct an ExtensionSet with explicit extension ids for efficient referencing
  /// during deserialization. Note that input vectors need not be densely packed; an empty
  /// (default constructed) Id may be used as a placeholder to indicate an unused
  /// _anchor/_reference.
  static Result<ExtensionSet> Make(
      std::vector<util::string_view> uris, std::vector<Id> type_variation_ids,
      std::vector<Id> type_ids, ExtensionIdRegistry* = default_extension_id_registry());

  ~ExtensionSet();

  // index in these vectors == value of _anchor/_reference fields
  /// FIXME this assumes that _anchor/_references won't be huge, which is not guaranteed.
  /// Could it be?
  const std::vector<util::string_view>& uris() const { return uris_; }

  const DataTypeVector& type_variations() const { return type_variations_; }
  const std::vector<Id>& type_variation_ids() const { return type_variation_ids_; }

  const DataTypeVector& types() const { return types_; }
  const std::vector<Id>& type_ids() const { return type_ids_; }

  /// Encode a type as a substrait::ExtensionTypeVariation.
  /// Returns an integer usable for Type.*.type_variation_reference.
  uint32_t EncodeTypeVariation(Id, const std::shared_ptr<DataType>&);

  /// Encode a type as a substrait::ExtensionType.
  /// Returns an integer usable for Type.user_defined_type_reference.
  uint32_t EncodeType(Id, const std::shared_ptr<DataType>&);

 private:
  DataTypeVector type_variations_, types_;
  std::vector<ExtensionIdRegistry::Id> type_variation_ids_, type_ids_;
  std::vector<util::string_view> uris_;

  // pimpl pattern to hide lookup details
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace engine
}  // namespace arrow
