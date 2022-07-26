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

#include <unordered_map>
#include <vector>

#include "arrow/engine/substrait/visibility.h"
#include "arrow/type_fwd.h"
#include "arrow/util/optional.h"
#include "arrow/util/string_view.h"

#include "arrow/util/hash_util.h"

namespace arrow {
namespace engine {

/// Substrait identifies functions and custom data types using a (uri, name) pair.
///
/// This registry is a bidirectional mapping between Substrait IDs and their corresponding
/// Arrow counterparts (arrow::DataType and function names in a function registry)
///
/// Substrait extension types and variations must be registered with their corresponding
/// arrow::DataType before they can be used!
///
/// Conceptually this can be thought of as two pairs of `unordered_map`s.  One pair to
/// go back and forth between Substrait ID and arrow::DataType and another pair to go
/// back and forth between Substrait ID and Arrow function names.
///
/// Unlike an ExtensionSet this registry is not created automatically when consuming
/// Substrait plans and must be configured ahead of time (although there is a default
/// instance).
class ARROW_ENGINE_EXPORT ExtensionIdRegistry {
 public:
  /// All uris registered in this ExtensionIdRegistry
  virtual std::vector<util::string_view> Uris() const = 0;

  struct Id {
    util::string_view uri, name;

    bool empty() const { return uri.empty() && name.empty(); }
  };

  struct IdHashEq {
    size_t operator()(Id id) const;
    bool operator()(Id l, Id r) const;
  };

  /// \brief A mapping between a Substrait ID and an arrow::DataType
  struct TypeRecord {
    Id id;
    const std::shared_ptr<DataType>& type;
  };
  virtual util::optional<TypeRecord> GetType(const DataType&) const = 0;
  virtual util::optional<TypeRecord> GetType(Id) const = 0;
  virtual Status CanRegisterType(Id, const std::shared_ptr<DataType>& type) const = 0;
  virtual Status RegisterType(Id, std::shared_ptr<DataType>) = 0;

  /// \brief A mapping between a Substrait ID and an Arrow function
  ///
  /// Note: At the moment we identify functions solely by the name
  /// of the function in the function registry.
  ///
  /// TODO(ARROW-15582) some functions will not be simple enough to convert without access
  /// to their arguments/options. For example is_in embeds the set in options rather than
  /// using an argument:
  ///     is_in(x, SetLookupOptions(set)) <-> (k...Uri, "is_in")(x, set)
  ///
  /// ... for another example, depending on the value of the first argument to
  /// substrait::add it either corresponds to arrow::add or arrow::add_checked
  struct FunctionRecord {
    Id id;
    const std::string& function_name;
  };
  virtual util::optional<FunctionRecord> GetFunction(Id) const = 0;
  virtual util::optional<FunctionRecord> GetFunction(
      util::string_view arrow_function_name) const = 0;
  virtual Status CanRegisterFunction(Id,
                                     const std::string& arrow_function_name) const = 0;
  // registers a function without taking ownership of uri and name within Id
  virtual Status RegisterFunction(Id, std::string arrow_function_name) = 0;
  // registers a function while taking ownership of uri and name
  virtual Status RegisterFunction(std::string uri, std::string name,
                                  std::string arrow_function_name) = 0;
};

constexpr util::string_view kArrowExtTypesUri =
    "https://github.com/apache/arrow/blob/master/format/substrait/"
    "extension_types.yaml";

/// A default registry with all supported functions and data types registered
///
/// Note: Function support is currently very minimal, see ARROW-15538
ARROW_ENGINE_EXPORT ExtensionIdRegistry* default_extension_id_registry();

/// \brief Make a nested registry with a given parent.
///
/// A nested registry supports registering types and functions other and on top of those
/// already registered in its parent registry. No conflicts in IDs and names used for
/// lookup are allowed. Normally, the given parent is the default registry.
///
/// One use case for a nested registry is for dynamic registration of functions defined
/// within a Substrait plan while keeping these registrations specific to the plan. When
/// the Substrait plan is disposed of, normally after its execution, the nested registry
/// can be disposed of as well.
ARROW_ENGINE_EXPORT std::shared_ptr<ExtensionIdRegistry> nested_extension_id_registry(
    const ExtensionIdRegistry* parent);

/// \brief A set of extensions used within a plan
///
/// Each time an extension is used within a Substrait plan the extension
/// must be included in an extension set that is defined at the root of the
/// plan.
///
/// The plan refers to a specific extension using an "anchor" which is an
/// arbitrary integer invented by the producer that has no meaning beyond a
/// plan but which should be consistent within a plan.
///
/// To support serialization and deserialization this type serves as a
/// bidirectional map between Substrait ID and "anchor"s.
///
/// When deserializing a Substrait plan the extension set should be extracted
/// after the plan has been converted from Protobuf and before the plan
/// is converted to an execution plan.
///
/// The extension set can be kept and reused during serialization if a perfect
/// round trip is required.  If serialization is not needed or round tripping
/// is not required then the extension set can be safely discarded after the
/// plan has been converted into an execution plan.
///
/// When converting an execution plan into a Substrait plan an extension set
/// can be automatically generated or a previously generated extension set can
/// be used.
///
/// ExtensionSet does not own strings; it only refers to strings in an
/// ExtensionIdRegistry.
class ARROW_ENGINE_EXPORT ExtensionSet {
 public:
  using Id = ExtensionIdRegistry::Id;
  using IdHashEq = ExtensionIdRegistry::IdHashEq;

  struct FunctionRecord {
    Id id;
    util::string_view name;
  };

  struct TypeRecord {
    Id id;
    std::shared_ptr<DataType> type;
  };

  /// Construct an empty ExtensionSet to be populated during serialization.
  explicit ExtensionSet(const ExtensionIdRegistry* = default_extension_id_registry());
  ARROW_DEFAULT_MOVE_AND_ASSIGN(ExtensionSet);

  /// Construct an ExtensionSet with explicit extension ids for efficient referencing
  /// during deserialization. Note that input vectors need not be densely packed; an empty
  /// (default constructed) Id may be used as a placeholder to indicate an unused
  /// _anchor/_reference. This factory will be used to wrap the extensions declared in a
  /// substrait::Plan before deserializing the plan's relations.
  ///
  /// Views will be replaced with equivalent views pointing to memory owned by the
  /// registry.
  ///
  /// Note: This is an advanced operation.  The order of the ids, types, and functions
  /// must match the anchor numbers chosen for a plan.
  ///
  /// An extension set should instead be created using
  /// arrow::engine::GetExtensionSetFromPlan
  static Result<ExtensionSet> Make(
      std::unordered_map<uint32_t, util::string_view> uris,
      std::unordered_map<uint32_t, Id> type_ids,
      std::unordered_map<uint32_t, Id> function_ids,
      const ExtensionIdRegistry* = default_extension_id_registry());

  const std::unordered_map<uint32_t, util::string_view>& uris() const { return uris_; }

  /// \brief Returns a data type given an anchor
  ///
  /// This is used when converting a Substrait plan to an Arrow execution plan.
  ///
  /// If the anchor does not exist in this extension set an error will be returned.
  Result<TypeRecord> DecodeType(uint32_t anchor) const;

  /// \brief Returns the number of custom type records in this extension set
  ///
  /// Note: the types are currently stored as a sparse vector, so this may return a value
  /// larger than the actual number of types. This behavior may change in the future; see
  /// ARROW-15583.
  std::size_t num_types() const { return types_.size(); }

  /// \brief Lookup the anchor for a given type
  ///
  /// This operation is used when converting an Arrow execution plan to a Substrait plan.
  /// If the type has been previously encoded then the same anchor value will returned.
  ///
  /// If the type has not been previously encoded then a new anchor value will be created.
  ///
  /// If the type does not exist in the extension id registry then an error will be
  /// returned.
  ///
  /// \return An anchor that can be used to refer to the type within a plan
  Result<uint32_t> EncodeType(const DataType& type);

  /// \brief Returns a function given an anchor
  ///
  /// This is used when converting a Substrait plan to an Arrow execution plan.
  ///
  /// If the anchor does not exist in this extension set an error will be returned.
  Result<FunctionRecord> DecodeFunction(uint32_t anchor) const;

  /// \brief Lookup the anchor for a given function
  ///
  /// This operation is used when converting an Arrow execution plan to a Substrait  plan.
  /// If the function has been previously encoded then the same anchor value will be
  /// returned.
  ///
  /// If the function has not been previously encoded then a new anchor value will be
  /// created.
  ///
  /// If the function name is not in the extension id registry then an error will be
  /// returned.
  ///
  /// \return An anchor that can be used to refer to the function within a plan
  Result<uint32_t> EncodeFunction(util::string_view function_name);

  /// \brief Returns the number of custom functions in this extension set
  ///
  /// Note: the functions are currently stored as a sparse vector, so this may return a
  /// value larger than the actual number of functions. This behavior may change in the
  /// future; see ARROW-15583.
  std::size_t num_functions() const { return functions_.size(); }

 private:
  const ExtensionIdRegistry* registry_;

  // Map from anchor values to URI values referenced by this extension set
  std::unordered_map<uint32_t, util::string_view> uris_;
  // Map from anchor values to type definitions, used during Substrait->Arrow
  // and populated from the Substrait extension set
  std::unordered_map<uint32_t, TypeRecord> types_;
  // Map from anchor values to function definitions, used during Substrait->Arrow
  // and populated from the Substrait extension set
  std::unordered_map<uint32_t, FunctionRecord> functions_;
  // Map from type names to anchor values.  Used during Arrow->Substrait
  // and built as the plan is created.
  std::unordered_map<Id, uint32_t, IdHashEq, IdHashEq> types_map_;
  // Map from function names to anchor values.  Used during Arrow->Substrait
  // and built as the plan is created.
  std::unordered_map<Id, uint32_t, IdHashEq, IdHashEq> functions_map_;

  Status CheckHasUri(util::string_view uri);
  void AddUri(std::pair<uint32_t, util::string_view> uri);
  Status AddUri(Id id);
};

}  // namespace engine
}  // namespace arrow
