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

#include "arrow/engine/substrait/extension_set.h"

#include <algorithm>
#include <iterator>
#include <list>
#include <memory>
#include <sstream>
#include <unordered_set>

#include "arrow/compute/api_scalar.h"
#include "arrow/engine/substrait/options.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/hash_util.h"
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"

namespace arrow {
namespace engine {
namespace {

struct TypePtrHashEq {
  template <typename Ptr>
  size_t operator()(const Ptr& type) const {
    return type->Hash();
  }

  template <typename Ptr>
  bool operator()(const Ptr& l, const Ptr& r) const {
    return *l == *r;
  }
};

}  // namespace

std::string Id::ToString() const {
  std::stringstream sstream;
  sstream << uri;
  sstream << '#';
  sstream << name;
  return sstream.str();
}

size_t IdHashEq::operator()(Id id) const {
  constexpr ::arrow::internal::StringViewHash hash = {};
  auto out = static_cast<size_t>(hash(id.uri));
  ::arrow::internal::hash_combine(out, hash(id.name));
  return out;
}

bool IdHashEq::operator()(Id l, Id r) const { return l.uri == r.uri && l.name == r.name; }

class IdStorageImpl : public IdStorage {
 public:
  Id Emplace(Id id) override {
    std::string_view owned_uri = EmplaceUri(id.uri);

    std::string_view owned_name;
    auto name_itr = names_.find(id.name);
    if (name_itr == names_.end()) {
      owned_names_.emplace_back(id.name);
      owned_name = owned_names_.back();
      names_.insert(owned_name);
    } else {
      owned_name = *name_itr;
    }

    return {owned_uri, owned_name};
  }

  std::optional<Id> Find(Id id) const override {
    std::optional<std::string_view> maybe_owned_uri = FindUri(id.uri);
    if (!maybe_owned_uri) {
      return std::nullopt;
    }

    auto name_itr = names_.find(id.name);
    if (name_itr == names_.end()) {
      return std::nullopt;
    } else {
      return Id{*maybe_owned_uri, *name_itr};
    }
  }

  std::optional<std::string_view> FindUri(std::string_view uri) const override {
    auto uri_itr = uris_.find(uri);
    if (uri_itr == uris_.end()) {
      return std::nullopt;
    }
    return *uri_itr;
  }

  std::string_view EmplaceUri(std::string_view uri) override {
    auto uri_itr = uris_.find(uri);
    if (uri_itr == uris_.end()) {
      owned_uris_.emplace_back(uri);
      std::string_view owned_uri = owned_uris_.back();
      uris_.insert(owned_uri);
      return owned_uri;
    }
    return *uri_itr;
  }

 private:
  std::unordered_set<std::string_view, ::arrow::internal::StringViewHash> uris_;
  std::unordered_set<std::string_view, ::arrow::internal::StringViewHash> names_;
  std::list<std::string> owned_uris_;
  std::list<std::string> owned_names_;
};

std::unique_ptr<IdStorage> IdStorage::Make() { return std::make_unique<IdStorageImpl>(); }

Result<std::string_view> SubstraitCall::GetEnumArg(int index) const {
  if (index >= size_) {
    return Status::Invalid("Expected Substrait call to have an enum argument at index ",
                           index, " but it did not have enough arguments");
  }
  auto enum_arg_it = enum_args_.find(index);
  if (enum_arg_it == enum_args_.end()) {
    return Status::Invalid("Expected Substrait call to have an enum argument at index ",
                           index, " but the argument was not an enum.");
  }
  return enum_arg_it->second;
}

bool SubstraitCall::HasEnumArg(int index) const {
  return enum_args_.find(index) != enum_args_.end();
}

void SubstraitCall::SetEnumArg(int index, std::string enum_arg) {
  size_ = std::max(size_, index + 1);
  enum_args_[index] = std::move(enum_arg);
}

Result<compute::Expression> SubstraitCall::GetValueArg(int index) const {
  if (index >= size_) {
    return Status::Invalid("Expected Substrait call to have a value argument at index ",
                           index, " but it did not have enough arguments");
  }
  auto value_arg_it = value_args_.find(index);
  if (value_arg_it == value_args_.end()) {
    return Status::Invalid("Expected Substrait call to have a value argument at index ",
                           index, " but the argument was not a value");
  }
  return value_arg_it->second;
}

bool SubstraitCall::HasValueArg(int index) const {
  return value_args_.find(index) != value_args_.end();
}

void SubstraitCall::SetValueArg(int index, compute::Expression value_arg) {
  size_ = std::max(size_, index + 1);
  value_args_[index] = std::move(value_arg);
}

std::optional<std::vector<std::string> const*> SubstraitCall::GetOption(
    std::string_view option_name) const {
  auto opt = options_.find(std::string(option_name));
  if (opt == options_.end()) {
    return std::nullopt;
  }
  return &opt->second;
}

void SubstraitCall::SetOption(std::string_view option_name,
                              const std::vector<std::string_view>& option_preferences) {
  auto& prefs = options_[std::string(option_name)];
  for (std::string_view pref : option_preferences) {
    prefs.emplace_back(pref);
  }
}

bool SubstraitCall::HasOptions() const { return !options_.empty(); }

// A builder used when creating a Substrait plan from an Arrow execution plan.  In
// that situation we do not have a set of anchor values already defined so we keep
// a map of what Ids we have seen.
ExtensionSet::ExtensionSet(const ExtensionIdRegistry* registry) : registry_(registry) {}

Status ExtensionSet::CheckHasUri(std::string_view uri) {
  auto it =
      std::find_if(uris_.begin(), uris_.end(),
                   [&uri](const std::pair<uint32_t, std::string_view>& anchor_uri_pair) {
                     return anchor_uri_pair.second == uri;
                   });
  if (it != uris_.end()) return Status::OK();

  return Status::Invalid(
      "Uri ", uri,
      " was referenced by an extension but was not declared in the ExtensionSet.");
}

void ExtensionSet::AddUri(std::pair<uint32_t, std::string_view> uri) {
  auto it =
      std::find_if(uris_.begin(), uris_.end(),
                   [&uri](const std::pair<uint32_t, std::string_view>& anchor_uri_pair) {
                     return anchor_uri_pair.second == uri.second;
                   });
  if (it != uris_.end()) return;
  uris_[uri.first] = uri.second;
}

Status ExtensionSet::AddUri(Id id) {
  auto uris_size = static_cast<unsigned int>(uris_.size());
  if (uris_.find(uris_size) != uris_.end()) {
    // Substrait plans shouldn't have repeated URIs in the extension set
    return Status::Invalid("Key already exists in the uris map");
  }
  uris_[uris_size] = id.uri;
  return Status::OK();
}

// Creates an extension set from the Substrait plan's top-level extensions block
Result<ExtensionSet> ExtensionSet::Make(
    std::unordered_map<uint32_t, std::string_view> uris,
    std::unordered_map<uint32_t, Id> type_ids,
    std::unordered_map<uint32_t, Id> function_ids,
    const ConversionOptions& conversion_options, const ExtensionIdRegistry* registry) {
  ExtensionSet set(default_extension_id_registry());
  set.registry_ = registry;

  for (auto& uri : uris) {
    std::optional<std::string_view> maybe_uri_internal = registry->FindUri(uri.second);
    if (maybe_uri_internal) {
      set.uris_[uri.first] = *maybe_uri_internal;
    } else {
      if (conversion_options.strictness == ConversionStrictness::EXACT_ROUNDTRIP) {
        return Status::Invalid(
            "Plan contained a URI that the extension registry is unaware of: ",
            uri.second);
      }
      set.uris_[uri.first] = set.plan_specific_ids_->EmplaceUri(uri.second);
    }
  }

  set.types_.reserve(type_ids.size());
  for (const auto& type_id : type_ids) {
    if (type_id.second.empty()) continue;
    RETURN_NOT_OK(set.CheckHasUri(type_id.second.uri));

    if (auto rec = registry->GetType(type_id.second)) {
      set.types_[type_id.first] = {rec->id, rec->type};
      continue;
    }
    return Status::Invalid("Type ", type_id.second.uri, "#", type_id.second.name,
                           " not found");
  }

  set.functions_.reserve(function_ids.size());
  for (const auto& function_id : function_ids) {
    if (function_id.second.empty()) continue;
    RETURN_NOT_OK(set.CheckHasUri(function_id.second.uri));
    std::optional<Id> maybe_id_internal = registry->FindId(function_id.second);
    if (maybe_id_internal) {
      set.functions_[function_id.first] = *maybe_id_internal;
    } else {
      if (conversion_options.strictness == ConversionStrictness::EXACT_ROUNDTRIP) {
        return Status::Invalid(
            "Plan contained a function id that the extension registry is unaware of: ",
            function_id.second.uri, "#", function_id.second.name);
      }
      set.functions_[function_id.first] =
          set.plan_specific_ids_->Emplace(function_id.second);
    }
  }

  return std::move(set);
}

Result<ExtensionSet::TypeRecord> ExtensionSet::DecodeType(uint32_t anchor) const {
  if (types_.find(anchor) == types_.end() || types_.at(anchor).id.empty()) {
    return Status::Invalid("User defined type reference ", anchor,
                           " did not have a corresponding anchor in the extension set");
  }
  return types_.at(anchor);
}

Result<uint32_t> ExtensionSet::EncodeType(const DataType& type) {
  if (auto rec = registry_->GetType(type)) {
    RETURN_NOT_OK(this->AddUri(rec->id));
    auto it_success =
        types_map_.emplace(rec->id, static_cast<uint32_t>(types_map_.size()));
    if (it_success.second) {
      DCHECK_EQ(types_.find(static_cast<uint32_t>(types_.size())), types_.end())
          << "Type existed in types_ but not types_map_.  ExtensionSet is inconsistent";
      types_[static_cast<uint32_t>(types_.size())] = {rec->id, rec->type};
    }
    return it_success.first->second;
  }
  return Status::KeyError("type ", type.ToString(), " not found in the registry");
}

Result<Id> ExtensionSet::DecodeFunction(uint32_t anchor) const {
  if (functions_.find(anchor) == functions_.end() || functions_.at(anchor).empty()) {
    return Status::Invalid("User defined function reference ", anchor,
                           " did not have a corresponding anchor in the extension set");
  }
  return functions_.at(anchor);
}

Result<uint32_t> ExtensionSet::EncodeFunction(Id function_id) {
  RETURN_NOT_OK(this->AddUri(function_id));
  auto it_success =
      functions_map_.emplace(function_id, static_cast<uint32_t>(functions_map_.size()));
  if (it_success.second) {
    DCHECK_EQ(functions_.find(static_cast<uint32_t>(functions_.size())), functions_.end())
        << "Function existed in functions_ but not functions_map_.  ExtensionSet is "
           "inconsistent";
    functions_[static_cast<uint32_t>(functions_.size())] = function_id;
  }
  return it_success.first->second;
}

template <typename KeyToIndex, typename Key>
const int* GetIndex(const KeyToIndex& key_to_index, const Key& key) {
  auto it = key_to_index.find(key);
  if (it == key_to_index.end()) return nullptr;
  return &it->second;
}

namespace {

ExtensionIdRegistry::SubstraitAggregateToArrow DecodeBasicAggregate(
    const std::string& arrow_function_name);

ExtensionIdRegistry::SubstraitCallToArrow kSimpleSubstraitToArrow =
    [](const SubstraitCall& call) -> Result<::arrow::compute::Expression> {
  std::vector<::arrow::compute::Expression> args;
  for (int i = 0; i < call.size(); i++) {
    if (!call.HasValueArg(i)) {
      return Status::Invalid("Simple function mappings can only use value arguments");
    }
    if (call.HasOptions()) {
      return Status::Invalid("Simple function mappings must not specify options");
    }
    ARROW_ASSIGN_OR_RAISE(::arrow::compute::Expression arg, call.GetValueArg(i));
    args.push_back(std::move(arg));
  }
  return ::arrow::compute::call(std::string(call.id().name), std::move(args));
};

ExtensionIdRegistry::SubstraitAggregateToArrow kSimpleSubstraitAggregateToArrow =
    [](const SubstraitCall& call) -> Result<::arrow::compute::Aggregate> {
  return DecodeBasicAggregate(std::string(call.id().name))(call);
};

struct ExtensionIdRegistryImpl : ExtensionIdRegistry {
  ExtensionIdRegistryImpl() : parent_(nullptr) {}
  explicit ExtensionIdRegistryImpl(const ExtensionIdRegistry* parent) : parent_(parent) {}

  virtual ~ExtensionIdRegistryImpl() {}

  std::optional<std::string_view> FindUri(std::string_view uri) const override {
    if (parent_) {
      std::optional<std::string_view> parent_uri = parent_->FindUri(uri);
      if (parent_uri) {
        return parent_uri;
      }
    }
    return ids_->FindUri(uri);
  }

  std::optional<Id> FindId(Id id) const override {
    if (parent_) {
      std::optional<Id> parent_id = parent_->FindId(id);
      if (parent_id) {
        return parent_id;
      }
    }
    return ids_->Find(id);
  }

  std::optional<TypeRecord> GetType(const DataType& type) const override {
    if (auto index = GetIndex(type_to_index_, &type)) {
      return TypeRecord{type_ids_[*index], types_[*index]};
    }
    if (parent_) {
      return parent_->GetType(type);
    }
    return {};
  }

  std::optional<TypeRecord> GetType(Id id) const override {
    if (auto index = GetIndex(id_to_index_, id)) {
      return TypeRecord{type_ids_[*index], types_[*index]};
    }
    if (parent_) {
      return parent_->GetType(id);
    }
    return {};
  }

  Status CanRegisterType(Id id, const std::shared_ptr<DataType>& type) const override {
    if (id_to_index_.find(id) != id_to_index_.end()) {
      return Status::Invalid("Type id was already registered");
    }
    if (type_to_index_.find(&*type) != type_to_index_.end()) {
      return Status::Invalid("Type was already registered");
    }
    if (parent_) {
      return parent_->CanRegisterType(id, type);
    }
    return Status::OK();
  }

  Status RegisterType(Id id, std::shared_ptr<DataType> type) override {
    DCHECK_EQ(type_ids_.size(), types_.size());

    if (parent_) {
      ARROW_RETURN_NOT_OK(parent_->CanRegisterType(id, type));
    }

    Id copied_id = ids_->Emplace(id);

    auto index = static_cast<int>(type_ids_.size());

    auto it_success = id_to_index_.emplace(copied_id, index);

    if (!it_success.second) {
      return Status::Invalid("Type id was already registered");
    }

    if (!type_to_index_.emplace(type.get(), index).second) {
      id_to_index_.erase(it_success.first);
      return Status::Invalid("Type was already registered");
    }

    type_ids_.push_back(copied_id);
    types_.push_back(std::move(type));
    return Status::OK();
  }

  Status CanAddSubstraitCallToArrow(Id substrait_function_id) const override {
    if (substrait_to_arrow_.find(substrait_function_id) != substrait_to_arrow_.end()) {
      return Status::Invalid("Cannot register function converter for Substrait id ",
                             substrait_function_id.ToString(),
                             " because a converter already exists");
    }
    if (parent_) {
      return parent_->CanAddSubstraitCallToArrow(substrait_function_id);
    }
    return Status::OK();
  }

  Status CanAddSubstraitAggregateToArrow(Id substrait_function_id) const override {
    if (substrait_to_arrow_agg_.find(substrait_function_id) !=
        substrait_to_arrow_agg_.end()) {
      return Status::Invalid(
          "Cannot register aggregate function converter for Substrait id ",
          substrait_function_id.ToString(),
          " because an aggregate converter already exists");
    }
    if (parent_) {
      return parent_->CanAddSubstraitAggregateToArrow(substrait_function_id);
    }
    return Status::OK();
  }

  template <typename ConverterType>
  Status AddSubstraitToArrowFunc(
      Id substrait_id, ConverterType conversion_func,
      std::unordered_map<Id, ConverterType, IdHashEq, IdHashEq>* dest) {
    // Convert id to view into registry-owned memory
    Id copied_id = ids_->Emplace(substrait_id);

    auto add_result = dest->emplace(copied_id, std::move(conversion_func));
    if (!add_result.second) {
      return Status::Invalid(
          "Failed to register Substrait to Arrow function converter because a converter "
          "already existed for Substrait id ",
          substrait_id.ToString());
    }

    return Status::OK();
  }

  Status AddSubstraitCallToArrow(Id substrait_function_id,
                                 SubstraitCallToArrow conversion_func) override {
    if (parent_) {
      ARROW_RETURN_NOT_OK(parent_->CanAddSubstraitCallToArrow(substrait_function_id));
    }
    return AddSubstraitToArrowFunc<SubstraitCallToArrow>(
        substrait_function_id, std::move(conversion_func), &substrait_to_arrow_);
  }

  Status AddSubstraitAggregateToArrow(
      Id substrait_function_id, SubstraitAggregateToArrow conversion_func) override {
    if (parent_) {
      ARROW_RETURN_NOT_OK(
          parent_->CanAddSubstraitAggregateToArrow(substrait_function_id));
    }
    return AddSubstraitToArrowFunc<SubstraitAggregateToArrow>(
        substrait_function_id, std::move(conversion_func), &substrait_to_arrow_agg_);
  }

  template <typename ConverterType>
  Status AddArrowToSubstraitFunc(std::string arrow_function_name, ConverterType converter,
                                 std::unordered_map<std::string, ConverterType>* dest) {
    auto add_result = dest->emplace(std::move(arrow_function_name), std::move(converter));
    if (!add_result.second) {
      return Status::Invalid(
          "Failed to register Arrow to Substrait function converter for Arrow function ",
          arrow_function_name, " because a converter already existed");
    }
    return Status::OK();
  }

  Status AddArrowToSubstraitCall(std::string arrow_function_name,
                                 ArrowToSubstraitCall converter) override {
    if (parent_) {
      ARROW_RETURN_NOT_OK(parent_->CanAddArrowToSubstraitCall(arrow_function_name));
    }
    return AddArrowToSubstraitFunc(std::move(arrow_function_name), converter,
                                   &arrow_to_substrait_);
  }

  Status AddArrowToSubstraitAggregate(std::string arrow_function_name,
                                      ArrowToSubstraitAggregate converter) override {
    if (parent_) {
      ARROW_RETURN_NOT_OK(parent_->CanAddArrowToSubstraitAggregate(arrow_function_name));
    }
    return AddArrowToSubstraitFunc(std::move(arrow_function_name), converter,
                                   &arrow_to_substrait_agg_);
  }

  Status CanAddArrowToSubstraitCall(const std::string& function_name) const override {
    if (arrow_to_substrait_.find(function_name) != arrow_to_substrait_.end()) {
      return Status::Invalid(
          "Cannot register function converter because a converter already exists");
    }
    if (parent_) {
      return parent_->CanAddArrowToSubstraitCall(function_name);
    }
    return Status::OK();
  }

  Status CanAddArrowToSubstraitAggregate(
      const std::string& function_name) const override {
    if (arrow_to_substrait_agg_.find(function_name) != arrow_to_substrait_agg_.end()) {
      return Status::Invalid(
          "Cannot register function converter because a converter already exists");
    }
    if (parent_) {
      return parent_->CanAddArrowToSubstraitAggregate(function_name);
    }
    return Status::OK();
  }

  Result<SubstraitCallToArrow> GetSubstraitCallToArrow(
      Id substrait_function_id) const override {
    if (substrait_function_id.uri == kArrowSimpleExtensionFunctionsUri) {
      return kSimpleSubstraitToArrow;
    }
    auto maybe_converter = substrait_to_arrow_.find(substrait_function_id);
    if (maybe_converter == substrait_to_arrow_.end()) {
      if (parent_) {
        return parent_->GetSubstraitCallToArrow(substrait_function_id);
      }
      return Status::NotImplemented(
          "No conversion function exists to convert the Substrait function ",
          substrait_function_id.uri, "#", substrait_function_id.name,
          " to an Arrow call expression");
    }
    return maybe_converter->second;
  }

  Result<SubstraitCallToArrow> GetSubstraitCallToArrowFallback(
      std::string_view function_name) const override {
    for (const auto& converter_item : substrait_to_arrow_) {
      if (converter_item.first.name == function_name) {
        return converter_item.second;
      }
    }
    if (parent_) {
      return parent_->GetSubstraitCallToArrowFallback(function_name);
    }
    return Status::NotImplemented(
        "No conversion function exists to convert the Substrait function ", function_name,
        " to an Arrow call expression");
  }

  Result<SubstraitAggregateToArrow> GetSubstraitAggregateToArrow(
      Id substrait_function_id) const override {
    if (substrait_function_id.uri == kArrowSimpleExtensionFunctionsUri) {
      return kSimpleSubstraitAggregateToArrow;
    }
    auto maybe_converter = substrait_to_arrow_agg_.find(substrait_function_id);
    if (maybe_converter == substrait_to_arrow_agg_.end()) {
      if (parent_) {
        return parent_->GetSubstraitAggregateToArrow(substrait_function_id);
      }
      return Status::NotImplemented(
          "No conversion function exists to convert the Substrait aggregate function ",
          substrait_function_id.uri, "#", substrait_function_id.name,
          " to an Arrow aggregate");
    }
    return maybe_converter->second;
  }

  Result<SubstraitAggregateToArrow> GetSubstraitAggregateToArrowFallback(
      std::string_view function_name) const override {
    for (const auto& converter_item : substrait_to_arrow_agg_) {
      if (converter_item.first.name == function_name) {
        return converter_item.second;
      }
    }
    if (parent_) {
      return parent_->GetSubstraitAggregateToArrowFallback(function_name);
    }
    return Status::NotImplemented(
        "No conversion function exists to convert the Substrait aggregate function ",
        function_name, " to an Arrow call expression");
  }

  Result<ArrowToSubstraitCall> GetArrowToSubstraitCall(
      const std::string& arrow_function_name) const override {
    auto maybe_converter = arrow_to_substrait_.find(arrow_function_name);
    if (maybe_converter == arrow_to_substrait_.end()) {
      if (parent_) {
        return parent_->GetArrowToSubstraitCall(arrow_function_name);
      }
      return Status::NotImplemented(
          "No conversion function exists to convert the Arrow function ",
          arrow_function_name, " to a Substrait call");
    }
    return maybe_converter->second;
  }

  Result<ArrowToSubstraitAggregate> GetArrowToSubstraitAggregate(
      const std::string& arrow_function_name) const override {
    auto maybe_converter = arrow_to_substrait_agg_.find(arrow_function_name);
    if (maybe_converter == arrow_to_substrait_agg_.end()) {
      if (parent_) {
        return parent_->GetArrowToSubstraitAggregate(arrow_function_name);
      }
      return Status::NotImplemented(
          "No conversion function exists to convert the Arrow aggregate ",
          arrow_function_name, " to a Substrait aggregate");
    }
    return maybe_converter->second;
  }

  std::vector<std::string> GetSupportedSubstraitFunctions() const override {
    std::vector<std::string> encoded_ids;
    for (const auto& entry : substrait_to_arrow_) {
      encoded_ids.push_back(entry.first.ToString());
    }
    for (const auto& entry : substrait_to_arrow_agg_) {
      encoded_ids.push_back(entry.first.ToString());
    }
    if (parent_) {
      std::vector<std::string> parent_ids = parent_->GetSupportedSubstraitFunctions();
      encoded_ids.insert(encoded_ids.end(), make_move_iterator(parent_ids.begin()),
                         make_move_iterator(parent_ids.end()));
    }
    std::sort(encoded_ids.begin(), encoded_ids.end());
    return encoded_ids;
  }

  // Defined below since it depends on some helper functions defined below
  Status AddSubstraitCallToArrow(Id substrait_function_id,
                                 std::string arrow_function_name) override;

  // Parent registry, null for the root, non-null for nested
  const ExtensionIdRegistry* parent_;

  // owning storage of ids & types
  std::unique_ptr<IdStorage> ids_ = IdStorage::Make();
  DataTypeVector types_;
  // There should only be one entry per Arrow function so there is no need
  // to separate ownership and lookup
  std::unordered_map<std::string, ArrowToSubstraitCall> arrow_to_substrait_;
  std::unordered_map<std::string, ArrowToSubstraitAggregate> arrow_to_substrait_agg_;

  // non-owning lookup helpers
  std::vector<Id> type_ids_;
  std::unordered_map<Id, int, IdHashEq, IdHashEq> id_to_index_;
  std::unordered_map<const DataType*, int, TypePtrHashEq, TypePtrHashEq> type_to_index_;
  std::unordered_map<Id, SubstraitCallToArrow, IdHashEq, IdHashEq> substrait_to_arrow_;
  std::unordered_map<Id, SubstraitAggregateToArrow, IdHashEq, IdHashEq>
      substrait_to_arrow_agg_;
};

template <typename Enum>
class EnumParser {
 public:
  explicit EnumParser(const std::vector<std::string>& options) {
    for (std::size_t i = 0; i < options.size(); i++) {
      parse_map_[options[i]] = static_cast<Enum>(i);
      reverse_map_[static_cast<Enum>(i)] = options[i];
    }
  }

  Result<Enum> Parse(std::string_view enum_val) const {
    auto it = parse_map_.find(std::string(enum_val));
    if (it == parse_map_.end()) {
      return Status::NotImplemented("The value ", enum_val,
                                    " is not an expected enum value");
    }
    return it->second;
  }

  std::string ImplementedOptionsAsString(
      const std::vector<Enum>& implemented_opts) const {
    std::vector<std::string_view> opt_strs;
    for (const Enum& implemented_opt : implemented_opts) {
      auto it = reverse_map_.find(implemented_opt);
      if (it == reverse_map_.end()) {
        opt_strs.emplace_back("Unknown");
      } else {
        opt_strs.emplace_back(it->second);
      }
    }
    return arrow::internal::JoinStrings(opt_strs, ", ");
  }

 private:
  std::unordered_map<std::string, Enum> parse_map_;
  std::unordered_map<Enum, std::string> reverse_map_;
};

enum class TemporalComponent { kYear = 0, kMonth, kDay, kSecond };
static std::vector<std::string> kTemporalComponentOptions = {"YEAR", "MONTH", "DAY",
                                                             "SECOND"};
static EnumParser<TemporalComponent> kTemporalComponentParser(kTemporalComponentOptions);

enum class OverflowBehavior { kSilent = 0, kSaturate, kError };
static std::vector<std::string> kOverflowOptions = {"SILENT", "SATURATE", "ERROR"};
static EnumParser<OverflowBehavior> kOverflowParser(kOverflowOptions);

static std::vector<std::string> kRoundModes = {
    "FLOOR",  "CEILING",          "TRUNCATE",           "AWAY_FROM_ZERO", "TIE_DOWN",
    "TIE_UP", "TIE_TOWARDS_ZERO", "TIE_AWAY_FROM_ZERO", "TIE_TO_EVEN",    "TIE_TO_ODD"};
static EnumParser<compute::RoundMode> kRoundModeParser(kRoundModes);

template <typename Enum>
Result<Enum> ParseOptionOrElse(const SubstraitCall& call, std::string_view option_name,
                               const EnumParser<Enum>& parser,
                               const std::vector<Enum>& implemented_options,
                               Enum fallback) {
  std::optional<std::vector<std::string> const*> enum_arg = call.GetOption(option_name);
  if (!enum_arg.has_value()) {
    return fallback;
  }
  std::vector<std::string> const* prefs = *enum_arg;
  for (const std::string& pref : *prefs) {
    ARROW_ASSIGN_OR_RAISE(Enum parsed, parser.Parse(pref));
    for (Enum implemented_opt : implemented_options) {
      if (implemented_opt == parsed) {
        return parsed;
      }
    }
  }

  // Prepare error message
  return Status::NotImplemented(
      "During a call to a function with id ", call.id().uri, "#", call.id().name,
      " the plan requested the option ", option_name, " to be one of [",
      arrow::internal::JoinStrings(*prefs, ", "),
      "] but the only supported options are [",
      parser.ImplementedOptionsAsString(implemented_options), "]");
}

template <typename Enum>
Result<Enum> ParseEnumArg(const SubstraitCall& call, int arg_index,
                          const EnumParser<Enum>& parser) {
  ARROW_ASSIGN_OR_RAISE(std::string_view enum_val, call.GetEnumArg(arg_index));
  return parser.Parse(enum_val);
}

Result<std::vector<compute::Expression>> GetValueArgs(const SubstraitCall& call,
                                                      int start_index) {
  std::vector<compute::Expression> expressions;
  for (int index = start_index; index < call.size(); index++) {
    ARROW_ASSIGN_OR_RAISE(compute::Expression arg, call.GetValueArg(index));
    expressions.push_back(arg);
  }
  return std::move(expressions);
}

ExtensionIdRegistry::SubstraitCallToArrow DecodeOptionlessOverflowableArithmetic(
    const std::string& function_name) {
  return [function_name](const SubstraitCall& call) -> Result<compute::Expression> {
    ARROW_ASSIGN_OR_RAISE(
        OverflowBehavior overflow_behavior,
        ParseOptionOrElse(call, "overflow", kOverflowParser,
                          {OverflowBehavior::kSilent, OverflowBehavior::kError},
                          OverflowBehavior::kSilent));
    ARROW_ASSIGN_OR_RAISE(std::vector<compute::Expression> value_args,
                          GetValueArgs(call, 0));
    if (overflow_behavior == OverflowBehavior::kSilent) {
      return arrow::compute::call(function_name, std::move(value_args));
    } else if (overflow_behavior == OverflowBehavior::kError) {
      return arrow::compute::call(function_name + "_checked", std::move(value_args));
    } else {
      return Status::NotImplemented(
          "Only SILENT and ERROR arithmetic kernels are currently implemented but ",
          kOverflowOptions[static_cast<int>(overflow_behavior)], " was requested");
    }
  };
}

ExtensionIdRegistry::SubstraitCallToArrow DecodeOptionlessUncheckedArithmetic(
    const std::string& function_name) {
  return [function_name](const SubstraitCall& call) -> Result<compute::Expression> {
    ARROW_ASSIGN_OR_RAISE(std::vector<compute::Expression> value_args,
                          GetValueArgs(call, 0));
    return arrow::compute::call(function_name, std::move(value_args));
  };
}

ExtensionIdRegistry::SubstraitCallToArrow DecodeBinaryRoundingMode(
    const std::string& function_name) {
  return [function_name](const SubstraitCall& call) -> Result<compute::Expression> {
    ARROW_ASSIGN_OR_RAISE(
        compute::RoundMode round_mode,
        ParseOptionOrElse(
            call, "rounding", kRoundModeParser,
            {compute::RoundMode::DOWN, compute::RoundMode::UP,
             compute::RoundMode::TOWARDS_ZERO, compute::RoundMode::TOWARDS_INFINITY,
             compute::RoundMode::HALF_DOWN, compute::RoundMode::HALF_UP,
             compute::RoundMode::HALF_TOWARDS_ZERO,
             compute::RoundMode::HALF_TOWARDS_INFINITY, compute::RoundMode::HALF_TO_EVEN,
             compute::RoundMode::HALF_TO_ODD},
            compute::RoundMode::HALF_TO_EVEN));
    ARROW_ASSIGN_OR_RAISE(std::vector<compute::Expression> value_args,
                          GetValueArgs(call, 0));
    std::shared_ptr<compute::RoundBinaryOptions> options =
        std::make_shared<compute::RoundBinaryOptions>();
    options->round_mode = round_mode;
    return arrow::compute::call("round_binary", std::move(value_args),
                                std::move(options));
  };
}

template <bool kChecked>
ExtensionIdRegistry::ArrowToSubstraitCall EncodeOptionlessOverflowableArithmetic(
    Id substrait_fn_id) {
  return
      [substrait_fn_id](const compute::Expression::Call& call) -> Result<SubstraitCall> {
        // nullable=true isn't quite correct but we don't know the nullability of
        // the inputs
        SubstraitCall substrait_call(substrait_fn_id, call.type.GetSharedPtr(),
                                     /*nullable=*/true);
        if (kChecked) {
          substrait_call.SetOption("overflow", {"ERROR"});
        } else {
          substrait_call.SetOption("overflow", {"SILENT"});
        }
        for (std::size_t i = 0; i < call.arguments.size(); i++) {
          substrait_call.SetValueArg(static_cast<int>(i), call.arguments[i]);
        }
        return std::move(substrait_call);
      };
}

ExtensionIdRegistry::ArrowToSubstraitCall EncodeOptionlessComparison(Id substrait_fn_id) {
  return
      [substrait_fn_id](const compute::Expression::Call& call) -> Result<SubstraitCall> {
        // nullable=true isn't quite correct but we don't know the nullability of
        // the inputs
        SubstraitCall substrait_call(substrait_fn_id, call.type.GetSharedPtr(),
                                     /*nullable=*/true);
        for (std::size_t i = 0; i < call.arguments.size(); i++) {
          substrait_call.SetValueArg(static_cast<int>(i), call.arguments[i]);
        }
        return std::move(substrait_call);
      };
}

ExtensionIdRegistry::SubstraitCallToArrow DecodeOptionlessBasicMapping(
    const std::string& function_name, int max_args) {
  return [function_name,
          max_args](const SubstraitCall& call) -> Result<compute::Expression> {
    if (call.size() > max_args) {
      return Status::NotImplemented("Acero does not have a kernel for ", function_name,
                                    " that receives ", call.size(), " arguments");
    }
    ARROW_ASSIGN_OR_RAISE(std::vector<compute::Expression> value_args,
                          GetValueArgs(call, 0));
    return arrow::compute::call(function_name, std::move(value_args));
  };
}

ExtensionIdRegistry::SubstraitCallToArrow DecodeTemporalExtractionMapping() {
  return [](const SubstraitCall& call) -> Result<compute::Expression> {
    ARROW_ASSIGN_OR_RAISE(TemporalComponent temporal_component,
                          ParseEnumArg(call, 0, kTemporalComponentParser));
    ARROW_ASSIGN_OR_RAISE(std::vector<compute::Expression> value_args,
                          GetValueArgs(call, 1));
    std::string func_name;
    switch (temporal_component) {
      case TemporalComponent::kYear:
        func_name = "year";
        break;
      case TemporalComponent::kMonth:
        func_name = "month";
        break;
      case TemporalComponent::kDay:
        func_name = "day";
        break;
      case TemporalComponent::kSecond:
        func_name = "second";
        break;
      default:
        return Status::Invalid("Unexpected value for temporal component in extract call");
    }
    return compute::call(func_name, std::move(value_args));
  };
}

ExtensionIdRegistry::SubstraitCallToArrow DecodeConcatMapping() {
  return [](const SubstraitCall& call) -> Result<compute::Expression> {
    ARROW_ASSIGN_OR_RAISE(std::vector<compute::Expression> value_args,
                          GetValueArgs(call, 0));
    value_args.push_back(compute::literal(""));
    return compute::call("binary_join_element_wise", std::move(value_args));
  };
}

ExtensionIdRegistry::SubstraitAggregateToArrow DecodeBasicAggregate(
    const std::string& arrow_function_name) {
  return [arrow_function_name](const SubstraitCall& call) -> Result<compute::Aggregate> {
    std::string fixed_arrow_func;
    if (call.is_hash()) {
      fixed_arrow_func = "hash_";
    }

    switch (call.size()) {
      case 0: {
        if (call.id().name == "count") {
          fixed_arrow_func += "count_all";
          return compute::Aggregate{std::move(fixed_arrow_func), ""};
        }
        return Status::Invalid("Expected aggregate call ", call.id().uri, "#",
                               call.id().name, " to have at least one argument");
      }
      case 1: {
        std::shared_ptr<compute::FunctionOptions> options = nullptr;
        if (arrow_function_name == "stddev" || arrow_function_name == "variance") {
          // See the following URL for the spec of stddev and variance:
          // https://github.com/substrait-io/substrait/blob/
          // 73228b4112d79eb1011af0ebb41753ce23ca180c/
          // extensions/functions_arithmetic.yaml#L1240
          auto maybe_dist = call.GetOption("distribution");
          if (maybe_dist) {
            auto& prefs = **maybe_dist;
            if (prefs.size() != 1) {
              return Status::Invalid("expected a single preference for ",
                                     arrow_function_name, " but got ", prefs.size());
            }
            int ddof;
            if (prefs[0] == "POPULATION") {
              ddof = 1;
            } else if (prefs[0] == "SAMPLE") {
              ddof = 0;
            } else {
              return Status::Invalid("unknown distribution preference ", prefs[0]);
            }
            options = std::make_shared<compute::VarianceOptions>(ddof);
          }
        }
        fixed_arrow_func += arrow_function_name;

        ARROW_ASSIGN_OR_RAISE(compute::Expression arg, call.GetValueArg(0));
        const FieldRef* arg_ref = arg.field_ref();
        if (!arg_ref) {
          return Status::Invalid("Expected an aggregate call ", call.id().uri, "#",
                                 call.id().name, " to have a direct reference");
        }

        return compute::Aggregate{std::move(fixed_arrow_func),
                                  options ? std::move(options) : nullptr, *arg_ref, ""};
      }
      default:
        break;
    }
    return Status::NotImplemented(
        "Only nullary and unary aggregate functions are currently supported");
  };
}

struct DefaultExtensionIdRegistry : ExtensionIdRegistryImpl {
  DefaultExtensionIdRegistry() {
    // ----------- Extension Types ----------------------------
    struct TypeName {
      std::shared_ptr<DataType> type;
      std::string_view name;
    };

    // The type (variation) mappings listed below need to be kept in sync
    // with the YAML at substrait/format/extension_types.yaml manually;
    // see ARROW-15535.
    for (TypeName e : {
             TypeName{uint8(), "u8"},
             TypeName{uint16(), "u16"},
             TypeName{uint32(), "u32"},
             TypeName{uint64(), "u64"},
             TypeName{float16(), "fp16"},
         }) {
      DCHECK_OK(RegisterType({kArrowExtTypesUri, e.name}, std::move(e.type)));
    }

    for (TypeName e :
         {TypeName{null(), "null"}, TypeName{month_interval(), "interval_month"},
          TypeName{day_time_interval(), "interval_day_milli"},
          TypeName{month_day_nano_interval(), "interval_month_day_nano"}}) {
      DCHECK_OK(RegisterType({kArrowExtTypesUri, e.name}, std::move(e.type)));
    }

    // -------------- Substrait -> Arrow Functions -----------------
    // Mappings with a _checked variant
    for (const auto& function_name :
         {"add", "subtract", "multiply", "divide", "power", "sqrt", "abs"}) {
      DCHECK_OK(
          AddSubstraitCallToArrow({kSubstraitArithmeticFunctionsUri, function_name},
                                  DecodeOptionlessOverflowableArithmetic(function_name)));
    }

    // Mappings without a _checked variant
    for (const auto& function_name : {"exp", "sign"}) {
      DCHECK_OK(
          AddSubstraitCallToArrow({kSubstraitArithmeticFunctionsUri, function_name},
                                  DecodeOptionlessUncheckedArithmetic(function_name)));
    }

    // Mappings for log functions
    for (const auto& function_name : {"ln", "log10", "log2", "logb", "log1p"}) {
      DCHECK_OK(
          AddSubstraitCallToArrow({kSubstraitLogarithmicFunctionsUri, function_name},
                                  DecodeOptionlessUncheckedArithmetic(function_name)));
    }

    // Mappings for rounding functions
    for (const auto& function_name : {"ceil", "floor"}) {
      DCHECK_OK(
          AddSubstraitCallToArrow({kSubstraitRoundingFunctionsUri, function_name},
                                  DecodeOptionlessUncheckedArithmetic(function_name)));
    }
    // Expose only the binary version of round
    DCHECK_OK(AddSubstraitCallToArrow({kSubstraitRoundingFunctionsUri, "round"},
                                      DecodeBinaryRoundingMode("round_binary")));

    // Basic mappings that need _kleene appended to them
    for (const auto& function_name : {"or", "and"}) {
      DCHECK_OK(AddSubstraitCallToArrow(
          {kSubstraitBooleanFunctionsUri, function_name},
          DecodeOptionlessBasicMapping(std::string(function_name) + "_kleene",
                                       /*max_args=*/2)));
    }
    // Basic binary mappings
    for (const auto& function_name :
         std::vector<std::pair<std::string_view, std::string_view>>{
             {kSubstraitBooleanFunctionsUri, "xor"},
             {kSubstraitComparisonFunctionsUri, "equal"},
             {kSubstraitComparisonFunctionsUri, "not_equal"}}) {
      DCHECK_OK(AddSubstraitCallToArrow(
          {function_name.first, function_name.second},
          DecodeOptionlessBasicMapping(std::string(function_name.second),
                                       /*max_args=*/2)));
    }
    for (const auto& uri :
         {kSubstraitComparisonFunctionsUri, kSubstraitDatetimeFunctionsUri}) {
      DCHECK_OK(AddSubstraitCallToArrow(
          {uri, "lt"}, DecodeOptionlessBasicMapping("less", /*max_args=*/2)));
      DCHECK_OK(AddSubstraitCallToArrow(
          {uri, "lte"}, DecodeOptionlessBasicMapping("less_equal", /*max_args=*/2)));
      DCHECK_OK(AddSubstraitCallToArrow(
          {uri, "gt"}, DecodeOptionlessBasicMapping("greater", /*max_args=*/2)));
      DCHECK_OK(AddSubstraitCallToArrow(
          {uri, "gte"}, DecodeOptionlessBasicMapping("greater_equal", /*max_args=*/2)));
    }
    // One-off mappings
    DCHECK_OK(
        AddSubstraitCallToArrow({kSubstraitBooleanFunctionsUri, "not"},
                                DecodeOptionlessBasicMapping("invert", /*max_args=*/1)));
    DCHECK_OK(AddSubstraitCallToArrow({kSubstraitDatetimeFunctionsUri, "extract"},
                                      DecodeTemporalExtractionMapping()));
    DCHECK_OK(AddSubstraitCallToArrow({kSubstraitStringFunctionsUri, "concat"},
                                      DecodeConcatMapping()));
    DCHECK_OK(
        AddSubstraitCallToArrow({kSubstraitComparisonFunctionsUri, "is_null"},
                                DecodeOptionlessBasicMapping("is_null", /*max_args=*/1)));
    DCHECK_OK(AddSubstraitCallToArrow(
        {kSubstraitComparisonFunctionsUri, "is_not_null"},
        DecodeOptionlessBasicMapping("is_valid", /*max_args=*/1)));

    // --------------- Substrait -> Arrow Aggregates --------------
    for (const auto& fn_name : {"sum", "min", "max", "variance"}) {
      DCHECK_OK(AddSubstraitAggregateToArrow({kSubstraitArithmeticFunctionsUri, fn_name},
                                             DecodeBasicAggregate(fn_name)));
    }
    DCHECK_OK(AddSubstraitAggregateToArrow({kSubstraitArithmeticFunctionsUri, "avg"},
                                           DecodeBasicAggregate("mean")));
    DCHECK_OK(AddSubstraitAggregateToArrow({kSubstraitArithmeticFunctionsUri, "std_dev"},
                                           DecodeBasicAggregate("stddev")));
    for (const auto& fn_name : {"count"}) {
      DCHECK_OK(
          AddSubstraitAggregateToArrow({kSubstraitAggregateGenericFunctionsUri, fn_name},
                                       DecodeBasicAggregate(fn_name)));
    }
    for (const auto& fn_name : {"first", "last"}) {
      DCHECK_OK(AddSubstraitAggregateToArrow({kArrowSimpleExtensionFunctionsUri, fn_name},
                                             DecodeBasicAggregate(fn_name)));
    }

    // --------------- Arrow -> Substrait Functions ---------------
    for (const auto& fn_name : {"add", "subtract", "multiply", "divide"}) {
      Id fn_id{kSubstraitArithmeticFunctionsUri, fn_name};
      DCHECK_OK(AddArrowToSubstraitCall(
          fn_name, EncodeOptionlessOverflowableArithmetic<false>(fn_id)));
      DCHECK_OK(
          AddArrowToSubstraitCall(std::string(fn_name) + "_checked",
                                  EncodeOptionlessOverflowableArithmetic<true>(fn_id)));
    }
    // Comparison operators
    for (const auto& fn_name : {"equal", "is_not_distinct_from"}) {
      Id fn_id{kSubstraitComparisonFunctionsUri, fn_name};
      DCHECK_OK(AddArrowToSubstraitCall(fn_name, EncodeOptionlessComparison(fn_id)));
    }
  }
};

}  // namespace

Status ExtensionIdRegistryImpl::AddSubstraitCallToArrow(Id substrait_function_id,
                                                        std::string arrow_function_name) {
  return AddSubstraitCallToArrow(
      substrait_function_id,
      [arrow_function_name](const SubstraitCall& call) -> Result<compute::Expression> {
        ARROW_ASSIGN_OR_RAISE(std::vector<compute::Expression> value_args,
                              GetValueArgs(call, 0));
        return compute::call(arrow_function_name, std::move(value_args));
      });
}

ExtensionIdRegistry* default_extension_id_registry() {
  static DefaultExtensionIdRegistry impl_;
  return &impl_;
}

std::shared_ptr<ExtensionIdRegistry> nested_extension_id_registry(
    const ExtensionIdRegistry* parent) {
  return std::make_shared<ExtensionIdRegistryImpl>(parent);
}

}  // namespace engine
}  // namespace arrow
