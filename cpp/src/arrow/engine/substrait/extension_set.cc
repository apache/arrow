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

#include <unordered_map>
#include <unordered_set>

#include "arrow/util/hash_util.h"
#include "arrow/util/hashing.h"
#include "arrow/util/string_view.h"

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

size_t ExtensionIdRegistry::IdHashEq::operator()(ExtensionIdRegistry::Id id) const {
  constexpr ::arrow::internal::StringViewHash hash = {};
  auto out = static_cast<size_t>(hash(id.uri));
  ::arrow::internal::hash_combine(out, hash(id.name));
  return out;
}

bool ExtensionIdRegistry::IdHashEq::operator()(ExtensionIdRegistry::Id l,
                                               ExtensionIdRegistry::Id r) const {
  return l.uri == r.uri && l.name == r.name;
}

// A builder used when creating a Substrait plan from an Arrow execution plan.  In
// that situation we do not have a set of anchor values already defined so we keep
// a map of what Ids we have seen.
ExtensionSet::ExtensionSet(const ExtensionIdRegistry* registry) : registry_(registry) {}

Status ExtensionSet::CheckHasUri(util::string_view uri) {
  auto it =
      std::find_if(uris_.begin(), uris_.end(),
                   [&uri](const std::pair<uint32_t, util::string_view>& anchor_uri_pair) {
                     return anchor_uri_pair.second == uri;
                   });
  if (it != uris_.end()) return Status::OK();

  return Status::Invalid(
      "Uri ", uri,
      " was referenced by an extension but was not declared in the ExtensionSet.");
}

void ExtensionSet::AddUri(std::pair<uint32_t, util::string_view> uri) {
  auto it =
      std::find_if(uris_.begin(), uris_.end(),
                   [&uri](const std::pair<uint32_t, util::string_view>& anchor_uri_pair) {
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
    std::unordered_map<uint32_t, util::string_view> uris,
    std::unordered_map<uint32_t, Id> type_ids,
    std::unordered_map<uint32_t, Id> function_ids, const ExtensionIdRegistry* registry) {
  ExtensionSet set;
  set.registry_ = registry;

  // TODO(bkietz) move this into the registry as registry->OwnUris(&uris) or so
  std::unordered_set<util::string_view, ::arrow::internal::StringViewHash>
      uris_owned_by_registry;
  for (util::string_view uri : registry->Uris()) {
    uris_owned_by_registry.insert(uri);
  }

  for (auto& uri : uris) {
    auto it = uris_owned_by_registry.find(uri.second);
    if (it == uris_owned_by_registry.end()) {
      return Status::KeyError("Uri '", uri.second, "' not found in registry");
    }
    uri.second = *it;  // Ensure uris point into the registry's memory
    set.AddUri(uri);
  }

  set.types_.reserve(type_ids.size());

  for (unsigned int i = 0; i < static_cast<unsigned int>(type_ids.size()); ++i) {
    if (type_ids[i].empty()) continue;
    RETURN_NOT_OK(set.CheckHasUri(type_ids[i].uri));

    if (auto rec = registry->GetType(type_ids[i])) {
      set.types_[i] = {rec->id, rec->type};
      continue;
    }
    return Status::Invalid("Type ", type_ids[i].uri, "#", type_ids[i].name, " not found");
  }

  set.functions_.reserve(function_ids.size());

  for (unsigned int i = 0; i < static_cast<unsigned int>(function_ids.size()); ++i) {
    if (function_ids[i].empty()) continue;
    RETURN_NOT_OK(set.CheckHasUri(function_ids[i].uri));

    if (auto rec = registry->GetFunction(function_ids[i])) {
      set.functions_[i] = {rec->id, rec->function_name};
      continue;
    }
    return Status::Invalid("Function ", function_ids[i].uri, "#", function_ids[i].name,
                           " not found");
  }

  set.uris_ = std::move(uris);

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
      DCHECK_EQ(types_.find(static_cast<unsigned int>(types_.size())), types_.end())
          << "Type existed in types_ but not types_map_.  ExtensionSet is inconsistent";
      types_[static_cast<unsigned int>(types_.size())] = {rec->id, rec->type};
    }
    return it_success.first->second;
  }
  return Status::KeyError("type ", type.ToString(), " not found in the registry");
}

Result<ExtensionSet::FunctionRecord> ExtensionSet::DecodeFunction(uint32_t anchor) const {
  if (functions_.find(anchor) == functions_.end() || functions_.at(anchor).id.empty()) {
    return Status::Invalid("User defined function reference ", anchor,
                           " did not have a corresponding anchor in the extension set");
  }
  return functions_.at(anchor);
}

Result<uint32_t> ExtensionSet::EncodeFunction(util::string_view function_name) {
  if (auto rec = registry_->GetFunction(function_name)) {
    RETURN_NOT_OK(this->AddUri(rec->id));
    auto it_success =
        functions_map_.emplace(rec->id, static_cast<uint32_t>(functions_map_.size()));
    if (it_success.second) {
      DCHECK_EQ(functions_.find(static_cast<unsigned int>(functions_.size())),
                functions_.end())
          << "Function existed in functions_ but not functions_map_.  ExtensionSet is "
             "inconsistent";
      functions_[static_cast<unsigned int>(functions_.size())] = {rec->id,
                                                                  rec->function_name};
    }
    return it_success.first->second;
  }
  return Status::KeyError("function ", function_name, " not found in the registry");
}

template <typename KeyToIndex, typename Key>
const int* GetIndex(const KeyToIndex& key_to_index, const Key& key) {
  auto it = key_to_index.find(key);
  if (it == key_to_index.end()) return nullptr;
  return &it->second;
}

namespace {

struct ExtensionIdRegistryImpl : ExtensionIdRegistry {
  virtual ~ExtensionIdRegistryImpl() {}

  std::vector<util::string_view> Uris() const override {
    return {uris_.begin(), uris_.end()};
  }

  util::optional<TypeRecord> GetType(const DataType& type) const override {
    if (auto index = GetIndex(type_to_index_, &type)) {
      return TypeRecord{type_ids_[*index], types_[*index]};
    }
    return {};
  }

  util::optional<TypeRecord> GetType(Id id) const override {
    if (auto index = GetIndex(id_to_index_, id)) {
      return TypeRecord{type_ids_[*index], types_[*index]};
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
    return Status::OK();
  }

  Status RegisterType(Id id, std::shared_ptr<DataType> type) override {
    DCHECK_EQ(type_ids_.size(), types_.size());

    Id copied_id{*uris_.emplace(id.uri.to_string()).first,
                 *names_.emplace(id.name.to_string()).first};

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

  util::optional<FunctionRecord> GetFunction(
      util::string_view arrow_function_name) const override {
    if (auto index = GetIndex(function_name_to_index_, arrow_function_name)) {
      return FunctionRecord{function_ids_[*index], *function_name_ptrs_[*index]};
    }
    return {};
  }

  util::optional<FunctionRecord> GetFunction(Id id) const override {
    if (auto index = GetIndex(function_id_to_index_, id)) {
      return FunctionRecord{function_ids_[*index], *function_name_ptrs_[*index]};
    }
    return {};
  }

  Status CanRegisterFunction(Id id,
                             const std::string& arrow_function_name) const override {
    if (function_id_to_index_.find(id) != function_id_to_index_.end()) {
      return Status::Invalid("Function id was already registered");
    }
    if (function_name_to_index_.find(arrow_function_name) !=
        function_name_to_index_.end()) {
      return Status::Invalid("Function name was already registered");
    }
    return Status::OK();
  }

  Status RegisterFunction(Id id, std::string arrow_function_name) override {
    DCHECK_EQ(function_ids_.size(), function_name_ptrs_.size());

    Id copied_id{*uris_.emplace(id.uri.to_string()).first,
                 *names_.emplace(id.name.to_string()).first};

    const std::string& copied_function_name{
        *function_names_.emplace(std::move(arrow_function_name)).first};

    auto index = static_cast<int>(function_ids_.size());

    auto it_success = function_id_to_index_.emplace(copied_id, index);

    if (!it_success.second) {
      return Status::Invalid("Function id was already registered");
    }

    if (!function_name_to_index_.emplace(copied_function_name, index).second) {
      function_id_to_index_.erase(it_success.first);
      return Status::Invalid("Function name was already registered");
    }

    function_name_ptrs_.push_back(&copied_function_name);
    function_ids_.push_back(copied_id);
    return Status::OK();
  }

  Status RegisterFunction(std::string uri, std::string name,
                          std::string arrow_function_name) override {
    return RegisterFunction({uri, name}, arrow_function_name);
  }

  // owning storage of uris, names, (arrow::)function_names, types
  //    note that storing strings like this is safe since references into an
  //    unordered_set are not invalidated on insertion
  std::unordered_set<std::string> uris_, names_, function_names_;
  DataTypeVector types_;

  // non-owning lookup helpers
  std::vector<Id> type_ids_, function_ids_;
  std::unordered_map<Id, int, IdHashEq, IdHashEq> id_to_index_;
  std::unordered_map<const DataType*, int, TypePtrHashEq, TypePtrHashEq> type_to_index_;

  std::vector<const std::string*> function_name_ptrs_;
  std::unordered_map<Id, int, IdHashEq, IdHashEq> function_id_to_index_;
  std::unordered_map<util::string_view, int, ::arrow::internal::StringViewHash>
      function_name_to_index_;
};

struct NestedExtensionIdRegistryImpl : ExtensionIdRegistryImpl {
  explicit NestedExtensionIdRegistryImpl(const ExtensionIdRegistry* parent)
      : parent_(parent) {}

  virtual ~NestedExtensionIdRegistryImpl() {}

  std::vector<util::string_view> Uris() const override {
    std::vector<util::string_view> uris = parent_->Uris();
    std::unordered_set<util::string_view> uri_set;
    uri_set.insert(uris.begin(), uris.end());
    uri_set.insert(uris_.begin(), uris_.end());
    return std::vector<util::string_view>(uris);
  }

  util::optional<TypeRecord> GetType(const DataType& type) const override {
    auto type_opt = ExtensionIdRegistryImpl::GetType(type);
    if (type_opt) {
      return type_opt;
    }
    return parent_->GetType(type);
  }

  util::optional<TypeRecord> GetType(Id id) const override {
    auto type_opt = ExtensionIdRegistryImpl::GetType(id);
    if (type_opt) {
      return type_opt;
    }
    return parent_->GetType(id);
  }

  Status CanRegisterType(Id id, const std::shared_ptr<DataType>& type) const override {
    return parent_->CanRegisterType(id, type) &
           ExtensionIdRegistryImpl::CanRegisterType(id, type);
  }

  Status RegisterType(Id id, std::shared_ptr<DataType> type) override {
    return parent_->CanRegisterType(id, type) &
           ExtensionIdRegistryImpl::RegisterType(id, type);
  }

  util::optional<FunctionRecord> GetFunction(
      util::string_view arrow_function_name) const override {
    auto func_opt = ExtensionIdRegistryImpl::GetFunction(arrow_function_name);
    if (func_opt) {
      return func_opt;
    }
    return parent_->GetFunction(arrow_function_name);
  }

  util::optional<FunctionRecord> GetFunction(Id id) const override {
    auto func_opt = ExtensionIdRegistryImpl::GetFunction(id);
    if (func_opt) {
      return func_opt;
    }
    return parent_->GetFunction(id);
  }

  Status CanRegisterFunction(Id id,
                             const std::string& arrow_function_name) const override {
    return parent_->CanRegisterFunction(id, arrow_function_name) &
           ExtensionIdRegistryImpl::CanRegisterFunction(id, arrow_function_name);
  }

  Status RegisterFunction(Id id, std::string arrow_function_name) override {
    return parent_->CanRegisterFunction(id, arrow_function_name) &
           ExtensionIdRegistryImpl::RegisterFunction(id, arrow_function_name);
  }

  const ExtensionIdRegistry* parent_;
};

struct DefaultExtensionIdRegistry : ExtensionIdRegistryImpl {
  DefaultExtensionIdRegistry() {
    struct TypeName {
      std::shared_ptr<DataType> type;
      util::string_view name;
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

    for (TypeName e : {
             TypeName{null(), "null"},
             TypeName{month_interval(), "interval_month"},
             TypeName{day_time_interval(), "interval_day_milli"},
             TypeName{month_day_nano_interval(), "interval_month_day_nano"},
         }) {
      DCHECK_OK(RegisterType({kArrowExtTypesUri, e.name}, std::move(e.type)));
    }

    // TODO: this is just a placeholder right now. We'll need a YAML file for
    // all functions (and prototypes) that Arrow provides that are relevant
    // for Substrait, and include mappings for all of them here. See
    // ARROW-15535.
    for (util::string_view name : {
             "add",
             "equal",
             "is_not_distinct_from",
         }) {
      DCHECK_OK(RegisterFunction({kArrowExtTypesUri, name}, name.to_string()));
    }
  }
};

}  // namespace

ExtensionIdRegistry* default_extension_id_registry() {
  static DefaultExtensionIdRegistry impl_;
  return &impl_;
}

std::shared_ptr<ExtensionIdRegistry> nested_extension_id_registry(
    const ExtensionIdRegistry* parent) {
  return std::make_shared<NestedExtensionIdRegistryImpl>(parent);
}

}  // namespace engine
}  // namespace arrow
