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

struct IdHashEq {
  using Id = ExtensionSet::Id;

  size_t operator()(Id id) const {
    constexpr ::arrow::internal::StringViewHash hash = {};
    auto out = static_cast<size_t>(hash(id.uri));
    ::arrow::internal::hash_combine(out, hash(id.name));
    return out;
  }

  bool operator()(Id l, Id r) const { return l.uri == r.uri && l.name == r.name; }
};

}  // namespace

// A builder used when creating a Substrait plan from an Arrow execution plan.  In
// that situation we do not have a set of anchor values already defined so we keep
// a map of what Ids we have seen.
struct ExtensionSet::Impl {
  void AddUri(util::string_view uri, ExtensionSet* self) {
    if (uris_.find(uri) != uris_.end()) return;

    self->uris_.push_back(uri);
    uris_.insert(self->uris_.back());  // lookup helper's keys should reference memory
                                       // owned by this ExtensionSet
  }

  Status CheckHasUri(util::string_view uri) {
    if (uris_.find(uri) != uris_.end()) return Status::OK();

    return Status::Invalid(
        "Uri ", uri,
        " was referenced by an extension but was not declared in the ExtensionSet.");
  }

  uint32_t EncodeType(ExtensionIdRegistry::TypeRecord type_record, ExtensionSet* self) {
    // note: at this point we're guaranteed to have an Id which points to memory owned by
    // the set's registry.
    AddUri(type_record.id.uri, self);
    auto it_success =
        types_.emplace(type_record.id, static_cast<uint32_t>(types_.size()));

    if (it_success.second) {
      self->types_.push_back(
          {type_record.id, type_record.type, type_record.is_variation});
    }

    return it_success.first->second;
  }

  uint32_t EncodeFunction(Id id, util::string_view function_name, ExtensionSet* self) {
    // note: at this point we're guaranteed to have an Id which points to memory owned by
    // the set's registry.
    AddUri(id.uri, self);
    auto it_success = functions_.emplace(id, static_cast<uint32_t>(functions_.size()));

    if (it_success.second) {
      self->functions_.push_back({id, function_name});
    }

    return it_success.first->second;
  }

  std::unordered_set<util::string_view, ::arrow::internal::StringViewHash> uris_;
  std::unordered_map<Id, uint32_t, IdHashEq, IdHashEq> types_, functions_;
};

ExtensionSet::ExtensionSet(ExtensionIdRegistry* registry)
    : registry_(registry), impl_(new Impl(), [](Impl* impl) { delete impl; }) {}

Result<ExtensionSet> ExtensionSet::Make(std::vector<util::string_view> uris,
                                        std::vector<Id> type_ids,
                                        std::vector<bool> type_is_variation,
                                        std::vector<Id> function_ids,
                                        ExtensionIdRegistry* registry) {
  ExtensionSet set;
  set.registry_ = registry;

  // TODO(bkietz) move this into the registry as registry->OwnUris(&uris) or so
  std::unordered_set<util::string_view, ::arrow::internal::StringViewHash>
      uris_owned_by_registry;
  for (util::string_view uri : registry->Uris()) {
    uris_owned_by_registry.insert(uri);
  }

  for (auto& uri : uris) {
    if (uri.empty()) continue;
    auto it = uris_owned_by_registry.find(uri);
    if (it == uris_owned_by_registry.end()) {
      return Status::KeyError("Uri '", uri, "' not found in registry");
    }
    uri = *it;  // Ensure uris point into the registry's memory
    set.impl_->AddUri(*it, &set);
  }

  if (type_ids.size() != type_is_variation.size()) {
    return Status::Invalid("Received ", type_ids.size(), " type ids but a ",
                           type_is_variation.size(), "-long is_variation vector");
  }

  set.types_.resize(type_ids.size());

  for (size_t i = 0; i < type_ids.size(); ++i) {
    if (type_ids[i].empty()) continue;
    RETURN_NOT_OK(set.impl_->CheckHasUri(type_ids[i].uri));

    if (auto rec = registry->GetType(type_ids[i], type_is_variation[i])) {
      set.types_[i] = {rec->id, rec->type, rec->is_variation};
      continue;
    }
    return Status::Invalid("Type", (type_is_variation[i] ? " variation" : ""), " ",
                           type_ids[i].uri, "#", type_ids[i].name, " not found");
  }

  set.functions_.resize(function_ids.size());

  for (size_t i = 0; i < function_ids.size(); ++i) {
    if (function_ids[i].empty()) continue;
    RETURN_NOT_OK(set.impl_->CheckHasUri(function_ids[i].uri));

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
  if (anchor >= types_.size() || types_[anchor].id.empty()) {
    return Status::Invalid("User defined type reference ", anchor,
                           " did not have a corresponding anchor in the extension set");
  }
  return types_[anchor];
}

Result<uint32_t> ExtensionSet::EncodeType(const DataType& type) {
  if (auto rec = registry_->GetType(type)) {
    return impl_->EncodeType(*rec, this);
  }
  return Status::KeyError("type ", type.ToString(), " not found in the registry");
}

Result<ExtensionSet::FunctionRecord> ExtensionSet::DecodeFunction(uint32_t anchor) const {
  if (anchor >= functions_.size() || functions_[anchor].id.empty()) {
    return Status::Invalid("User defined function reference ", anchor,
                           " did not have a corresponding anchor in the extension set");
  }
  return functions_[anchor];
}

Result<uint32_t> ExtensionSet::EncodeFunction(util::string_view function_name) {
  if (auto rec = registry_->GetFunction(function_name)) {
    return impl_->EncodeFunction(rec->id, rec->function_name, this);
  }
  return Status::KeyError("function ", function_name, " not found in the registry");
}

template <typename KeyToIndex, typename Key>
const int* GetIndex(const KeyToIndex& key_to_index, const Key& key) {
  auto it = key_to_index.find(key);
  if (it == key_to_index.end()) return nullptr;
  return &it->second;
}

ExtensionIdRegistry* default_extension_id_registry() {
  static struct Impl : ExtensionIdRegistry {
    Impl() {
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
        DCHECK_OK(RegisterType({kArrowExtTypesUri, e.name}, std::move(e.type),
                               /*is_variation=*/true));
      }

      for (TypeName e : {
               TypeName{null(), "null"},
               TypeName{month_interval(), "interval_month"},
               TypeName{day_time_interval(), "interval_day_milli"},
               TypeName{month_day_nano_interval(), "interval_month_day_nano"},
           }) {
        DCHECK_OK(RegisterType({kArrowExtTypesUri, e.name}, std::move(e.type),
                               /*is_variation=*/false));
      }

      // TODO: this is just a placeholder right now. We'll need a YAML file for
      // all functions (and prototypes) that Arrow provides that are relevant
      // for Substrait, and include mappings for all of them here. See
      // ARROW-15535.
      for (util::string_view name : {
               "add",
           }) {
        DCHECK_OK(RegisterFunction({kArrowExtTypesUri, name}, name.to_string()));
      }
    }

    std::vector<util::string_view> Uris() const override {
      return {uris_.begin(), uris_.end()};
    }

    util::optional<TypeRecord> GetType(const DataType& type) const override {
      if (auto index = GetIndex(type_to_index_, &type)) {
        return TypeRecord{type_ids_[*index], types_[*index], type_is_variation_[*index]};
      }
      return {};
    }

    util::optional<TypeRecord> GetType(Id id, bool is_variation) const override {
      if (auto index =
              GetIndex(is_variation ? variation_id_to_index_ : id_to_index_, id)) {
        return TypeRecord{type_ids_[*index], types_[*index], type_is_variation_[*index]};
      }
      return {};
    }

    Status RegisterType(Id id, std::shared_ptr<DataType> type,
                        bool is_variation) override {
      DCHECK_EQ(type_ids_.size(), types_.size());
      DCHECK_EQ(type_ids_.size(), type_is_variation_.size());

      Id copied_id{*uris_.emplace(id.uri.to_string()).first,
                   *names_.emplace(id.name.to_string()).first};

      auto index = static_cast<int>(type_ids_.size());

      auto* id_to_index = is_variation ? &variation_id_to_index_ : &id_to_index_;
      auto it_success = id_to_index->emplace(copied_id, index);

      if (!it_success.second) {
        return Status::Invalid("Type id was already registered");
      }

      if (!type_to_index_.emplace(type.get(), index).second) {
        id_to_index->erase(it_success.first);
        return Status::Invalid("Type was already registered");
      }

      type_ids_.push_back(copied_id);
      types_.push_back(std::move(type));
      type_is_variation_.push_back(is_variation);
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

    // owning storage of uris, names, (arrow::)function_names, types
    //    note that storing strings like this is safe since references into an
    //    unordered_set are not invalidated on insertion
    std::unordered_set<std::string> uris_, names_, function_names_;
    DataTypeVector types_;
    std::vector<bool> type_is_variation_;

    // non-owning lookup helpers
    std::vector<Id> type_ids_, function_ids_;
    std::unordered_map<Id, int, IdHashEq, IdHashEq> id_to_index_, variation_id_to_index_;
    std::unordered_map<const DataType*, int, TypePtrHashEq, TypePtrHashEq> type_to_index_;

    std::vector<const std::string*> function_name_ptrs_;
    std::unordered_map<Id, int, IdHashEq, IdHashEq> function_id_to_index_;
    std::unordered_map<util::string_view, int, ::arrow::internal::StringViewHash>
        function_name_to_index_;
  } impl_;

  return &impl_;
}

}  // namespace engine
}  // namespace arrow
