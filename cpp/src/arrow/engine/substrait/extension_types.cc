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

#include "arrow/engine/substrait/extension_types.h"

#include <unordered_map>
#include <unordered_set>

#include "arrow/engine/simple_extension_type_internal.h"
#include "arrow/util/hash_util.h"
#include "arrow/util/hashing.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace engine {
namespace {

constexpr util::string_view kUuidExtensionName = "uuid";
struct UuidExtensionParams {};
std::shared_ptr<DataType> UuidGetStorage(const UuidExtensionParams&) {
  return fixed_size_binary(16);
}
static auto kUuidExtensionParamsProperties = internal::MakeProperties();

using UuidType = SimpleExtensionType<kUuidExtensionName, UuidExtensionParams,
                                     decltype(kUuidExtensionParamsProperties),
                                     kUuidExtensionParamsProperties, UuidGetStorage>;

constexpr util::string_view kFixedCharExtensionName = "fixed_char";
struct FixedCharExtensionParams {
  int32_t length;
};
std::shared_ptr<DataType> FixedCharGetStorage(const FixedCharExtensionParams& params) {
  return fixed_size_binary(params.length);
}
static auto kFixedCharExtensionParamsProperties = internal::MakeProperties(
    internal::DataMember("length", &FixedCharExtensionParams::length));

using FixedCharType =
    SimpleExtensionType<kFixedCharExtensionName, FixedCharExtensionParams,
                        decltype(kFixedCharExtensionParamsProperties),
                        kFixedCharExtensionParamsProperties, FixedCharGetStorage>;

constexpr util::string_view kVarCharExtensionName = "varchar";
struct VarCharExtensionParams {
  int32_t length;
};
std::shared_ptr<DataType> VarCharGetStorage(const VarCharExtensionParams&) {
  return utf8();
}
static auto kVarCharExtensionParamsProperties = internal::MakeProperties(
    internal::DataMember("length", &VarCharExtensionParams::length));

using VarCharType =
    SimpleExtensionType<kVarCharExtensionName, VarCharExtensionParams,
                        decltype(kVarCharExtensionParamsProperties),
                        kVarCharExtensionParamsProperties, VarCharGetStorage>;

constexpr util::string_view kIntervalYearExtensionName = "interval_year";
struct IntervalYearExtensionParams {};
std::shared_ptr<DataType> IntervalYearGetStorage(const IntervalYearExtensionParams&) {
  return fixed_size_list(int32(), 2);
}
static auto kIntervalYearExtensionParamsProperties = internal::MakeProperties();

using IntervalYearType =
    SimpleExtensionType<kIntervalYearExtensionName, IntervalYearExtensionParams,
                        decltype(kIntervalYearExtensionParamsProperties),
                        kIntervalYearExtensionParamsProperties, IntervalYearGetStorage>;

constexpr util::string_view kIntervalDayExtensionName = "interval_day";
struct IntervalDayExtensionParams {};
std::shared_ptr<DataType> IntervalDayGetStorage(const IntervalDayExtensionParams&) {
  return fixed_size_list(int32(), 2);
}
static auto kIntervalDayExtensionParamsProperties = internal::MakeProperties();

using IntervalDayType =
    SimpleExtensionType<kIntervalDayExtensionName, IntervalDayExtensionParams,
                        decltype(kIntervalDayExtensionParamsProperties),
                        kIntervalDayExtensionParamsProperties, IntervalDayGetStorage>;

}  // namespace

std::shared_ptr<DataType> uuid() { return UuidType::Make({}); }

std::shared_ptr<DataType> fixed_char(int32_t length) {
  return FixedCharType::Make({length});
}

std::shared_ptr<DataType> varchar(int32_t length) { return VarCharType::Make({length}); }

std::shared_ptr<DataType> interval_year() { return IntervalYearType::Make({}); }

std::shared_ptr<DataType> interval_day() { return IntervalDayType::Make({}); }

bool UnwrapUuid(const DataType& t) {
  if (auto params = UuidType::GetIf(t)) {
    return true;
  }
  return false;
}

util::optional<int32_t> UnwrapFixedChar(const DataType& t) {
  if (auto params = FixedCharType::GetIf(t)) {
    return params->length;
  }
  return util::nullopt;
}

util::optional<int32_t> UnwrapVarChar(const DataType& t) {
  if (auto params = VarCharType::GetIf(t)) {
    return params->length;
  }
  return util::nullopt;
}

bool UnwrapIntervalYear(const DataType& t) {
  if (auto params = IntervalYearType::GetIf(t)) {
    return true;
  }
  return false;
}

bool UnwrapIntervalDay(const DataType& t) {
  if (auto params = IntervalDayType::GetIf(t)) {
    return true;
  }
  return false;
}

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
    constexpr internal::StringViewHash hash = {};
    auto out = hash(id.uri);
    internal::hash_combine(out, hash(id.name));
    return out;
  }

  bool operator()(Id l, Id r) const { return l.uri == r.uri && l.name == r.name; }
};

}  // namespace

struct ExtensionSet::Impl {
  void AddUri(util::string_view uri, ExtensionSet* self) {
    if (uris_.find(uri) != uris_.end()) return;

    self->uris_.push_back(uri.to_string());
    uris_.insert(self->uris_.back());  // lookup helper's keys should reference memory
                                       // owned by this ExtensionSet
  }

  Status CheckHasUri(util::string_view uri) {
    if (uris_.find(uri) != uris_.end()) return Status::OK();

    return Status::Invalid(
        "Uri ", uri,
        " was referenced by an extension but was not declared in the ExtensionSet.");
  }

  uint32_t EncodeType(Id id, const std::shared_ptr<DataType>& type, bool is_variation,
                      ExtensionSet* self) {
    AddUri(id.uri, self);
    auto it_success = types_.emplace(id, static_cast<uint32_t>(types_.size()));

    if (it_success.second) {
      DCHECK_EQ(self->type_ids_.size(), self->types_.size());
      self->type_ids_.push_back(id);
      self->types_.push_back(type);
      self->type_is_variation_.push_back(is_variation);
    }

    return it_success.first->second;
  }

  std::unordered_set<util::string_view, internal::StringViewHash> uris_;
  std::unordered_map<Id, uint32_t, IdHashEq, IdHashEq> types_;
};

ExtensionSet::ExtensionSet(ExtensionIdRegistry* registry)
    : registry_(registry), impl_(new Impl(), [](Impl* impl) { delete impl; }) {}

Result<ExtensionSet> ExtensionSet::Make(std::vector<std::string> uris,
                                        std::vector<Id> type_ids,
                                        std::vector<bool> type_is_variation,
                                        ExtensionIdRegistry* registry) {
  ExtensionSet set;
  set.registry_ = registry;

  for (auto uri : uris) {
    if (uri.empty()) continue;
    set.impl_->AddUri(uri, &set);
  }
  set.uris_ = std::move(uris);

  if (type_ids.size() != type_is_variation.size()) {
    return Status::Invalid("Received ", type_ids.size(), " type ids but a ",
                           type_is_variation.size(), "-long is_variation vector");
  }
  set.types_.resize(type_ids.size());

  for (size_t i = 0; i < type_ids.size(); ++i) {
    if (type_ids[i].empty()) continue;
    RETURN_NOT_OK(set.impl_->CheckHasUri(type_ids[i].uri));

    if (auto rec = registry->GetType(type_ids[i], type_is_variation[i])) {
      set.types_[i] = rec->type;
      type_ids[i] = rec->id;  // use Id which references memory owned by the registry
      continue;
    }
    return Status::Invalid("Type not found");
  }

  set.type_ids_ = std::move(type_ids);
  set.type_is_variation_ = std::move(type_is_variation);

  return std::move(set);
}

uint32_t ExtensionSet::EncodeType(Id id, const std::shared_ptr<DataType>& type,
                                  bool is_variation) {
  return impl_->EncodeType(id, type, is_variation, this);
}

Result<uint32_t> ExtensionSet::EncodeType(const DataType& type) {
  if (auto rec = registry_->GetType(type)) {
    return EncodeType(rec->id, rec->type, rec->is_variation);
  }
  return Status::KeyError("type ", type.ToString(), " not found in the registry");
}

ExtensionIdRegistry* default_extension_id_registry() {
  static struct Impl : ExtensionIdRegistry {
    Impl() {
      struct TypeName {
        std::shared_ptr<DataType> type;
        util::string_view name;
      };
      for (TypeName e : {
               TypeName{null(), "null"},
               TypeName{uint8(), "u8"},
               TypeName{uint16(), "u16"},
               TypeName{uint32(), "u32"},
               TypeName{uint64(), "u64"},
           }) {
        DCHECK_OK(RegisterType({kArrowExtTypesUri, e.name}, std::move(e.type),
                               /*is_variation=*/true));
      }
    }
    util::optional<TypeRecord> GetType(const DataType& type) const override {
      auto it = type_to_index_.find(&type);
      if (it == type_to_index_.end()) return {};

      int index = it->second;
      return TypeRecord{ids_[index], types_[index], type_is_variation_[index]};
    }

    util::optional<TypeRecord> GetType(Id id, bool is_variation) const override {
      auto* id_to_index = is_variation ? &variation_id_to_index_ : &id_to_index_;
      auto it = id_to_index->find(id);
      if (it == id_to_index->end()) return {};

      int index = it->second;
      return TypeRecord{ids_[index], types_[index], type_is_variation_[index]};
    }

    Status RegisterType(Id id, std::shared_ptr<DataType> type,
                        bool is_variation) override {
      DCHECK_EQ(ids_.size(), types_.size());
      DCHECK_EQ(ids_.size(), type_is_variation_.size());

      Id copied_id{*uris_.emplace(id.uri.to_string()).first,
                   *names_.emplace(id.name.to_string()).first};

      size_t index = ids_.size();

      auto* id_to_index = is_variation ? &variation_id_to_index_ : &id_to_index_;
      auto it_success = id_to_index->emplace(copied_id, index);

      if (!it_success.second) {
        return Status::Invalid("Type id was already registered");
      }

      if (!type_to_index_.emplace(type.get(), index).second) {
        id_to_index->erase(it_success.first);
        return Status::Invalid("Type was already registered");
      }

      ids_.push_back(copied_id);
      types_.push_back(std::move(type));
      type_is_variation_.push_back(is_variation);
      return Status::OK();
    }

    // owning storage of uris, names, types
    std::unordered_set<std::string> uris_, names_;
    DataTypeVector types_;
    std::vector<bool> type_is_variation_;

    // non-owning lookup helpers
    std::vector<Id> ids_;
    std::unordered_map<Id, int, IdHashEq, IdHashEq> id_to_index_, variation_id_to_index_;
    std::unordered_map<const DataType*, int, TypePtrHashEq, TypePtrHashEq> type_to_index_;
  } impl_;

  return &impl_;
}

}  // namespace engine
}  // namespace arrow
