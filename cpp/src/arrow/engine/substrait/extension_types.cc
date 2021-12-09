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

struct ExtensionSet::Impl {
  struct IdHash {
    size_t operator()(Id id) const {
      internal::StringViewHash hash;
      auto out = hash(id.uri);
      internal::hash_combine(out, hash(id.name));
      return out;
    }
  };

  using IdToIndexMap = std::unordered_map<Id, uint32_t, IdHash>;

  void AddUri(util::string_view uri, ExtensionSet* self) {
    if (uris_.insert(uri).second) {
      self->uris_.push_back(uri);
    }
  }

  Status CheckHasUri(util::string_view uri) {
    if (uris_.count(uri) == 0) {
      return Status::Invalid(
          "Uri ", uri,
          " was referenced by an extension but was not declared in the ExtensionSet.");
    }
    return Status::OK();
  }

  enum { kType, kTypeVariation };

  template <int kTypeOrTypeVariation>
  uint32_t EncodeTypeOrTypeVariation(Id id, const std::shared_ptr<DataType>& type,
                                     ExtensionSet* self) {
    AddUri(id.uri, self);

    auto map = (kTypeOrTypeVariation == kType ? &types_ : &type_variations_);

    auto ids =
        (kTypeOrTypeVariation == kType ? &self->type_ids_ : &self->type_variation_ids_);

    auto types =
        (kTypeOrTypeVariation == kType ? &self->types_ : &self->type_variations_);

    auto it_success = map->emplace(id, static_cast<uint32_t>(map->size()));

    if (it_success.second) {
      DCHECK_EQ(ids->size(), types->size());
      ids->push_back(id);
      types->push_back(type);
    }

    return it_success.first->second;
  }

  std::unordered_set<util::string_view, internal::StringViewHash> uris_;
  IdToIndexMap type_variations_, types_;
};

ExtensionSet::ExtensionSet() : impl_(new Impl()) {}

Result<ExtensionSet> ExtensionSet::Make(std::vector<util::string_view> uris,
                                        std::vector<Id> type_variation_ids,
                                        std::vector<Id> type_ids,
                                        ExtensionIdRegistry* registry) {
  ExtensionSet set;

  for (auto uri : uris) {
    if (uri.empty()) continue;
    set.impl_->AddUri(uri, &set);
  }
  set.uris_ = std::move(uris);

  set.types_.resize(type_ids.size());
  set.type_variations_.resize(type_variation_ids.size());

  uint32_t anchor = 0;
  for (Id id : type_variation_ids) {
    auto i = anchor++;
    if (id.empty()) continue;
    RETURN_NOT_OK(set.impl_->CheckHasUri(id.uri));

    auto type = registry->GetTypeVariation(id);
    if (type == nullptr) {
      return Status::Invalid("Type variation not found");
    }
    set.types_[i] = std::move(type);
  }

  anchor = 0;
  for (Id id : type_ids) {
    auto i = anchor++;
    if (id.empty()) continue;
    RETURN_NOT_OK(set.impl_->CheckHasUri(id.uri));

    auto type = registry->GetType(id);
    if (type == nullptr) {
      return Status::Invalid("Type not found");
    }
    set.types_[i] = std::move(type);
  }

  set.type_ids_ = std::move(type_ids);
  set.type_variation_ids_ = std::move(type_variation_ids);

  return std::move(set);
}

ExtensionSet::~ExtensionSet() = default;

uint32_t ExtensionSet::EncodeTypeVariation(Id id, const std::shared_ptr<DataType>& type) {
  return impl_->EncodeTypeOrTypeVariation<Impl::kTypeVariation>(id, type, this);
}

uint32_t ExtensionSet::EncodeType(Id id, const std::shared_ptr<DataType>& type) {
  return impl_->EncodeTypeOrTypeVariation<Impl::kType>(id, type, this);
}

ExtensionIdRegistry* default_extension_id_registry() {
  static struct : ExtensionIdRegistry {
    struct TypeHashEq {
      size_t operator()(const std::shared_ptr<DataType>& type) const {
        return type->Hash();
      }
      bool operator()(const std::shared_ptr<DataType>& l,
                      const std::shared_ptr<DataType>& r) const {
        return *l == *r;
      }
    };

    util::optional<Id> GetTypeVariation(
        const std::shared_ptr<DataType>& type) const override {
      auto it = type_variations_.find(type);
      if (it == type_variations_.end()) return {};
      return it->second;
    }
    std::shared_ptr<DataType> GetTypeVariation(Id) const override { return nullptr; }
    Status RegisterTypeVariation(Id id, const std::shared_ptr<DataType>& type) override {
      if (type_variations_.emplace(type, id).second) return Status::OK();
      return Status::Invalid("TypeVariation id was already registered");
    }

    util::optional<Id> GetType(const std::shared_ptr<DataType>&) const override {
      return {};
    }
    std::shared_ptr<DataType> GetType(Id) const override { return nullptr; }
    Status RegisterType(Id, const std::shared_ptr<DataType>&) override {
      return Status::NotImplemented("FIXME");
    }

    std::unordered_map<std::shared_ptr<DataType>, Id, TypeHashEq, TypeHashEq>
        type_variations_, types_;
  } impl_;

  constexpr util::string_view kArrowExtTypesUrl =
      "https://github.com/apache/arrow/blob/master/format/substrait/extension_types.yaml";

  DCHECK_OK(impl_.RegisterTypeVariation({kArrowExtTypesUrl, "null"}, null()));

  return &impl_;
}

}  // namespace engine
}  // namespace arrow
