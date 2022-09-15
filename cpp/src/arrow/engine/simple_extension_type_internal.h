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

#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/extension_type.h"
#include "arrow/util/logging.h"
#include "arrow/util/reflection_internal.h"
#include "arrow/util/string.h"

namespace arrow {
namespace engine {

/// \brief A helper class for creating simple extension types
///
/// Extension types can be parameterized by flat structs
///
/// Each item in the struct will be serialized and deserialized using
/// the STL insertion and extraction operators (i.e. << and >>).
///
/// Note: The serialization is a very barebones JSON-like format and
/// probably shouldn't be hand-edited

template <const util::string_view& kExtensionName, typename Params,
          typename ParamsProperties, const ParamsProperties* kProperties,
          std::shared_ptr<DataType> GetStorage(const Params&)>
class SimpleExtensionType : public ExtensionType {
 public:
  explicit SimpleExtensionType(std::shared_ptr<DataType> storage_type, Params params = {})
      : ExtensionType(std::move(storage_type)), params_(std::move(params)) {}

  static std::shared_ptr<DataType> Make(Params params) {
    auto storage_type = GetStorage(params);
    return std::make_shared<SimpleExtensionType>(std::move(storage_type),
                                                 std::move(params));
  }

  /// \brief Returns the parameters object for the type
  ///
  /// If the type is not an instance of this extension type then nullptr will be returned
  static const Params* GetIf(const DataType& type) {
    if (type.id() != Type::EXTENSION) return nullptr;

    const auto& ext_type = ::arrow::internal::checked_cast<const ExtensionType&>(type);
    if (ext_type.extension_name() != kExtensionName) return nullptr;

    return &::arrow::internal::checked_cast<const SimpleExtensionType&>(type).params_;
  }

  std::string extension_name() const override { return kExtensionName.to_string(); }

  std::string ToString() const override { return "extension<" + this->Serialize() + ">"; }

  /// \brief A comparator which returns true iff all parameter properties are equal
  struct ExtensionEqualsImpl {
    ExtensionEqualsImpl(const Params& l, const Params& r) : left_(l), right_(r) {
      kProperties->ForEach(*this);
    }

    template <typename Property>
    void operator()(const Property& prop, size_t i) {
      equal_ &= prop.get(left_) == prop.get(right_);
    }

    const Params& left_;
    const Params& right_;
    bool equal_ = true;
  };

  bool ExtensionEquals(const ExtensionType& other) const override {
    if (kExtensionName != other.extension_name()) return false;
    const auto& other_params = static_cast<const SimpleExtensionType&>(other).params_;
    return ExtensionEqualsImpl(params_, other_params).equal_;
  }

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override {
    DCHECK_EQ(data->type->id(), Type::EXTENSION);
    DCHECK_EQ(static_cast<const ExtensionType&>(*data->type).extension_name(),
              kExtensionName);
    return std::make_shared<ExtensionArray>(data);
  }

  struct DeserializeImpl {
    explicit DeserializeImpl(util::string_view repr) {
      Init(kExtensionName, repr, kProperties->size());
      kProperties->ForEach(*this);
    }

    void Fail() { params_ = std::nullopt; }

    void Init(util::string_view class_name, util::string_view repr,
              size_t num_properties) {
      if (!repr.starts_with(class_name)) return Fail();

      repr = repr.substr(class_name.size());
      if (repr.empty()) return Fail();
      if (repr.front() != '{') return Fail();
      if (repr.back() != '}') return Fail();

      repr = repr.substr(1, repr.size() - 2);
      members_ = ::arrow::internal::SplitString(repr, ',');
      if (members_.size() != num_properties) return Fail();
    }

    template <typename Property>
    void operator()(const Property& prop, size_t i) {
      if (!params_) return;

      auto first_colon = members_[i].find_first_of(':');
      if (first_colon == util::string_view::npos) return Fail();

      auto name = members_[i].substr(0, first_colon);
      if (name != prop.name()) return Fail();

      auto value_repr = members_[i].substr(first_colon + 1);
      typename Property::Type value;
      try {
        std::stringstream ss(value_repr.to_string());
        ss >> value;
        if (!ss.eof()) return Fail();
      } catch (...) {
        return Fail();
      }
      prop.set(&*params_, std::move(value));
    }

    std::optional<Params> params_;
    std::vector<util::string_view> members_;
  };
  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized) const override {
    if (auto params = DeserializeImpl(serialized).params_) {
      if (!storage_type->Equals(GetStorage(*params))) {
        return Status::Invalid("Invalid storage type for ", kExtensionName, ": ",
                               storage_type->ToString(), " (expected ",
                               GetStorage(*params)->ToString(), ")");
      }

      return std::make_shared<SimpleExtensionType>(std::move(storage_type),
                                                   std::move(*params));
    }

    return Status::Invalid("Could not parse parameters for extension type ",
                           extension_name(), " from ", serialized);
  }

  struct SerializeImpl {
    explicit SerializeImpl(const Params& params)
        : params_(params), members_(kProperties->size()) {
      kProperties->ForEach(*this);
    }

    template <typename Property>
    void operator()(const Property& prop, size_t i) {
      std::stringstream ss;
      ss << prop.name() << ":" << prop.get(params_);
      members_[i] = ss.str();
    }

    std::string Finish() {
      return kExtensionName.to_string() + "{" +
             ::arrow::internal::JoinStrings(members_, ",") + "}";
    }

    const Params& params_;
    std::vector<std::string> members_;
  };
  std::string Serialize() const override { return SerializeImpl(params_).Finish(); }

 private:
  Params params_;
};

}  // namespace engine
}  // namespace arrow
