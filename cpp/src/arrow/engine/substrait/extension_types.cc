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

#include "arrow/engine/simple_extension_type_internal.h"
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

}  // namespace

std::shared_ptr<DataType> uuid() { return UuidType::Make({}); }

std::shared_ptr<DataType> fixed_char(int32_t length) {
  return FixedCharType::Make({length});
}

std::shared_ptr<DataType> varchar(int32_t length) { return VarCharType::Make({length}); }

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

}  // namespace engine
}  // namespace arrow
