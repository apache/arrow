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

#include <cstdint>
#include <string>
#include <string_view>

#include "arrow/engine/simple_extension_type_internal.h"
#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "arrow/util/reflection_internal.h"

namespace arrow {

using internal::DataMember;
using internal::MakeProperties;

namespace engine {
namespace {

constexpr std::string_view kUuidExtensionName = "uuid";
struct UuidExtensionParams {};
std::shared_ptr<DataType> UuidGetStorage(const UuidExtensionParams&) {
  return fixed_size_binary(16);
}
static auto kUuidExtensionParamsProperties = MakeProperties();

using UuidType = SimpleExtensionType<kUuidExtensionName, UuidExtensionParams,
                                     decltype(kUuidExtensionParamsProperties),
                                     &kUuidExtensionParamsProperties, UuidGetStorage>;

constexpr std::string_view kFixedCharExtensionName = "fixed_char";
struct FixedCharExtensionParams {
  int32_t length;
};
std::shared_ptr<DataType> FixedCharGetStorage(const FixedCharExtensionParams& params) {
  return fixed_size_binary(params.length);
}
static auto kFixedCharExtensionParamsProperties =
    MakeProperties(DataMember("length", &FixedCharExtensionParams::length));

using FixedCharType =
    SimpleExtensionType<kFixedCharExtensionName, FixedCharExtensionParams,
                        decltype(kFixedCharExtensionParamsProperties),
                        &kFixedCharExtensionParamsProperties, FixedCharGetStorage>;

constexpr std::string_view kVarCharExtensionName = "varchar";
struct VarCharExtensionParams {
  int32_t length;
};
std::shared_ptr<DataType> VarCharGetStorage(const VarCharExtensionParams&) {
  return utf8();
}
static auto kVarCharExtensionParamsProperties =
    MakeProperties(DataMember("length", &VarCharExtensionParams::length));

using VarCharType =
    SimpleExtensionType<kVarCharExtensionName, VarCharExtensionParams,
                        decltype(kVarCharExtensionParamsProperties),
                        &kVarCharExtensionParamsProperties, VarCharGetStorage>;

constexpr std::string_view kIntervalYearExtensionName = "interval_year";
struct IntervalYearExtensionParams {};
std::shared_ptr<DataType> IntervalYearGetStorage(const IntervalYearExtensionParams&) {
  return fixed_size_list(int32(), 2);
}
static auto kIntervalYearExtensionParamsProperties = MakeProperties();

using IntervalYearType =
    SimpleExtensionType<kIntervalYearExtensionName, IntervalYearExtensionParams,
                        decltype(kIntervalYearExtensionParamsProperties),
                        &kIntervalYearExtensionParamsProperties, IntervalYearGetStorage>;

constexpr std::string_view kIntervalDayExtensionName = "interval_day";
struct IntervalDayExtensionParams {};
std::shared_ptr<DataType> IntervalDayGetStorage(const IntervalDayExtensionParams&) {
  return fixed_size_list(int32(), 2);
}
static auto kIntervalDayExtensionParamsProperties = MakeProperties();

using IntervalDayType =
    SimpleExtensionType<kIntervalDayExtensionName, IntervalDayExtensionParams,
                        decltype(kIntervalDayExtensionParamsProperties),
                        &kIntervalDayExtensionParamsProperties, IntervalDayGetStorage>;

}  // namespace

std::shared_ptr<DataType> uuid() { return UuidType::Make({}); }

std::shared_ptr<DataType> fixed_char(int32_t length) {
  return FixedCharType::Make({length});
}

std::shared_ptr<DataType> varchar(int32_t length) { return VarCharType::Make({length}); }

std::shared_ptr<DataType> interval_year() { return IntervalYearType::Make({}); }

std::shared_ptr<DataType> interval_day() { return IntervalDayType::Make({}); }

bool UnwrapUuid(const DataType& t) {
  if (UuidType::GetIf(t)) {
    return true;
  }
  return false;
}

std::optional<int32_t> UnwrapFixedChar(const DataType& t) {
  if (auto params = FixedCharType::GetIf(t)) {
    return params->length;
  }
  return std::nullopt;
}

std::optional<int32_t> UnwrapVarChar(const DataType& t) {
  if (auto params = VarCharType::GetIf(t)) {
    return params->length;
  }
  return std::nullopt;
}

bool UnwrapIntervalYear(const DataType& t) {
  if (IntervalYearType::GetIf(t)) {
    return true;
  }
  return false;
}

bool UnwrapIntervalDay(const DataType& t) {
  if (IntervalDayType::GetIf(t)) {
    return true;
  }
  return false;
}

}  // namespace engine
}  // namespace arrow
