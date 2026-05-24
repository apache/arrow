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

#include "arrow/extension/range.h"

#include <sstream>
#include <string_view>

#include "arrow/json/rapidjson_defs.h"  // IWYU pragma: keep
#include "arrow/util/logging_internal.h"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/writer.h>

namespace arrow::extension {

namespace {

/// Map RangeClosed -> the JSON string value used in serialization.
std::string_view ClosedToString(RangeClosed closed) {
  switch (closed) {
    case RangeClosed::Left:
      return "left";
    case RangeClosed::Right:
      return "right";
    case RangeClosed::Both:
      return "both";
    case RangeClosed::Neither:
      return "neither";
  }
  // unreachable
  return "right";
}

/// Parse the JSON "closed" string into a RangeClosed enum.
/// Returns an error if the string is not one of the four valid values.
Result<RangeClosed> ClosedFromString(std::string_view s) {
  if (s == "left") return RangeClosed::Left;
  if (s == "right") return RangeClosed::Right;
  if (s == "both") return RangeClosed::Both;
  if (s == "neither") return RangeClosed::Neither;
  return Status::Invalid(
      "Invalid value for RangeType \"closed\" parameter: \"", s,
      "\". Expected one of: \"left\", \"right\", \"both\", \"neither\".");
}

/// Build the storage Struct type for a given value subtype.
std::shared_ptr<DataType> MakeStorageType(const std::shared_ptr<DataType>& value_type,
                                          bool allow_unbounded) {
  // Nullable bounds can represent an unbounded (infinite) endpoint; non-nullable
  // bounds are always finite.
  return struct_({field("lower", value_type, allow_unbounded),
                  field("upper", value_type, allow_unbounded)});
}

}  // namespace

// ---------------------------------------------------------------------------
// RangeType

std::shared_ptr<DataType> RangeType::value_type() const {
  // storage_type() is a struct with two fields; both share the same type.
  return internal::checked_cast<const StructType&>(*storage_type()).field(0)->type();
}

std::string RangeType::ToString(bool show_metadata) const {
  std::stringstream ss;
  ss << "extension<" << this->extension_name()
     << "[value_type=" << value_type()->ToString(show_metadata)
     << ", closed=" << ClosedToString(closed_) << "]>";
  return ss.str();
}

bool RangeType::ExtensionEquals(const ExtensionType& other) const {
  if (extension_name() != other.extension_name()) {
    return false;
  }
  const auto& other_range = internal::checked_cast<const RangeType&>(other);
  return storage_type()->Equals(*other_range.storage_type()) &&
         closed_ == other_range.closed_;
}

std::string RangeType::Serialize() const {
  rapidjson::Document document;
  document.SetObject();
  rapidjson::Document::AllocatorType& allocator = document.GetAllocator();

  auto closed_str = ClosedToString(closed_);
  rapidjson::Value closed_value(
      closed_str.data(), static_cast<rapidjson::SizeType>(closed_str.size()), allocator);
  document.AddMember(rapidjson::Value("closed", allocator), closed_value, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  document.Accept(writer);
  return buffer.GetString();
}

Result<std::shared_ptr<DataType>> RangeType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized_data) const {
  // Validate storage type structure.
  if (storage_type->id() != Type::STRUCT) {
    return Status::Invalid("RangeType storage type must be a Struct, got ",
                           storage_type->ToString());
  }
  const auto& struct_type = internal::checked_cast<const StructType&>(*storage_type);
  if (struct_type.num_fields() != 2) {
    return Status::Invalid("RangeType storage Struct must have exactly 2 fields, got ",
                           struct_type.num_fields());
  }
  const auto& lower_field = struct_type.field(0);
  const auto& upper_field = struct_type.field(1);
  if (lower_field->name() != "lower") {
    return Status::Invalid(
        "RangeType storage Struct field 0 must be named \"lower\", got \"",
        lower_field->name(), "\"");
  }
  if (upper_field->name() != "upper") {
    return Status::Invalid(
        "RangeType storage Struct field 1 must be named \"upper\", got \"",
        upper_field->name(), "\"");
  }
  if (!lower_field->type()->Equals(*upper_field->type())) {
    return Status::Invalid(
        "RangeType storage Struct fields \"lower\" and \"upper\" must have the same "
        "type, got \"",
        lower_field->type()->ToString(), "\" and \"", upper_field->type()->ToString(),
        "\"");
  }

  // Parse the required "closed" parameter from JSON metadata. The closedness
  // is not defaulted on the wire: empty metadata or a missing key is invalid.
  if (serialized_data.empty()) {
    return Status::Invalid(
        "RangeType metadata must be a JSON object with a required \"closed\" key, "
        "got an empty string");
  }
  rapidjson::Document document;
  const auto& parsed = document.Parse(serialized_data.data(), serialized_data.length());
  if (parsed.HasParseError()) {
    return Status::Invalid("Invalid serialized JSON data for RangeType: ",
                           rapidjson::GetParseError_En(parsed.GetParseError()), ": ",
                           serialized_data);
  }
  if (!document.IsObject()) {
    return Status::Invalid("Invalid serialized JSON data for RangeType: not an object");
  }
  if (!document.HasMember("closed")) {
    return Status::Invalid("RangeType metadata is missing the required \"closed\" key: ",
                           serialized_data);
  }
  const auto& closed_val = document["closed"];
  if (!closed_val.IsString()) {
    return Status::Invalid(
        "Invalid serialized JSON data for RangeType: \"closed\" is not a string");
  }
  ARROW_ASSIGN_OR_RAISE(RangeClosed closed,
                        ClosedFromString(std::string_view(closed_val.GetString(),
                                                          closed_val.GetStringLength())));

  return std::make_shared<RangeType>(std::move(storage_type), closed);
}

std::shared_ptr<Array> RangeType::MakeArray(std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("arrow.range",
            internal::checked_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<RangeArray>(data);
}

Result<std::shared_ptr<DataType>> RangeType::Make(std::shared_ptr<DataType> value_type,
                                                  RangeClosed closed,
                                                  bool allow_unbounded) {
  auto storage = MakeStorageType(value_type, allow_unbounded);
  return std::make_shared<RangeType>(std::move(storage), closed);
}

// ---------------------------------------------------------------------------
// Free factory function

std::shared_ptr<DataType> range(std::shared_ptr<DataType> value_type, RangeClosed closed,
                                bool allow_unbounded) {
  auto result = RangeType::Make(std::move(value_type), closed, allow_unbounded);
  ARROW_CHECK_OK(result.status());
  return std::move(result).ValueOrDie();
}

}  // namespace arrow::extension
