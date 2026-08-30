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

#include "arrow/extension/opaque.h"

#include <sstream>

#include "arrow/json/json_writer_internal.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/simdjson_internal.h"

#include <simdjson.h>

using ::arrow::json::JsonWriter;

namespace arrow::extension {

std::string OpaqueType::ToString(bool show_metadata) const {
  std::stringstream ss;
  ss << "extension<" << this->extension_name()
     << "[storage_type=" << storage_type_->ToString(show_metadata)
     << ", type_name=" << type_name_ << ", vendor_name=" << vendor_name_ << "]>";
  return ss.str();
}

bool OpaqueType::ExtensionEquals(const ExtensionType& other) const {
  if (extension_name() != other.extension_name()) {
    return false;
  }
  const auto& opaque = internal::checked_cast<const OpaqueType&>(other);
  return storage_type()->Equals(*opaque.storage_type()) &&
         type_name() == opaque.type_name() && vendor_name() == opaque.vendor_name();
}

std::string OpaqueType::Serialize() const {
  JsonWriter writer;

  writer.StartObject();

  writer.StringField("type_name", type_name_);
  writer.StringField("vendor_name", vendor_name_);

  writer.EndObject();

  Result<std::string_view> json = writer.GetString();
  // can only fail in OutOfMemory scenarios
  ARROW_CHECK_OK(json.status());
  return std::string(*json);
}

Result<std::shared_ptr<DataType>> OpaqueType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized_data) const {
  simdjson::padded_string padded_json(serialized_data);
  simdjson::ondemand::parser parser;

  ARROW_ASSIGN_OR_RAISE(auto document,
                        internal::ResolveSimdjsonResult(parser.iterate(padded_json),
                                                        "Failed to parse JSON"));

  ARROW_ASSIGN_OR_RAISE(auto object,
                        internal::ResolveSimdjsonResult(document.get_object(),
                                                        "Failed to get JSON object"));

  std::string type_name;
  std::string vendor_name;
  bool has_type_name = false;
  bool has_vendor_name = false;

  for (auto field_result : object) {
    ARROW_ASSIGN_OR_RAISE(auto field, internal::ResolveSimdjsonResult(
                                          field_result, "Failed to iterate JSON object"));

    ARROW_ASSIGN_OR_RAISE(
        auto key, internal::ResolveSimdjsonResult(field.unescaped_key(),
                                                  "Failed to get JSON object key"));

    auto value = field.value();

    if (key == "type_name") {
      has_type_name = true;

      ARROW_ASSIGN_OR_RAISE(auto type,
                            internal::ResolveSimdjsonResult(
                                value.type(), "Failed to determine type_name JSON type"));

      if (type != simdjson::ondemand::json_type::string) {
        return Status::Invalid(
            "Invalid serialized JSON data for OpaqueType: type_name is not a string");
      }

      ARROW_ASSIGN_OR_RAISE(
          auto name,
          internal::ResolveSimdjsonResult(value.get_string(), "Failed to get type_name"));
      type_name = std::string(name);

    } else if (key == "vendor_name") {
      has_vendor_name = true;

      ARROW_ASSIGN_OR_RAISE(
          auto type, internal::ResolveSimdjsonResult(
                         value.type(), "Failed to determine vendor_name JSON type"));

      if (type != simdjson::ondemand::json_type::string) {
        return Status::Invalid(
            "Invalid serialized JSON data for OpaqueType: vendor_name is not a string");
      }

      ARROW_ASSIGN_OR_RAISE(auto name,
                            internal::ResolveSimdjsonResult(value.get_string(),
                                                            "Failed to get vendor_name"));
      vendor_name = std::string(name);
    }
  }

  if (!has_type_name) {
    return Status::Invalid(
        "Invalid serialized JSON data for OpaqueType: missing type_name");
  }

  if (!has_vendor_name) {
    return Status::Invalid(
        "Invalid serialized JSON data for OpaqueType: missing vendor_name");
  }

  return opaque(std::move(storage_type), std::move(type_name), std::move(vendor_name));
}

std::shared_ptr<Array> OpaqueType::MakeArray(std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("arrow.opaque",
            internal::checked_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<OpaqueArray>(data);
}

std::shared_ptr<DataType> opaque(std::shared_ptr<DataType> storage_type,
                                 std::string type_name, std::string vendor_name) {
  return std::make_shared<OpaqueType>(std::move(storage_type), std::move(type_name),
                                      std::move(vendor_name));
}

}  // namespace arrow::extension
