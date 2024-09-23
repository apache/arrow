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

#include "arrow/json/rapidjson_defs.h"  // IWYU pragma: keep
#include "arrow/util/logging.h"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/writer.h>

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
  rapidjson::Document document;
  document.SetObject();
  rapidjson::Document::AllocatorType& allocator = document.GetAllocator();

  rapidjson::Value type_name(rapidjson::StringRef(type_name_));
  document.AddMember(rapidjson::Value("type_name", allocator), type_name, allocator);
  rapidjson::Value vendor_name(rapidjson::StringRef(vendor_name_));
  document.AddMember(rapidjson::Value("vendor_name", allocator), vendor_name, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  document.Accept(writer);
  return buffer.GetString();
}

Result<std::shared_ptr<DataType>> OpaqueType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized_data) const {
  rapidjson::Document document;
  const auto& parsed = document.Parse(serialized_data.data(), serialized_data.length());
  if (parsed.HasParseError()) {
    return Status::Invalid("Invalid serialized JSON data for OpaqueType: ",
                           rapidjson::GetParseError_En(parsed.GetParseError()), ": ",
                           serialized_data);
  } else if (!document.IsObject()) {
    return Status::Invalid("Invalid serialized JSON data for OpaqueType: not an object");
  }
  if (!document.HasMember("type_name")) {
    return Status::Invalid(
        "Invalid serialized JSON data for OpaqueType: missing type_name");
  } else if (!document.HasMember("vendor_name")) {
    return Status::Invalid(
        "Invalid serialized JSON data for OpaqueType: missing vendor_name");
  }

  const auto& type_name = document["type_name"];
  const auto& vendor_name = document["vendor_name"];
  if (!type_name.IsString()) {
    return Status::Invalid(
        "Invalid serialized JSON data for OpaqueType: type_name is not a string");
  } else if (!vendor_name.IsString()) {
    return Status::Invalid(
        "Invalid serialized JSON data for OpaqueType: vendor_name is not a string");
  }

  return opaque(std::move(storage_type), type_name.GetString(), vendor_name.GetString());
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
