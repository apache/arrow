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

#include "./arrow_types.h"

#if defined(ARROW_R_WITH_ARROW)

#include <arrow/type.h>
#include <arrow/extension_type.h>
#include <arrow/array.h>


class RExtensionType: public arrow::ExtensionType {
public:
  RExtensionType(const std::shared_ptr<arrow::DataType> storage_type,
                 std::string extension_name, std::string extension_metadata,
                 cpp11::environment r6_type_generator,
                 cpp11::environment r6_array_generator)
    : arrow::ExtensionType(storage_type),
      extension_name_(extension_name),
      extension_metadata_(extension_metadata),
      r6_type_generator_(r6_type_generator),
      r6_array_generator_(r6_array_generator) {}

  std::string extension_name() const { return extension_name_; }
  bool ExtensionEquals(const arrow::ExtensionType &other) const;
  std::shared_ptr<arrow::Array> MakeArray(std::shared_ptr<arrow::ArrayData> data) const;
  arrow::Result<std::shared_ptr<arrow::DataType>> Deserialize(
      std::shared_ptr<arrow::DataType> storage_type,
      const std::string &serialized_data) const;
  std::string Serialize() const { return extension_metadata_; }

  std::shared_ptr<RExtensionType> Clone() const;

private:
  std::string extension_name_;
  std::string extension_metadata_;
  cpp11::environment r6_type_generator_;
  cpp11::environment r6_array_generator_;
};

bool RExtensionType::ExtensionEquals(const arrow::ExtensionType &other) const {
  if (other.extension_name() != extension_name()) {
    return false;
  }

  if (other.Serialize() != Serialize()) {
    return false;
  }

  return true;
}

std::shared_ptr<arrow::Array> RExtensionType::MakeArray(std::shared_ptr<arrow::ArrayData> data) const {
  std::shared_ptr<arrow::ArrayData> new_data = data->Copy();
  new_data->type = Clone();
  return std::make_shared<arrow::ExtensionArray>(new_data);
}

arrow::Result<std::shared_ptr<arrow::DataType>> RExtensionType::Deserialize(
    std::shared_ptr<arrow::DataType> storage_type,
    const std::string &serialized_data) const {
  try {
    cpp11::function make_extension_type(cpp11::package("arrow")["MakeExtensionType"]);
    cpp11::sexp storage_type_r6 = cpp11::to_r6<arrow::DataType>(storage_type);
    cpp11::writable::raws serialized_data_raw(serialized_data);

    cpp11::sexp result = make_extension_type(
      storage_type_r6,
      extension_name(),
      serialized_data_raw,
      r6_type_generator_,
      r6_array_generator_
    );

    auto ptr = arrow::r::r6_to_pointer<std::shared_ptr<arrow::DataType>*>(result);
    return *ptr;
  } catch(std::exception& e) {
    return arrow::Status::UnknownError(e.what());
  }
}

std::shared_ptr<RExtensionType> RExtensionType::Clone() const {
  return std::make_shared<RExtensionType>(
    storage_type(),
    extension_name_,
    extension_metadata_,
    r6_type_generator_,
    r6_array_generator_
  );
}

// [[arrow::export]]
cpp11::sexp ExtensionType__initialize(
    const std::shared_ptr<arrow::DataType>& storage_type,
    std::string extension_name,
    cpp11::raws extension_metadata,
    cpp11::environment r6_type_generator,
    cpp11::environment r6_array_generator
) {
  cpp11::function constructor(r6_type_generator["new"]);
  std::string metadata_string(extension_metadata.begin(), extension_metadata.end());
  auto shared_ptr_ptr = new std::shared_ptr<RExtensionType>(
    new RExtensionType(
        storage_type,
        extension_name,
        metadata_string,
        r6_type_generator,
        r6_array_generator));
  auto external_ptr = cpp11::external_pointer<std::shared_ptr<RExtensionType>>(shared_ptr_ptr);
  return constructor(external_ptr);
}

// [[arrow::export]]
std::string ExtensionType__extension_name(const std::shared_ptr<arrow::ExtensionType>& type) {
  return type->extension_name();
}

// [[arrow::export]]
cpp11::raws ExtensionType__Serialize(const std::shared_ptr<arrow::ExtensionType>& type) {
  std::string serialized_string = type->Serialize();
  cpp11::writable::raws bytes(serialized_string.begin(), serialized_string.end());
  return bytes;
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> ExtensionType__storage_type(const std::shared_ptr<arrow::ExtensionType>& type) {
  return type->storage_type();
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> ExtensionType__MakeArray(const std::shared_ptr<arrow::ExtensionType>& type,
                                                       const std::shared_ptr<arrow::ArrayData>& data) {
  return type->MakeArray(data);
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> ExtensionArray__storage(const std::shared_ptr<arrow::ExtensionArray>& array) {
  return array->storage();
}

// [[arrow::export]]
void arrow__RegisterRExtensionType(const std::shared_ptr<arrow::DataType>& type) {
  auto ext_type = std::dynamic_pointer_cast<arrow::ExtensionType>(type);
  StopIfNotOk(arrow::RegisterExtensionType(ext_type));
}

// [[arrow::export]]
void arrow__UnregisterRExtensionType(std::string type_name) {
  StopIfNotOk(arrow::UnregisterExtensionType(type_name));
}

#endif
