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

#include <arrow/array.h>
#include <arrow/extension_type.h>
#include <arrow/type.h>

// A wrapper around arrow::ExtensionType that allows R to register extension
// types whose Deserialize, ExtensionEquals, and Sersialize methods are
// in practice handled at the R level.
class RExtensionType : public arrow::ExtensionType {
 public:

  // An instance of RExtensionType already has a copy of its seraialized
  // extension metadata when constructed.
  RExtensionType(const std::shared_ptr<arrow::DataType> storage_type,
                 std::string extension_name, std::string extension_metadata,
                 cpp11::environment r6_class)
      : arrow::ExtensionType(storage_type),
        extension_name_(extension_name),
        extension_metadata_(extension_metadata),
        r6_class_(r6_class) {}

  std::string extension_name() const { return extension_name_; }

  bool ExtensionEquals(const arrow::ExtensionType& other) const;

  std::shared_ptr<arrow::Array> MakeArray(std::shared_ptr<arrow::ArrayData> data) const;

  arrow::Result<std::shared_ptr<arrow::DataType>> Deserialize(
      std::shared_ptr<arrow::DataType> storage_type,
      const std::string& serialized_data) const;

  std::string Serialize() const { return extension_metadata_; }

  std::shared_ptr<RExtensionType> Clone() const;

  cpp11::environment R6Class() { return r6_class_; }

  cpp11::environment to_r6(std::shared_ptr<arrow::DataType> storage_type,
                           const std::string& serialized_data) const;

 private:
  std::string extension_name_;
  std::string extension_metadata_;
  cpp11::environment r6_class_;
};

bool RExtensionType::ExtensionEquals(const arrow::ExtensionType& other) const {
  // Avoid materializing the R6 type if at all possible, since this is slow
  // and in some cases not possible due to threading
  if (other.extension_name() != extension_name()) {
    return false;
  }

  if (other.Serialize() == Serialize()) {
    return true;
  }

  // With any ambiguity, we need to materialize the R6 type and call its
  // ExtensionEquals method.
  cpp11::environment instance = to_r6(storage_type(), Serialize());
  cpp11::function instance_ExtensionEquals(instance[".ExtensionEquals"]);

  std::shared_ptr<DataType> other_shared =
      ValueOrStop(other.Deserialize(other.storage_type(), other.Serialize()));
  cpp11::sexp other_r6 = cpp11::to_r6<DataType>(other_shared, "ExtensionType");

  cpp11::logicals result(instance_ExtensionEquals(other_r6));
  return cpp11::as_cpp<bool>(result);
}

std::shared_ptr<arrow::Array> RExtensionType::MakeArray(
    std::shared_ptr<arrow::ArrayData> data) const {
  std::shared_ptr<arrow::ArrayData> new_data = data->Copy();
  new_data->type = Clone();
  return std::make_shared<arrow::ExtensionArray>(new_data);
}

arrow::Result<std::shared_ptr<arrow::DataType>> RExtensionType::Deserialize(
    std::shared_ptr<arrow::DataType> storage_type,
    const std::string& serialized_data) const {
  try {
    cpp11::environment result = to_r6(storage_type, serialized_data);
    auto ptr = arrow::r::r6_to_pointer<std::shared_ptr<arrow::DataType>*>(result);
    return *ptr;
  } catch (std::exception& e) {
    return arrow::Status::UnknownError(e.what());
  }
}

std::shared_ptr<RExtensionType> RExtensionType::Clone() const {
  return std::make_shared<RExtensionType>(storage_type(), extension_name_,
                                          extension_metadata_, r6_class_);
}

cpp11::environment RExtensionType::to_r6(std::shared_ptr<arrow::DataType> storage_type,
                                         const std::string& serialized_data) const {
  cpp11::function make_extension_type(cpp11::package("arrow")["MakeExtensionType"]);
  cpp11::sexp storage_type_r6 = cpp11::to_r6<arrow::DataType>(storage_type);
  cpp11::writable::raws serialized_data_raw(serialized_data);

  cpp11::sexp result = make_extension_type(storage_type_r6, extension_name(),
                                           serialized_data_raw, r6_class_);

  return result;
}

// [[arrow::export]]
cpp11::sexp ExtensionType__initialize(
    const std::shared_ptr<arrow::DataType>& storage_type, std::string extension_name,
    cpp11::raws extension_metadata, cpp11::environment r6_class) {
  cpp11::function constructor(r6_class["new"]);
  std::string metadata_string(extension_metadata.begin(), extension_metadata.end());
  auto shared_ptr_ptr = new std::shared_ptr<RExtensionType>(new RExtensionType(
      storage_type, extension_name, metadata_string, r6_class));
  auto external_ptr =
      cpp11::external_pointer<std::shared_ptr<RExtensionType>>(shared_ptr_ptr);
  return constructor(external_ptr);
}

// [[arrow::export]]
std::string ExtensionType__extension_name(
    const std::shared_ptr<arrow::ExtensionType>& type) {
  return type->extension_name();
}

// [[arrow::export]]
cpp11::raws ExtensionType__Serialize(const std::shared_ptr<arrow::ExtensionType>& type) {
  std::string serialized_string = type->Serialize();
  cpp11::writable::raws bytes(serialized_string.begin(), serialized_string.end());
  return bytes;
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> ExtensionType__storage_type(
    const std::shared_ptr<arrow::ExtensionType>& type) {
  return type->storage_type();
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> ExtensionType__MakeArray(
    const std::shared_ptr<arrow::ExtensionType>& type,
    const std::shared_ptr<arrow::ArrayData>& data) {
  return type->MakeArray(data);
}

// [[arrow::export]]
cpp11::environment ExtensionType__r6_class(const std::shared_ptr<arrow::ExtensionType>& type) {
  auto r_type = arrow::internal::checked_pointer_cast<RExtensionType, arrow::ExtensionType>(type);
  return r_type->R6Class();
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> ExtensionArray__storage(
    const std::shared_ptr<arrow::ExtensionArray>& array) {
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
