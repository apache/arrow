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

#include <thread>

#include <arrow/array.h>
#include <arrow/extension_type.h>
#include <arrow/type.h>

// A wrapper around arrow::ExtensionType that allows R to register extension
// types whose Deserialize, ExtensionEquals, and Serialize methods are
// in meanintfully handled at the R level. At the C++ level, the type is
// already serialized to minimize calls to R from C++.
//
// Using a std::shared_ptr<> to wrap a cpp11::sexp type is unusual, but we
// need it here to avoid calling the copy constructor from another thread,
// since this might call into the R API. If we don't do this, we get crashes
// when reading a multi-file Dataset.
class RExtensionType : public arrow::ExtensionType {
 public:
  RExtensionType(const std::shared_ptr<arrow::DataType> storage_type,
                 std::string extension_name, std::string extension_metadata,
                 std::shared_ptr<cpp11::environment> r6_class,
                 std::thread::id creation_thread)
      : arrow::ExtensionType(storage_type),
        extension_name_(extension_name),
        extension_metadata_(extension_metadata),
        r6_class_(r6_class),
        creation_thread_(creation_thread) {}

  std::string extension_name() const { return extension_name_; }

  bool ExtensionEquals(const arrow::ExtensionType& other) const;

  std::shared_ptr<arrow::Array> MakeArray(std::shared_ptr<arrow::ArrayData> data) const;

  arrow::Result<std::shared_ptr<arrow::DataType>> Deserialize(
      std::shared_ptr<arrow::DataType> storage_type,
      const std::string& serialized_data) const;

  std::string Serialize() const { return extension_metadata_; }

  std::string ToString() const;

  std::unique_ptr<RExtensionType> Clone() const;

  cpp11::environment r6_class() const { return *r6_class_; }

  cpp11::environment r6_instance(std::shared_ptr<arrow::DataType> storage_type,
                                 const std::string& serialized_data) const;

  cpp11::environment r6_instance() const {
    return r6_instance(storage_type(), Serialize());
  }

 private:
  std::string extension_name_;
  std::string extension_metadata_;
  std::string cached_to_string_;
  std::shared_ptr<cpp11::environment> r6_class_;
  std::thread::id creation_thread_;

  arrow::Status assert_r_thread() const {
    if (std::this_thread::get_id() == creation_thread_) {
      return arrow::Status::OK();
    } else {
      return arrow::Status::ExecutionError("RExtensionType <", extension_name_,
                                           "> attempted to call into R ",
                                           "from a non-R thread");
    }
  }
};

bool RExtensionType::ExtensionEquals(const arrow::ExtensionType& other) const {
  // Avoid materializing the R6 instance if at all possible, since this is slow
  // and in some cases not possible due to threading
  if (other.extension_name() != extension_name()) {
    return false;
  }

  if (other.Serialize() == Serialize()) {
    return true;
  }

  // With any ambiguity, we need to materialize the R6 instance and call its
  // ExtensionEquals method. We can't do this on the non-R thread.
  arrow::Status is_r_thread = assert_r_thread();
  if (!assert_r_thread().ok()) {
    throw std::runtime_error(is_r_thread.message());
  }

  cpp11::environment instance = r6_instance();
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
  std::unique_ptr<RExtensionType> cloned = Clone();
  new_data->type = std::shared_ptr<RExtensionType>(cloned.release());
  return std::make_shared<arrow::ExtensionArray>(new_data);
}

arrow::Result<std::shared_ptr<arrow::DataType>> RExtensionType::Deserialize(
    std::shared_ptr<arrow::DataType> storage_type,
    const std::string& serialized_data) const {
  std::unique_ptr<RExtensionType> cloned = Clone();
  cloned->storage_type_ = storage_type;
  cloned->extension_metadata_ = serialized_data;

  // We probably should create an ephemeral R6 instance here, which will call
  // the R6 instance's .Deserialize() method, possibly erroring when the metadata is
  // invalid or the deserialized values are invalid. When there is an error it will be
  // confusing, since it will only occur when the result surfaces to R
  // (which might be much later). Unfortunately, the Deserialize() method gets
  // called from other threads frequently (e.g., when reading a multi-file Dataset),
  // and we get crashes if we try this. As a compromise, we call this method when we can
  // to maximize the likelihood an error is surfaced.
  if (assert_r_thread().ok()) {
    cloned->r6_instance();
  }

  return std::shared_ptr<RExtensionType>(cloned.release());
}

std::string RExtensionType::ToString() const {
  // In case this gets called from another thread
  if (!assert_r_thread().ok()) {
    return ExtensionType::ToString();
  }

  cpp11::environment instance = r6_instance();
  cpp11::function instance_ToString(instance[".ToString"]);
  cpp11::sexp result = instance_ToString();
  return cpp11::as_cpp<std::string>(result);
}

std::unique_ptr<RExtensionType> RExtensionType::Clone() const {
  RExtensionType* ptr = new RExtensionType(
      storage_type(), extension_name_, extension_metadata_, r6_class_, creation_thread_);
  return std::unique_ptr<RExtensionType>(ptr);
}

cpp11::environment RExtensionType::r6_instance(
    std::shared_ptr<arrow::DataType> storage_type,
    const std::string& serialized_data) const {
  // This is a version of to_r6<>() that is a more direct route to creating the object.
  // This is done to avoid circular calls, since to_r6<>() has to go through
  // ExtensionType$new(), which then calls back to C++ to get r6_class_ to then
  // return the correct subclass.
  std::unique_ptr<RExtensionType> cloned = Clone();
  cpp11::external_pointer<std::shared_ptr<RExtensionType>> xp(
      new std::shared_ptr<RExtensionType>(cloned.release()));

  cpp11::function r6_class_new(r6_class()["new"]);
  return r6_class_new(xp);
}

// [[arrow::export]]
cpp11::environment ExtensionType__initialize(
    const std::shared_ptr<arrow::DataType>& storage_type, std::string extension_name,
    cpp11::raws extension_metadata, cpp11::environment r6_class) {
  std::string metadata_string(extension_metadata.begin(), extension_metadata.end());
  auto r6_class_shared = std::make_shared<cpp11::environment>(r6_class);
  RExtensionType cpp_type(storage_type, extension_name, metadata_string, r6_class_shared,
                          std::this_thread::get_id());
  return cpp_type.r6_instance();
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
cpp11::environment ExtensionType__r6_class(
    const std::shared_ptr<arrow::ExtensionType>& type) {
  auto r_type =
      arrow::internal::checked_pointer_cast<RExtensionType, arrow::ExtensionType>(type);
  return r_type->r6_class();
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
