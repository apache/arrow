// Licensed to the Apache Software Foundation (ASF) under one
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// or more contributor license agreements.  See the NOTICE file
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

#include <arrow/array.h>
#include <arrow/extension_type.h>
#include <arrow/type.h>

// A wrapper around arrow::ExtensionType that allows R to register extension
// types whose Deserialize, ExtensionEquals, and Serialize methods are
// in meaningfully handled at the R level. At the C++ level, the type is
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
                 std::shared_ptr<cpp11::environment> r6_class)
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

  std::string ToString() const;

  cpp11::sexp Convert(const std::shared_ptr<arrow::ChunkedArray>& array) const;

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
};
