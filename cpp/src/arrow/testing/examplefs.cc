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

#include <any>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/filesystem_library.h"
#include "arrow/result.h"
#include "arrow/testing/examplefs.h"
#include "arrow/util/uri.h"

#include <gtest/gtest.h>

namespace arrow::fs {

auto kExampleFileSystemModule = ARROW_REGISTER_FILESYSTEM(
    "example",
    [](const Uri& uri, const FileSystemFactoryOptions& options,
       const io::IOContext& io_context,
       std::string* out_path) -> Result<std::shared_ptr<FileSystem>> {
      constexpr std::string_view kScheme = "example";
      EXPECT_EQ(uri.scheme(), kScheme);
      auto local_uri = "file" + uri.ToString().substr(kScheme.size());
      ARROW_ASSIGN_OR_RAISE(auto fs, FileSystemFromUri(local_uri, io_context, out_path));
      for (const auto& [key, value] : options) {
        EXPECT_TRUE(value.has_value());
        if (key == "example_option_string") {
          if (const auto* s = std::any_cast<std::string>(&value)) {
            if (out_path != nullptr) *out_path += "/" + *s;
          } else {
            ADD_FAILURE() << "example_option_string has wrong type";
          }
        } else if (key == "example_option_int") {
          if (const auto* i = std::any_cast<int>(&value)) {
            if (out_path != nullptr) *out_path += "/" + std::to_string(*i);
          } else {
            ADD_FAILURE() << "example_option_int has wrong type";
          }
        } else if (key == "example_typed_option") {
          if (const auto* opt =
                  std::any_cast<std::shared_ptr<ExampleTypedOption>>(&value)) {
            if (out_path != nullptr) {
              *out_path += "/" + std::to_string((*opt)->value());
            }
          } else {
            ADD_FAILURE() << "example_typed_option has wrong type";
          }
        } else {
          ADD_FAILURE() << "Unexpected option: " << key;
        }
      }
      return fs;
    },
    {});

}  // namespace arrow::fs
