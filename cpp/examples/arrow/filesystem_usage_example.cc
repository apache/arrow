// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <iostream>

#include <arrow/filesystem/filesystem.h>
#include <arrow/result.h>

namespace fs = arrow::fs;

// Demonstrate dynamically loading a user-defined Arrow FileSystem

arrow::Status Execute() {
  ARROW_RETURN_NOT_OK(arrow::fs::LoadFileSystemFactories(FILESYSTEM_EXAMPLE_LIBPATH));

  std::string uri = "example:///example_file";
  std::cout << "Uri: " << uri << std::endl;

  std::string path;
  ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(uri, &path));
  std::cout << "Path: " << path << std::endl;

  fs::FileSelector sel;
  sel.base_dir = "/";
  ARROW_ASSIGN_OR_RAISE(auto infos, fs->GetFileInfo(sel));

  std::cout << "Root directory contains:" << std::endl;
  for (const auto& info : infos) {
    std::cout << "- " << info << std::endl;
  }
  return arrow::Status::OK();
}

int main() {
  auto status = Execute();
  if (!status.ok()) {
    std::cerr << "Error occurred : " << status.message() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
