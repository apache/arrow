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

#pragma once

#include <string>

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace util {

/// \brief Dynamic library interface
///
class ARROW_EXPORT DynamicLibrary {
 public:
  DynamicLibrary(const std::string& path);

  virtual ~DynamicLibrary();

  bool HasFunction(const std::string& name);

  void* GetFunction(const std::string& name);

  const std::string& GetPath() const { return path_; }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(DynamicLibrary);

  DynamicLibrary() = delete;

  void *library_;

  std::string path_;
};

}  // namespace util
}  // namespace arrow
