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

#include "arrow/util/dynamic_library.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif

#include "arrow/status.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace util {

DynamicLibrary::DynamicLibrary(const std::string& path)
    : library_(nullptr), path_(path) {
#ifdef _WIN32
  library_ = LoadLibraryW(path_.c_str());
  if (library_ == nullptr) {
    ARROW_LOG(ERROR) << "Unable to load '" << path
                     << "': " << WinErrorMessage(GetLastError());
  }
#else
  library_ = dlopen(path_.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (library_ == nullptr) {
    std::string error_message = "unknown error";
    const char* err_msg = dlerror();
    if (err_msg) {
      error_message = err_msg;
    }
    ARROW_LOG(ERROR) << "Unable to load '" << path << "': " << error_message;
  }
#endif
}

DynamicLibrary::~DynamicLibrary() {
  if (library_) {
#ifdef _WIN32
    FreeLibrary((HMODULE)library_);
#else
    dlclose(library_);
#endif
  }
}

void* DynamicLibrary::GetFunction(const std::string& name) {
  if (library_ == nullptr) return nullptr;
#ifdef _WIN32
  void* result =
      reinterpret_cast<void*>(GetProcAddress((HMODULE)library_, name.c_str()));
#else
  void* result = dlsym(library_, name.c_str());
#endif
  if (result == nullptr) {
    ARROW_LOG(INFO) << "library '" << path_ << "' does not expose function " << name;
  }
  return result;
}

bool DynamicLibrary::HasFunction(const std::string& name) {
  return GetFunction(name) != nullptr;
}

}  // namespace util
}  // namespace arrow
