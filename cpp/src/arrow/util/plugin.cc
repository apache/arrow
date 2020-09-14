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

#include "arrow/util/plugin.h"

#include "arrow/util/logging.h"

namespace arrow {
namespace util {

static const char* CallGetPluginName(DynamicLibrary& library) {
  typedef const char* (*GetPluginName)();

#ifdef WIN32
  GetPluginName getPluginName =
      reinterpret_cast<GetPluginName>(library.GetFunction("ArrowPluginGetName"));
#else
  GetPluginName getPluginName;
  *reinterpret_cast<void**>(&getPluginName) = library.GetFunction("ArrowPluginGetName");
#endif

  if (getPluginName != nullptr) {
    return getPluginName();
  } else {
    ARROW_LOG(ERROR) << "Can't find ArrowPluginGetName for plugin " << library.GetPath();
    return "unknown name";
  }
}

static const char* CallGetPluginVersion(DynamicLibrary& library) {
  typedef const char* (*GetPluginVersion)();

#ifdef WIN32
  GetPluginVersion getPluginVersion =
      reinterpret_cast<GetPluginVersion>(library.GetFunction("ArrowPluginGetVersion"));
#else
  GetPluginVersion getPluginVersion;
  *reinterpret_cast<void**>(&getPluginVersion) =
      library.GetFunction("ArrowPluginGetVersion");
#endif

  if (getPluginVersion != nullptr) {
    return getPluginVersion();
  } else {
    ARROW_LOG(ERROR) << "Can't find ArrowPluginGetVersion for plugin "
                     << library.GetPath();
    return "unknown version";
  }
}

Plugin::Plugin(const std::string& path) : library_(path) {
  memset(&context_, 0, sizeof(context_));
  name_ = CallGetPluginName(library_);
  version_ = CallGetPluginVersion(library_);
}

Status Plugin::Initialize() {
  typedef int32_t (*PluginInitialize)(const ArrowPluginContext*);

#ifdef WIN32
  PluginInitialize pluginInitialize =
      reinterpret_cast<PluginInitialize>(library_.GetFunction("ArrowPluginInitialize"));
#else
  PluginInitialize pluginInitialize;
  *reinterpret_cast<void**>(&pluginInitialize) =
      library_.GetFunction("ArrowPluginInitialize");
#endif

  if (pluginInitialize != nullptr) {
    int32_t result = pluginInitialize(&context_);
    if (result != 0) {
      return Status::Invalid("Error initializing for plugin ", name_);
    }
  } else {
    return Status::Invalid("Can't find ArrowPluginInitialize for plugin ", name_);
  }

  return Status::OK();
}

Status Plugin::Cleanup() {
  typedef void (*PluginCleanup)(void);

#ifdef WIN32
  PluginCleanup pluginCleanup =
      reinterpret_cast<PluginCleanup>(library_.GetFunction("ArrowPluginCleanup"));
#else
  PluginCleanup pluginCleanup;
  *reinterpret_cast<void**>(&pluginCleanup) = library_.GetFunction("ArrowPluginCleanup");
#endif

  if (pluginCleanup != nullptr) {
    pluginCleanup();
  } else {
    return Status::Invalid("Can't find ArrowPluginCleanup for plugin ", name_);
  }

  return Status::OK();
}

bool Plugin::IsArrowPlugin() {
  return (library_.HasFunction("ArrowPluginInitialize") &&
          library_.HasFunction("ArrowPluginCleanup") &&
          library_.HasFunction("ArrowPluginGetName") &&
          library_.HasFunction("ArrowPluginGetVersion"));
}

}  // namespace util
}  // namespace arrow
