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

#include <memory>
#include <string>
#include <mutex>
#include <unordered_map>

#include "arrow/status.h"
#include "arrow/result.h"
#include "arrow/util/dynamic_library.h"
#include "arrow/util/arrow_plugin.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace util {

// Forward declaration
class Plugin;

/// \brief Plugin manager interface
///
class ARROW_EXPORT PluginManager {
 public:
  static Result<PluginManager*> GetPluginManager();

  /// Register plugin
  Result<std::string> RegisterPlugin(const std::string& path);

  /// UnRegister plugin
  Status UnregisterPlugin(const std::string& name);

  //void ListPlugins(std::list<std::string>& result) const;

  bool HasPlugin(const std::string& name);

  /// \brief Get a Plugin instance for a particular plugin
  /// \param[in] name the plugin name
  /// \return shared plugin
  Result<std::shared_ptr<Plugin>> GetPlugin(const std::string& name);

  virtual  ~PluginManager();

 private:
  PluginManager() = default;
  static std::unique_ptr<PluginManager> plugin_manager_;

  typedef std::unordered_map<std::string, std::shared_ptr<Plugin>> Plugins;
  Plugins plugins_;

  std::mutex lock_;
};

}  // namespace util
}  // namespace arrow
