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

#include "arrow/util/plugin_manager.h"

#include <atomic>

#include "arrow/util/plugin.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace util {

PluginManager::~PluginManager() {
  for (auto it = plugins_.begin(); it != plugins_.end();) {
    if (it->second != nullptr) {
      ARROW_LOG(INFO) << "Unregistering plugin: '" << it->second->GetName()
                      << "', version: '" << it->second->GetVersion() << "'";
      it->second->Cleanup();
      it = plugins_.erase(it);
    }
  }
}

std::unique_ptr<PluginManager> PluginManager::plugin_manager_ = nullptr;

Result<PluginManager*> PluginManager::GetPluginManager() {
  static std::mutex mutex;
  static std::atomic<bool> plugin_manager_initialized(false);

  if (!plugin_manager_initialized) {
    std::lock_guard<std::mutex> lock(mutex);
    if (!plugin_manager_initialized) {
      plugin_manager_.reset(new PluginManager());
      plugin_manager_initialized = true;
    }
  }

  return plugin_manager_.get();
}

Result<std::string> PluginManager::RegisterPlugin(const std::string& path) {
  std::lock_guard<std::mutex> lock(lock_);

  auto plugin = std::make_shared<Plugin>(path);

  if (!plugin->IsArrowPlugin()) {
    return Status::Invalid("Plugin '", path, "' does not export the proper functions");
  }

  std::string name = plugin->GetName();
  if (plugins_.find(name) != plugins_.end()) {
    return Status::Invalid("Plugin '", name, "' already registered");
  }

  ARROW_LOG(INFO) << "Registering plugin: '" << name
                  << "', version: '" << plugin->GetVersion() << "'";

  plugin->Initialize();

  plugins_[name] = std::move(plugin);

  return name;
}

Status PluginManager::UnregisterPlugin(const std::string& name) {
  std::lock_guard<std::mutex> lock(lock_);

  auto it = plugins_.find(name);
  if (it == plugins_.end()) {
    return Status::Invalid("No plugin with name '", name, "' registered");
  }

  it->second->Cleanup();

  plugins_.erase(it);

  ARROW_LOG(INFO) << "Unregistering plugin '" << name << "' done";

  return Status::OK();
}

bool PluginManager::HasPlugin(const std::string& name) {
  std::lock_guard<std::mutex> lock(lock_);

  return plugins_.find(name) != plugins_.end();
}

Result<std::shared_ptr<Plugin>> PluginManager::GetPlugin(const std::string& name) {
  std::lock_guard<std::mutex> lock(lock_);

  auto it = plugins_.find(name);
  if (it == plugins_.end()) {
    return Status::Invalid("No plugin with name '", name, "' registered");
  }
  return it->second;
}

}  // namespace util
}  // namespace arrow
