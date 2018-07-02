// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifndef CONFIG_HOLDER_H
#define CONFIG_HOLDER_H

#include <mutex>
#include <memory>
#include <unordered_map>
#include <utility>

#include "gandiva/configuration.h"

namespace gandiva {

class ConfigHolder {
 public:
  static int64_t MapInsert(std::shared_ptr<Configuration> config) {
    g_mtx_.lock();

    int64_t result = config_id_++;
    configuration_map_.insert(
          std::pair<int64_t, std::shared_ptr<Configuration>>(result, config));

    g_mtx_.unlock();
    return result;
  }

  static void MapErase(int64_t config_id_) {
    g_mtx_.lock();
    configuration_map_.erase(config_id_);
    g_mtx_.unlock();
  }

  static std::shared_ptr<Configuration> MapLookup(int64_t config_id_) {
    std::shared_ptr<Configuration> result = nullptr;

    try {
      result = configuration_map_.at(config_id_);
    } catch (const std::out_of_range& e) {
    }

    return result;
  }

 private:
  // map of configuration objects created so far
  static std::unordered_map<int64_t, std::shared_ptr<Configuration>> configuration_map_;

  static std::mutex g_mtx_;

  // atomic counter for projector module ids
  static int64_t config_id_;
};
} //namespace gandiva
#endif // CONFIG_HOLDER_H
