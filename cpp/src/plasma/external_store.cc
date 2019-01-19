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

#include <iostream>
#include <sstream>

#include "external_store.h"

namespace plasma {

std::shared_ptr<std::map<std::string, std::shared_ptr<ExternalStore>>> ExternalStores::external_stores_ = nullptr;

std::string ExternalStores::ExtractStoreName(const std::string& endpoint) {
  size_t off = endpoint.find_first_of(':');
  if (off == std::string::npos) {
    throw std::invalid_argument("Malformed endpoint " + endpoint);
  }
  return endpoint.substr(0, off);
}

void ExternalStores::RegisterStore(const std::string& store_name,
                                   std::shared_ptr<ExternalStore> store) {
  std::cerr << "Registering external store \"" << store_name << "\"" << std::endl;
  Stores()->insert({ store_name, store });
}

void ExternalStores::DeregisterStore(const std::string &store_name) {
  std::cerr << "Deregistering external store \"" << store_name << "\"" << std::endl;
  auto it = Stores()->find(store_name);
  if (it == Stores()->end()) {
    return;
  }
  std::shared_ptr<ExternalStore> store = it->second;
  Stores()->erase(it);
}

std::shared_ptr<ExternalStore> ExternalStores::GetStore(const std::string &store_name) {
  auto it = Stores()->find(store_name);
  if (it == Stores()->end()) {
    return nullptr;
  }
  return it->second;
}

std::shared_ptr<std::map<std::string, std::shared_ptr<ExternalStore>>> ExternalStores::Stores() {
  if (external_stores_ == nullptr) {
    external_stores_ = std::make_shared<std::map<std::string, std::shared_ptr<ExternalStore>>>();
  }
  return external_stores_;
}

}

