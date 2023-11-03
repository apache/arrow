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

#include "gandiva/configuration.h"

#include "arrow/util/hash_util.h"

namespace gandiva {

const std::shared_ptr<Configuration> ConfigurationBuilder::default_configuration_ =
    InitDefaultConfig();

std::size_t Configuration::Hash() const {
  static constexpr size_t kHashSeed = 0;
  size_t result = kHashSeed;
  arrow::internal::hash_combine(result, static_cast<size_t>(optimize_));
  arrow::internal::hash_combine(result, static_cast<size_t>(target_host_cpu_));
  arrow::internal::hash_combine(
      result, reinterpret_cast<std::uintptr_t>(function_registry_.get()));
  return result;
}

bool Configuration::operator==(const Configuration& other) const {
  return optimize_ == other.optimize_ && target_host_cpu_ == other.target_host_cpu_ &&
         function_registry_ == other.function_registry_;
}

bool Configuration::operator!=(const Configuration& other) const {
  return !(*this == other);
}

}  // namespace gandiva
