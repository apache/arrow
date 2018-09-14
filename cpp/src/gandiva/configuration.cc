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

#include "boost/functional/hash.hpp"

namespace gandiva {

const std::shared_ptr<Configuration> ConfigurationBuilder::default_configuration_ =
    InitDefaultConfig();

std::size_t Configuration::Hash() const {
  boost::hash<std::string> string_hash;
  return string_hash(byte_code_file_path_);
}

bool Configuration::operator==(const Configuration& other) const {
  return other.byte_code_file_path() == byte_code_file_path();
}

bool Configuration::operator!=(const Configuration& other) const {
  return !(*this == other);
}

}  // namespace gandiva
