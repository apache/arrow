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

#include "arrow/status.h"

#include "gandiva/visibility.h"

namespace gandiva {

GANDIVA_EXPORT
extern const char kByteCodeFilePath[];

class ConfigurationBuilder;
/// \brief runtime config for gandiva
///
/// It contains elements to customize gandiva execution
/// at run time.
class GANDIVA_EXPORT Configuration {
 public:
  friend class ConfigurationBuilder;

  const std::string& byte_code_file_path() const { return byte_code_file_path_; }

  std::size_t Hash() const;
  bool operator==(const Configuration& other) const;
  bool operator!=(const Configuration& other) const;

 private:
  explicit Configuration(const std::string& byte_code_file_path)
      : byte_code_file_path_(byte_code_file_path) {}

  const std::string byte_code_file_path_;
};

/// \brief configuration builder for gandiva
///
/// Provides a default configuration and convenience methods
/// to override specific values and build a custom instance
class GANDIVA_EXPORT ConfigurationBuilder {
 public:
  ConfigurationBuilder() : byte_code_file_path_(kByteCodeFilePath) {}

  ConfigurationBuilder& set_byte_code_file_path(const std::string& byte_code_file_path) {
    byte_code_file_path_ = byte_code_file_path;
    return *this;
  }

  std::shared_ptr<Configuration> build() {
    std::shared_ptr<Configuration> configuration(new Configuration(byte_code_file_path_));
    return configuration;
  }

  static std::shared_ptr<Configuration> DefaultConfiguration() {
    return default_configuration_;
  }

 private:
  std::string byte_code_file_path_;

  static std::shared_ptr<Configuration> InitDefaultConfig() {
    std::shared_ptr<Configuration> configuration(new Configuration(kByteCodeFilePath));
    return configuration;
  }

  static const std::shared_ptr<Configuration> default_configuration_;
};

}  // namespace gandiva
