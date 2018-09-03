/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef GANDIVA_CONFIGURATION_H
#define GANDIVA_CONFIGURATION_H

#include <memory>
#include <string>

#include "gandiva/status.h"

namespace gandiva {

extern const char kByteCodeFilePath[];
extern const char kHelperLibFilePath[];

class ConfigurationBuilder;
/// \brief runtime config for gandiva
///
/// It contains elements to customize gandiva execution
/// at run time.
class Configuration {
 public:
  friend class ConfigurationBuilder;

  const std::string &byte_code_file_path() const { return byte_code_file_path_; }
  const std::string &helper_lib_file_path() const { return helper_lib_file_path_; }

  std::size_t Hash() const;
  bool operator==(const Configuration &other) const;
  bool operator!=(const Configuration &other) const;

 private:
  explicit Configuration(const std::string &byte_code_file_path,
                         const std::string &helper_lib_file_path)
      : byte_code_file_path_(byte_code_file_path),
        helper_lib_file_path_(helper_lib_file_path) {}

  const std::string byte_code_file_path_;
  const std::string helper_lib_file_path_;
};

/// \brief configuration builder for gandiva
///
/// Provides a default configuration and convenience methods
/// to override specific values and build a custom instance
class ConfigurationBuilder {
 public:
  ConfigurationBuilder()
      : byte_code_file_path_(kByteCodeFilePath),
        helper_lib_file_path_(kHelperLibFilePath) {}

  ConfigurationBuilder &set_byte_code_file_path(const std::string &byte_code_file_path) {
    byte_code_file_path_ = byte_code_file_path;
    return *this;
  }

  ConfigurationBuilder &set_helper_lib_file_path(
      const std::string &helper_lib_file_path) {
    helper_lib_file_path_ = helper_lib_file_path;
    return *this;
  }

  std::shared_ptr<Configuration> build() {
    std::shared_ptr<Configuration> configuration(
        new Configuration(byte_code_file_path_, helper_lib_file_path_));
    return configuration;
  }

  static std::shared_ptr<Configuration> DefaultConfiguration() {
    return default_configuration_;
  }

 private:
  std::string byte_code_file_path_;
  std::string helper_lib_file_path_;

  static std::shared_ptr<Configuration> InitDefaultConfig() {
    std::shared_ptr<Configuration> configuration(
        new Configuration(kByteCodeFilePath, kHelperLibFilePath));
    return configuration;
  }

  static const std::shared_ptr<Configuration> default_configuration_;
};

}  // namespace gandiva
#endif  // GANDIVA_CONFIGURATION_H
