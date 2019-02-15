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
extern const std::string kPrecompiledBitcode;

class ConfigurationBuilder;
/// \brief runtime config for gandiva
///
/// It contains elements to customize gandiva execution
/// at run time.
class GANDIVA_EXPORT Configuration {
 public:
  friend class ConfigurationBuilder;

  const std::string& precompiled_bitcode() const { return precompiled_bitcode_; }

  // std::size_t Hash() const;
  bool operator==(const Configuration& other) const;
  bool operator!=(const Configuration& other) const;

 private:
  explicit Configuration(const std::string& precompiled_bitcode)
      : precompiled_bitcode_(precompiled_bitcode) {}

  const std::string& precompiled_bitcode_;
};

/// \brief configuration builder for gandiva
///
/// Provides a default configuration and convenience methods
/// to override specific values and build a custom instance
class GANDIVA_EXPORT ConfigurationBuilder {
 public:
  ConfigurationBuilder() : precompiled_bitcode_(kPrecompiledBitcode) {}

  ConfigurationBuilder& set_precompiled_bitcode(const std::string& precompiled_bitcode) {
    precompiled_bitcode_ = precompiled_bitcode;
    return *this;
  }

  std::shared_ptr<Configuration> build() {
    std::shared_ptr<Configuration> configuration(new Configuration(precompiled_bitcode_));
    return configuration;
  }

  static std::shared_ptr<Configuration> DefaultConfiguration() {
    return default_configuration_;
  }

 private:
  std::string precompiled_bitcode_;

  static std::shared_ptr<Configuration> InitDefaultConfig() {
    std::shared_ptr<Configuration> configuration(new Configuration(kPrecompiledBitcode));
    return configuration;
  }

  static const std::shared_ptr<Configuration> default_configuration_;
};

}  // namespace gandiva
