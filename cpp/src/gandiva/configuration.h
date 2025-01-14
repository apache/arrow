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
#include "gandiva/function_registry.h"
#include "gandiva/visibility.h"

namespace gandiva {

class ConfigurationBuilder;
/// \brief runtime config for gandiva
///
/// It contains elements to customize gandiva execution
/// at run time.
class GANDIVA_EXPORT Configuration {
 public:
  friend class ConfigurationBuilder;

  explicit Configuration(bool optimize,
                         std::shared_ptr<FunctionRegistry> function_registry =
                             gandiva::default_function_registry())
      : optimize_(optimize),
        target_host_cpu_(true),
        function_registry_(function_registry) {}

  Configuration() : Configuration(true) {}

  std::size_t Hash() const;
  bool operator==(const Configuration& other) const;
  bool operator!=(const Configuration& other) const;

  bool optimize() const { return optimize_; }
  bool target_host_cpu() const { return target_host_cpu_; }
  std::shared_ptr<FunctionRegistry> function_registry() const {
    return function_registry_;
  }

  void set_optimize(bool optimize) { optimize_ = optimize; }
  void target_host_cpu(bool target_host_cpu) { target_host_cpu_ = target_host_cpu; }
  void set_function_registry(std::shared_ptr<FunctionRegistry> function_registry) {
    function_registry_ = std::move(function_registry);
  }

 private:
  bool optimize_;        /* optimise the generated llvm IR */
  bool target_host_cpu_; /* set the mcpu flag to host cpu while compiling llvm ir */
  std::shared_ptr<FunctionRegistry>
      function_registry_; /* function registry that may contain external functions */
};

/// \brief configuration builder for gandiva
///
/// Provides a default configuration and convenience methods
/// to override specific values and build a custom instance
class GANDIVA_EXPORT ConfigurationBuilder {
 public:
  std::shared_ptr<Configuration> build() {
    std::shared_ptr<Configuration> configuration(new Configuration());
    return configuration;
  }

  std::shared_ptr<Configuration> build(bool optimize) {
    std::shared_ptr<Configuration> configuration(new Configuration(optimize));
    return configuration;
  }

  std::shared_ptr<Configuration> build(
      std::shared_ptr<FunctionRegistry> function_registry) {
    std::shared_ptr<Configuration> configuration(
        new Configuration(true, std::move(function_registry)));
    return configuration;
  }

  static std::shared_ptr<Configuration> DefaultConfiguration() {
    return default_configuration_;
  }

 private:
  static std::shared_ptr<Configuration> InitDefaultConfig() {
    std::shared_ptr<Configuration> configuration(new Configuration());
    return configuration;
  }

  static const std::shared_ptr<Configuration> default_configuration_;
};

}  // namespace gandiva
