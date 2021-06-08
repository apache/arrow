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

enum LLVMExecutionMode { JIT_AND_OPTIMIZE, JIT, INTERPRETED };

class ConfigurationBuilder;
/// \brief runtime config for gandiva
///
/// It contains elements to customize gandiva execution
/// at run time.
class GANDIVA_EXPORT Configuration {
 public:
  friend class ConfigurationBuilder;

  Configuration()
      : execution_mode_(LLVMExecutionMode::JIT_AND_OPTIMIZE), target_host_cpu_(true) {}

  explicit Configuration(bool optimize) : target_host_cpu_(true) {
    set_execution_mode(true, optimize);
  }

  Configuration(bool optimize, bool compile) : target_host_cpu_(true) {
    set_execution_mode(compile, optimize);
  }

  std::size_t Hash() const;
  bool operator==(const Configuration& other) const;
  bool operator!=(const Configuration& other) const;

  LLVMExecutionMode execution_mode() const { return execution_mode_; }

  bool target_host_cpu() const { return target_host_cpu_; }

  void set_execution_mode(bool compile, bool optimize) {
    if (compile) {
      if (optimize) {
        execution_mode_ = LLVMExecutionMode::JIT_AND_OPTIMIZE;
      } else {
        execution_mode_ = LLVMExecutionMode::JIT;
      }
    } else {
      execution_mode_ = LLVMExecutionMode::INTERPRETED;
    }
  }

  void target_host_cpu(bool target_host_cpu) { target_host_cpu_ = target_host_cpu; }

 private:
  // It represents the mode that the LLVM will be executed inside the program.
  // The user can choose between a JIT mode, both optimized and non-optimized, or
  // run the LLVM in Interpreted mode.
  LLVMExecutionMode execution_mode_;
  bool target_host_cpu_; /* set the mcpu flag to host cpu while compiling llvm ir */
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

  std::shared_ptr<Configuration> build(bool optimize, bool compile) {
    std::shared_ptr<Configuration> configuration(new Configuration(optimize, compile));
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
