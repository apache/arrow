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

#include <vector>
#include "gandiva/function_registry.h"
#include "gandiva/visibility.h"

namespace gandiva {

class Engine;

// Base-class type for exporting functions that can be accessed from LLVM/IR.
class ExportedFuncsBase {
 public:
  virtual ~ExportedFuncsBase() = default;

  virtual arrow::Status AddMappings(Engine* engine) const = 0;
};

// Class for exporting Stub functions
class ExportedStubFunctions : public ExportedFuncsBase {
  arrow::Status AddMappings(Engine* engine) const override;
};

// Class for exporting Context functions
class ExportedContextFunctions : public ExportedFuncsBase {
  arrow::Status AddMappings(Engine* engine) const override;
};

// Class for exporting Time functions
class ExportedTimeFunctions : public ExportedFuncsBase {
  arrow::Status AddMappings(Engine* engine) const override;
};

// Class for exporting Decimal functions
class ExportedDecimalFunctions : public ExportedFuncsBase {
  arrow::Status AddMappings(Engine* engine) const override;
};

// Class for exporting String functions
class ExportedStringFunctions : public ExportedFuncsBase {
  arrow::Status AddMappings(Engine* engine) const override;
};

// Class for exporting Hash functions
class ExportedHashFunctions : public ExportedFuncsBase {
  arrow::Status AddMappings(Engine* engine) const override;
};

class ExternalCFunctions : public ExportedFuncsBase {
 public:
  explicit ExternalCFunctions(std::shared_ptr<FunctionRegistry> function_registry)
      : function_registry_(std::move(function_registry)) {}

  arrow::Status AddMappings(Engine* engine) const override;

 private:
  std::shared_ptr<FunctionRegistry> function_registry_;
};

GANDIVA_EXPORT void RegisterExportedFuncs();
}  // namespace gandiva
