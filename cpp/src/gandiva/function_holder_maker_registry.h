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
#include <unordered_map>

#include "arrow/status.h"
#include "gandiva/function_holder.h"
#include "gandiva/node.h"

namespace gandiva {

/// registry of function holder makers
class FunctionHolderMakerRegistry {
 public:
  using FunctionHolderMaker =
      std::function<arrow::Result<FunctionHolderPtr>(const FunctionNode&)>;

  FunctionHolderMakerRegistry();

  arrow::Status Register(const std::string& name, FunctionHolderMaker holder_maker);

  /// \brief lookup a function holder maker using the given function name,
  /// and make a FunctionHolderPtr using the found holder maker and the given FunctionNode
  arrow::Result<FunctionHolderPtr> Make(const std::string& name,
                                        const FunctionNode& node);

 private:
  using MakerMap = std::unordered_map<std::string, FunctionHolderMaker>;

  MakerMap function_holder_makers_;
  static MakerMap DefaultHolderMakers();
};

}  // namespace gandiva
