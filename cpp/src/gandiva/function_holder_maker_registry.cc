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

#include "gandiva/function_holder_maker_registry.h"

#include <functional>

#include "gandiva/function_holder.h"
#include "gandiva/interval_holder.h"
#include "gandiva/random_generator_holder.h"
#include "gandiva/regex_functions_holder.h"
#include "gandiva/to_date_holder.h"

namespace gandiva {

FunctionHolderMakerRegistry::FunctionHolderMakerRegistry()
    : function_holder_makers_(DefaultHolderMakers()) {}

static std::string to_lower(const std::string& str) {
  std::string data = str;
  std::transform(data.begin(), data.end(), data.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return data;
}
arrow::Status FunctionHolderMakerRegistry::Register(const std::string& name,
                                                    FunctionHolderMaker holder_maker) {
  function_holder_makers_.emplace(to_lower(name), std::move(holder_maker));
  return arrow::Status::OK();
}

template <typename HolderType>
static arrow::Result<FunctionHolderPtr> HolderMaker(const FunctionNode& node) {
  std::shared_ptr<HolderType> derived_instance;
  ARROW_RETURN_NOT_OK(HolderType::Make(node, &derived_instance));
  return derived_instance;
}

arrow::Result<FunctionHolderPtr> FunctionHolderMakerRegistry::Make(
    const std::string& name, const FunctionNode& node) {
  auto lowered_name = to_lower(name);
  auto found = function_holder_makers_.find(lowered_name);
  if (found == function_holder_makers_.end()) {
    return Status::Invalid("function holder not registered for function " + name);
  }

  return found->second(node);
}

FunctionHolderMakerRegistry::MakerMap FunctionHolderMakerRegistry::DefaultHolderMakers() {
  static const MakerMap maker_map = {
      {"like", HolderMaker<LikeHolder>},
      {"to_date", HolderMaker<ToDateHolder>},
      {"random", HolderMaker<RandomGeneratorHolder>},
      {"rand", HolderMaker<RandomGeneratorHolder>},
      {"regexp_replace", HolderMaker<ReplaceHolder>},
      {"regexp_extract", HolderMaker<ExtractHolder>},
      {"castintervalday", HolderMaker<IntervalDaysHolder>},
      {"castintervalyear", HolderMaker<IntervalYearsHolder>}};
  return maker_map;
}
}  // namespace gandiva
