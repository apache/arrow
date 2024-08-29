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

#include "arrow/engine/substrait/plan_internal.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "arrow/compute/type_fwd.h"
#include "arrow/engine/substrait/relation_internal.h"
#include "arrow/engine/substrait/type_fwd.h"
#include "arrow/engine/substrait/util.h"
#include "arrow/engine/substrait/util_internal.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/hashing.h"
#include "arrow/util/macros.h"
#include "arrow/util/unreachable.h"

#include "substrait/extensions/extensions.pb.h"

namespace arrow {

using internal::checked_cast;

namespace engine {

Status AddExtensionSetToPlan(const ExtensionSet& ext_set, substrait::Plan* plan) {
  return AddExtensionSetToMessage(ext_set, plan);
}

Result<ExtensionSet> GetExtensionSetFromPlan(const substrait::Plan& plan,
                                             const ConversionOptions& conversion_options,
                                             const ExtensionIdRegistry* registry) {
  return GetExtensionSetFromMessage(plan, conversion_options, registry);
}

Result<std::unique_ptr<substrait::Plan>> PlanToProto(
    const acero::Declaration& declr, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  auto subs_plan = std::make_unique<substrait::Plan>();
  subs_plan->set_allocated_version(CreateVersion().release());
  auto plan_rel = std::make_unique<substrait::PlanRel>();
  auto rel_root = std::make_unique<substrait::RelRoot>();
  ARROW_ASSIGN_OR_RAISE(auto rel, ToProto(declr, ext_set, conversion_options));
  rel_root->set_allocated_input(rel.release());
  plan_rel->set_allocated_root(rel_root.release());
  subs_plan->mutable_relations()->AddAllocated(plan_rel.release());
  RETURN_NOT_OK(AddExtensionSetToPlan(*ext_set, subs_plan.get()));
  return std::move(subs_plan);
}

}  // namespace engine
}  // namespace arrow
