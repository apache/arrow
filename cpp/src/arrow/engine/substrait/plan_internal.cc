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
#include "arrow/config.h"
#include "arrow/engine/substrait/relation_internal.h"
#include "arrow/engine/substrait/type_fwd.h"
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
  plan->clear_extension_uris();

  std::unordered_map<std::string_view, int, ::arrow::internal::StringViewHash> map;

  auto uris = plan->mutable_extension_uris();
  uris->Reserve(static_cast<int>(ext_set.uris().size()));
  for (uint32_t anchor = 0; anchor < ext_set.uris().size(); ++anchor) {
    auto uri = ext_set.uris().at(anchor);
    if (uri.empty()) continue;

    auto ext_uri = std::make_unique<substrait::extensions::SimpleExtensionURI>();
    ext_uri->set_uri(std::string(uri));
    ext_uri->set_extension_uri_anchor(anchor);
    uris->AddAllocated(ext_uri.release());

    map[uri] = anchor;
  }

  auto extensions = plan->mutable_extensions();
  extensions->Reserve(static_cast<int>(ext_set.num_types() + ext_set.num_functions()));

  using ExtDecl = substrait::extensions::SimpleExtensionDeclaration;

  for (uint32_t anchor = 0; anchor < ext_set.num_types(); ++anchor) {
    ARROW_ASSIGN_OR_RAISE(auto type_record, ext_set.DecodeType(anchor));
    if (type_record.id.empty()) continue;

    auto ext_decl = std::make_unique<ExtDecl>();

    auto type = std::make_unique<ExtDecl::ExtensionType>();
    type->set_extension_uri_reference(map[type_record.id.uri]);
    type->set_type_anchor(anchor);
    type->set_name(std::string(type_record.id.name));
    ext_decl->set_allocated_extension_type(type.release());
    extensions->AddAllocated(ext_decl.release());
  }

  for (uint32_t anchor = 0; anchor < ext_set.num_functions(); ++anchor) {
    ARROW_ASSIGN_OR_RAISE(Id function_id, ext_set.DecodeFunction(anchor));

    auto fn = std::make_unique<ExtDecl::ExtensionFunction>();
    fn->set_extension_uri_reference(map[function_id.uri]);
    fn->set_function_anchor(anchor);
    fn->set_name(std::string(function_id.name));

    auto ext_decl = std::make_unique<ExtDecl>();
    ext_decl->set_allocated_extension_function(fn.release());
    extensions->AddAllocated(ext_decl.release());
  }

  return Status::OK();
}

Result<ExtensionSet> GetExtensionSetFromPlan(const substrait::Plan& plan,
                                             const ConversionOptions& conversion_options,
                                             const ExtensionIdRegistry* registry) {
  if (registry == NULLPTR) {
    registry = default_extension_id_registry();
  }
  std::unordered_map<uint32_t, std::string_view> uris;
  uris.reserve(plan.extension_uris_size());
  for (const auto& uri : plan.extension_uris()) {
    uris[uri.extension_uri_anchor()] = uri.uri();
  }

  // NOTE: it's acceptable to use views to memory owned by plan; ExtensionSet::Make
  // will only store views to memory owned by registry.

  std::unordered_map<uint32_t, Id> type_ids, function_ids;
  for (const auto& ext : plan.extensions()) {
    switch (ext.mapping_type_case()) {
      case substrait::extensions::SimpleExtensionDeclaration::kExtensionTypeVariation: {
        return Status::NotImplemented("Type Variations are not yet implemented");
      }

      case substrait::extensions::SimpleExtensionDeclaration::kExtensionType: {
        const auto& type = ext.extension_type();
        std::string_view uri = uris[type.extension_uri_reference()];
        type_ids[type.type_anchor()] = Id{uri, type.name()};
        break;
      }

      case substrait::extensions::SimpleExtensionDeclaration::kExtensionFunction: {
        const auto& fn = ext.extension_function();
        std::string_view uri = uris[fn.extension_uri_reference()];
        function_ids[fn.function_anchor()] = Id{uri, fn.name()};
        break;
      }

      default:
        Unreachable();
    }
  }

  return ExtensionSet::Make(std::move(uris), std::move(type_ids), std::move(function_ids),
                            conversion_options, registry);
}

namespace {

// TODO(ARROW-18145) Populate these from cmake files
constexpr uint32_t kSubstraitMajorVersion = 0;
constexpr uint32_t kSubstraitMinorVersion = 20;
constexpr uint32_t kSubstraitPatchVersion = 0;

std::unique_ptr<substrait::Version> CreateVersion() {
  auto version = std::make_unique<substrait::Version>();
  version->set_major_number(kSubstraitMajorVersion);
  version->set_minor_number(kSubstraitMinorVersion);
  version->set_patch_number(kSubstraitPatchVersion);
  version->set_producer("Acero " + GetBuildInfo().version_string);
  return version;
}

}  // namespace

Result<std::unique_ptr<substrait::Plan>> PlanToProto(
    const compute::Declaration& declr, ExtensionSet* ext_set,
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
