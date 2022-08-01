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

#include "arrow/dataset/plan.h"
#include "arrow/dataset/scanner.h"
#include "arrow/engine/substrait/registry.h"
#include "arrow/result.h"
#include "arrow/util/hash_util.h"
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/unreachable.h"

#include <unordered_map>

namespace arrow {

using internal::checked_cast;

namespace engine {

namespace internal {
using ::arrow::internal::hash_combine;
using ::arrow::internal::make_unique;
}  // namespace internal

Status AddExtensionSetToPlan(const ExtensionSet& ext_set, substrait::Plan* plan) {
  plan->clear_extension_uris();

  std::unordered_map<util::string_view, int, ::arrow::internal::StringViewHash> map;

  auto uris = plan->mutable_extension_uris();
  uris->Reserve(static_cast<int>(ext_set.uris().size()));
  for (uint32_t anchor = 0; anchor < ext_set.uris().size(); ++anchor) {
    auto uri = ext_set.uris().at(anchor);
    if (uri.empty()) continue;

    auto ext_uri = internal::make_unique<substrait::extensions::SimpleExtensionURI>();
    ext_uri->set_uri(uri.to_string());
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

    auto ext_decl = internal::make_unique<ExtDecl>();

    auto type = internal::make_unique<ExtDecl::ExtensionType>();
    type->set_extension_uri_reference(map[type_record.id.uri]);
    type->set_type_anchor(anchor);
    type->set_name(type_record.id.name.to_string());
    ext_decl->set_allocated_extension_type(type.release());
    extensions->AddAllocated(ext_decl.release());
  }

  for (uint32_t anchor = 0; anchor < ext_set.num_functions(); ++anchor) {
    ARROW_ASSIGN_OR_RAISE(Id function_id, ext_set.DecodeFunction(anchor));

    auto fn = internal::make_unique<ExtDecl::ExtensionFunction>();
    fn->set_extension_uri_reference(map[function_id.uri]);
    fn->set_function_anchor(anchor);
    fn->set_name(function_id.name.to_string());

    auto ext_decl = internal::make_unique<ExtDecl>();
    ext_decl->set_allocated_extension_function(fn.release());
    extensions->AddAllocated(ext_decl.release());
  }

  return Status::OK();
}

Result<ExtensionSet> GetExtensionSetFromPlan(const substrait::Plan& plan,
                                             const ExtensionIdRegistry* registry) {
  if (registry == NULLPTR) {
    registry = default_extension_id_registry();
  }
  std::unordered_map<uint32_t, util::string_view> uris;
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
        util::string_view uri = uris[type.extension_uri_reference()];
        type_ids[type.type_anchor()] = Id{uri, type.name()};
        break;
      }

      case substrait::extensions::SimpleExtensionDeclaration::kExtensionFunction: {
        const auto& fn = ext.extension_function();
        util::string_view uri = uris[fn.extension_uri_reference()];
        function_ids[fn.function_anchor()] = Id{uri, fn.name()};
        break;
      }

      default:
        Unreachable();
    }
  }

  return ExtensionSet::Make(std::move(uris), std::move(type_ids), std::move(function_ids),
                            registry);
}

Status SetRelation(const std::unique_ptr<substrait::Rel>& plan,
                   const std::unique_ptr<substrait::Rel>& partial_plan,
                   const std::string& factory_name) {
  if (factory_name == "scan" && partial_plan->has_read()) {
    plan->set_allocated_read(partial_plan->release_read());
  } else if (factory_name == "filter" && partial_plan->has_filter()) {
    plan->set_allocated_filter(partial_plan->release_filter());
  } else {
    return Status::NotImplemented("Substrait converter ", factory_name,
                                  " not supported.");
  }
  return Status::OK();
}

Result<std::shared_ptr<Schema>> ExtractSchemaToBind(const compute::Declaration& declr) {
  std::shared_ptr<Schema> bind_schema;
  if (declr.factory_name == "scan") {
    const auto& opts = checked_cast<const dataset::ScanNodeOptions&>(*(declr.options));
    bind_schema = opts.dataset->schema();
  } else if (declr.factory_name == "filter") {
    auto input_declr = util::get<compute::Declaration>(declr.inputs[0]);
    ARROW_ASSIGN_OR_RAISE(bind_schema, ExtractSchemaToBind(input_declr));
  } else if (declr.factory_name == "hashjoin") {
  } else if (declr.factory_name == "sink") {
    return bind_schema;
  } else {
    return Status::Invalid("Schema extraction failed, unsupported factory ",
                           declr.factory_name);
  }
  return bind_schema;
}

Status TraverseDeclarations(const compute::Declaration& declaration,
                            ExtensionSet* ext_set, std::unique_ptr<substrait::Rel>& rel) {
  std::vector<compute::Declaration::Input> inputs = declaration.inputs;
  for (auto& input : inputs) {
    auto input_decl = util::get<compute::Declaration>(input);
    RETURN_NOT_OK(TraverseDeclarations(input_decl, ext_set, rel));
  }
  const auto& factory_name = declaration.factory_name;
  std::cout << factory_name << std::endl;
  ARROW_ASSIGN_OR_RAISE(auto schema, ExtractSchemaToBind(declaration));
  SubstraitConversionRegistry* registry = default_substrait_conversion_registry();
  if (factory_name != "sink") {
    ARROW_ASSIGN_OR_RAISE(auto factory, registry->GetConverter(factory_name));
    ARROW_ASSIGN_OR_RAISE(auto factory_rel, factory(schema, declaration, ext_set));
  }
  return Status::OK();
}

Result<std::unique_ptr<substrait::PlanRel>> ToProto(compute::ExecPlan* plan,
                                                    const compute::Declaration& declr,
                                                    ExtensionSet* ext_set) {
  auto plan_rel = internal::make_unique<substrait::PlanRel>();
  auto rel = internal::make_unique<substrait::Rel>();
  RETURN_NOT_OK(TraverseDeclarations(declr, ext_set, rel));
  return plan_rel;
}

}  // namespace engine
}  // namespace arrow
