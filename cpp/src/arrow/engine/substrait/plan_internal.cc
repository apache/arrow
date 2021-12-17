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

#include "arrow/result.h"
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace engine {

Status AddExtensionSetToPlan(const ExtensionSet& ext_set, st::Plan* plan) {
  plan->clear_extension_uris();

  std::unordered_map<util::string_view, int, internal::StringViewHash> map;

  auto uris = plan->mutable_extension_uris();
  uris->Reserve(static_cast<int>(ext_set.uris().size()));
  for (size_t anchor = 0; anchor < ext_set.uris().size(); ++anchor) {
    const auto& uri = ext_set.uris()[anchor];
    if (uri.empty()) continue;

    auto ext_uri = internal::make_unique<st::extensions::SimpleExtensionURI>();
    ext_uri->set_uri(uri);
    ext_uri->set_extension_uri_anchor(anchor);
    uris->AddAllocated(ext_uri.release());

    map[uri] = anchor;
  }

  auto extensions = plan->mutable_extensions();
  extensions->Reserve(static_cast<int>(ext_set.types().size()));

  for (size_t anchor = 0; anchor < ext_set.type_ids().size(); ++anchor) {
    auto id = ext_set.type_ids()[anchor];
    if (id.empty()) continue;

    using ExtDecl = st::extensions::SimpleExtensionDeclaration;
    auto ext_decl = internal::make_unique<ExtDecl>();

    if (ext_set.type_is_variation(anchor)) {
      auto type_var = internal::make_unique<ExtDecl::ExtensionTypeVariation>();
      type_var->set_extension_uri_pointer(map[id.uri]);
      type_var->set_type_variation_anchor(anchor);
      type_var->set_name(id.name.to_string());
      ext_decl->set_allocated_extension_type_variation(type_var.release());
    } else {
      auto type = internal::make_unique<ExtDecl::ExtensionType>();
      type->set_extension_uri_reference(map[id.uri]);
      type->set_type_anchor(anchor);
      type->set_name(id.name.to_string());
      ext_decl->set_allocated_extension_type(type.release());
    }

    extensions->AddAllocated(ext_decl.release());
  }

  return Status::OK();
}

namespace {
template <typename T>
void SetElement(size_t i, T element, std::vector<T>* vector) {
  DCHECK_LE(i, 1 << 20);
  if (i >= vector->size()) {
    vector->resize(i + 1);
  }
  (*vector)[i] = std::move(element);
}
}  // namespace

Result<ExtensionSet> GetExtensionSetFromPlan(const st::Plan& plan,
                                             ExtensionIdRegistry* registry) {
  std::vector<std::string> uris;
  for (const auto& uri : plan.extension_uris()) {
    SetElement(uri.extension_uri_anchor(), uri.uri(), &uris);
  }

  // NOTE: it's acceptable to use views to memory owned by plan; ExtensionSet::Make
  // will only store views to memory owned by registry.

  std::vector<ExtensionSet::Id> type_ids;
  std::vector<bool> type_is_variation;
  for (const auto& ext : plan.extensions()) {
    switch (ext.mapping_type_case()) {
      case st::extensions::SimpleExtensionDeclaration::kExtensionTypeVariation: {
        const auto& type_var = ext.extension_type_variation();
        util::string_view uri = uris[type_var.extension_uri_pointer()];
        SetElement(type_var.type_variation_anchor(), {uri, type_var.name()}, &type_ids);
        SetElement(type_var.type_variation_anchor(), true, &type_is_variation);
        break;
      }

      case st::extensions::SimpleExtensionDeclaration::kExtensionType: {
        const auto& type = ext.extension_type();
        util::string_view uri = uris[type.extension_uri_reference()];
        SetElement(type.type_anchor(), {uri, type.name()}, &type_ids);
        SetElement(type.type_anchor(), false, &type_is_variation);
        break;
      }

      case st::extensions::SimpleExtensionDeclaration::kExtensionFunction:
      default:
        return Status::NotImplemented("");
    }
  }

  return ExtensionSet::Make(std::move(uris), std::move(type_ids),
                            std::move(type_is_variation), registry);
}

}  // namespace engine
}  // namespace arrow
