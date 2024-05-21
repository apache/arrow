// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <unordered_map>

#include "arrow/engine/substrait/extension_set.h"
#include "arrow/engine/substrait/options.h"
#include "arrow/engine/substrait/visibility.h"
#include "arrow/result.h"
#include "arrow/util/hashing.h"
#include "arrow/util/unreachable.h"

#include "substrait/algebra.pb.h"  // IWYU pragma: export
#include "substrait/plan.pb.h"     // IWYU pragma: export

namespace arrow {
namespace engine {

ARROW_ENGINE_EXPORT std::string EnumToString(
    int value, const google::protobuf::EnumDescriptor& descriptor);

// Extension sets can be present in both substrait::Plan and substrait::ExtendedExpression
// and so this utility is templated to support both.
template <typename MessageType>
Result<ExtensionSet> GetExtensionSetFromMessage(
    const MessageType& message, const ConversionOptions& conversion_options,
    const ExtensionIdRegistry* registry) {
  if (registry == NULLPTR) {
    registry = default_extension_id_registry();
  }
  std::unordered_map<uint32_t, std::string_view> uris;
  uris.reserve(message.extension_uris_size());
  for (const auto& uri : message.extension_uris()) {
    uris[uri.extension_uri_anchor()] = uri.uri();
  }

  // NOTE: it's acceptable to use views to memory owned by message; ExtensionSet::Make
  // will only store views to memory owned by registry.

  std::unordered_map<uint32_t, Id> type_ids, function_ids;
  for (const auto& ext : message.extensions()) {
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

template <typename Message>
Status AddExtensionSetToMessage(const ExtensionSet& ext_set, Message* message) {
  message->clear_extension_uris();

  std::unordered_map<std::string_view, int, ::arrow::internal::StringViewHash> map;

  auto uris = message->mutable_extension_uris();
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

  auto extensions = message->mutable_extensions();
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

std::unique_ptr<substrait::Version> CreateVersion();

}  // namespace engine
}  // namespace arrow
