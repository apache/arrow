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

#include "arrow/engine/substrait/options.h"

#include <google/protobuf/util/json_util.h>
#include <mutex>

#include "arrow/compute/exec/asof_join_node.h"
#include "arrow/compute/exec/options.h"
#include "arrow/engine/substrait/expression_internal.h"
#include "arrow/engine/substrait/options_internal.h"
#include "arrow/engine/substrait/relation_internal.h"
#include "substrait/extension_rels.pb.h"

namespace arrow {
namespace engine {

namespace {

std::vector<compute::Declaration::Input> MakeDeclarationInputs(
    const std::vector<DeclarationInfo>& inputs) {
  std::vector<compute::Declaration::Input> input_decls(inputs.size());
  for (size_t i = 0; i < inputs.size(); i++) {
    input_decls[i] = inputs[i].declaration;
  }
  return input_decls;
}

}  // namespace

class BaseExtensionProvider : public ExtensionProvider {
 public:
  Result<RelationInfo> MakeRel(const ConversionOptions& conv_opts,
                               const std::vector<DeclarationInfo>& inputs,
                               const ExtensionDetails& ext_details,
                               const ExtensionSet& ext_set) override {
    auto details = dynamic_cast<const DefaultExtensionDetails&>(ext_details);
    return MakeRel(conv_opts, inputs, details.rel, ext_set);
  }

  virtual Result<RelationInfo> MakeRel(const ConversionOptions& conv_opts,
                                       const std::vector<DeclarationInfo>& inputs,
                                       const google::protobuf::Any& rel,
                                       const ExtensionSet& ext_set) = 0;
};

class DefaultExtensionProvider : public BaseExtensionProvider {
 public:
  Result<RelationInfo> MakeRel(const ConversionOptions& conv_opts,
                               const std::vector<DeclarationInfo>& inputs,
                               const google::protobuf::Any& rel,
                               const ExtensionSet& ext_set) override {
    if (rel.Is<substrait_ext::AsOfJoinRel>()) {
      substrait_ext::AsOfJoinRel as_of_join_rel;
      rel.UnpackTo(&as_of_join_rel);
      return MakeAsOfJoinRel(inputs, as_of_join_rel, ext_set);
    }
    if (rel.Is<substrait_ext::NamedTapRel>()) {
      substrait_ext::NamedTapRel named_tap_rel;
      rel.UnpackTo(&named_tap_rel);
      return MakeNamedTapRel(conv_opts, inputs, named_tap_rel, ext_set);
    }
    return Status::NotImplemented("Unrecognized extension in Susbstrait plan: ",
                                  rel.DebugString());
  }

 private:
  Result<RelationInfo> MakeAsOfJoinRel(const std::vector<DeclarationInfo>& inputs,
                                       const substrait_ext::AsOfJoinRel& as_of_join_rel,
                                       const ExtensionSet& ext_set) {
    if (inputs.size() < 2) {
      return Status::Invalid("substrait_ext::AsOfJoinNode too few input tables: ",
                             inputs.size());
    }
    if (static_cast<size_t>(as_of_join_rel.keys_size()) != inputs.size()) {
      return Status::Invalid("substrait_ext::AsOfJoinNode mismatched number of inputs");
    }

    size_t n_input = inputs.size(), i = 0;
    std::vector<compute::AsofJoinNodeOptions::Keys> input_keys(n_input);
    for (const auto& keys : as_of_join_rel.keys()) {
      // on-key
      if (!keys.has_on()) {
        return Status::Invalid("substrait_ext::AsOfJoinNode missing on-key for input ",
                               i);
      }
      ARROW_ASSIGN_OR_RAISE(auto on_key_expr, FromProto(keys.on(), ext_set, {}));
      if (on_key_expr.field_ref() == NULLPTR) {
        return Status::NotImplemented(
            "substrait_ext::AsOfJoinNode non-field-ref on-key for input ", i);
      }
      const FieldRef& on_key = *on_key_expr.field_ref();

      // by-key
      std::vector<FieldRef> by_key;
      for (const auto& by_item : keys.by()) {
        ARROW_ASSIGN_OR_RAISE(auto by_key_expr, FromProto(by_item, ext_set, {}));
        if (by_key_expr.field_ref() == NULLPTR) {
          return Status::NotImplemented(
              "substrait_ext::AsOfJoinNode non-field-ref by-key for input ", i);
        }
        by_key.push_back(*by_key_expr.field_ref());
      }

      input_keys[i] = {std::move(on_key), std::move(by_key)};
      ++i;
    }

    // schema
    int64_t tolerance = as_of_join_rel.tolerance();
    std::vector<std::shared_ptr<Schema>> input_schema(inputs.size());
    for (size_t i = 0; i < inputs.size(); i++) {
      input_schema[i] = inputs[i].output_schema;
    }
    std::vector<int> field_output_indices;
    ARROW_ASSIGN_OR_RAISE(auto schema,
                          compute::asofjoin::MakeOutputSchema(input_schema, input_keys,
                                                              &field_output_indices));
    compute::AsofJoinNodeOptions asofjoin_node_opts{std::move(input_keys), tolerance};

    // declaration
    auto input_decls = MakeDeclarationInputs(inputs);
    return RelationInfo{
        {compute::Declaration("asofjoin", input_decls, std::move(asofjoin_node_opts)),
         std::move(schema)},
        std::move(field_output_indices)};
  }

  Result<RelationInfo> MakeNamedTapRel(const ConversionOptions& conv_opts,
                                       const std::vector<DeclarationInfo>& inputs,
                                       const substrait_ext::NamedTapRel& named_tap_rel,
                                       const ExtensionSet& ext_set) {
    if (inputs.size() != 1) {
      return Status::Invalid(
          "substrait_ext::NamedTapRel requires a single input but got: ", inputs.size());
    }

    auto schema = inputs[0].output_schema;
    int num_fields = schema->num_fields();
    if (named_tap_rel.columns_size() != num_fields) {
      return Status::Invalid("Got ", named_tap_rel.columns_size(),
                             " NamedTapRel columns but expected ", num_fields);
    }
    std::vector<std::string> columns(named_tap_rel.columns().begin(),
                                     named_tap_rel.columns().end());
    ARROW_ASSIGN_OR_RAISE(auto renamed_schema, schema->WithNames(columns));
    auto input_decls = MakeDeclarationInputs(inputs);
    ARROW_ASSIGN_OR_RAISE(
        auto decl, conv_opts.named_tap_provider(named_tap_rel.kind(), input_decls,
                                                named_tap_rel.name(), renamed_schema));
    return RelationInfo{{std::move(decl), std::move(renamed_schema)}, std::nullopt};
  }
};

namespace {

template <typename T>
class ConfigurableSingleton {
 public:
  explicit ConfigurableSingleton(T new_value) : instance(std::move(new_value)) {}

  T Get() {
    std::lock_guard lk(mutex);
    return instance;
  }

  void Set(T new_value) {
    std::lock_guard lk(mutex);
    instance = std::move(new_value);
  }

 private:
  T instance;
  std::mutex mutex;
};

ConfigurableSingleton<std::shared_ptr<ExtensionProvider>>&
default_extension_provider_singleton() {
  static ConfigurableSingleton<std::shared_ptr<ExtensionProvider>> singleton(
      std::make_shared<DefaultExtensionProvider>());
  return singleton;
}

ConfigurableSingleton<NamedTapProvider>& default_named_tap_provider_singleton() {
  static ConfigurableSingleton<NamedTapProvider> singleton(
      [](const std::string& tap_kind, std::vector<compute::Declaration::Input> inputs,
         const std::string& tap_name,
         std::shared_ptr<Schema> tap_schema) -> Result<compute::Declaration> {
        return Status::NotImplemented(
            "Plan contained a NamedTapRel but no provider configured");
      });
  return singleton;
}

}  // namespace

std::shared_ptr<ExtensionProvider> default_extension_provider() {
  return default_extension_provider_singleton().Get();
}

void set_default_extension_provider(const std::shared_ptr<ExtensionProvider>& provider) {
  default_extension_provider_singleton().Set(provider);
}

NamedTapProvider default_named_tap_provider() {
  return default_named_tap_provider_singleton().Get();
}

void set_default_named_tap_provider(NamedTapProvider provider) {
  default_named_tap_provider_singleton().Set(std::move(provider));
}

}  // namespace engine
}  // namespace arrow
