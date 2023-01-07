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
#include <iostream>

#include "arrow/engine/substrait/options.h"

#include <google/protobuf/util/json_util.h>
#include "arrow/compute/exec/asof_join_node.h"
#include "arrow/compute/exec/options.h"
#include "arrow/engine/substrait/expression_internal.h"
#include "arrow/engine/substrait/options_internal.h"
#include "arrow/engine/substrait/relation_internal.h"
#include "substrait/extension_rels.pb.h"

namespace arrow {
namespace engine {

class DefaultExtensionProvider : public ExtensionProvider {
 public:
  Result<DeclarationInfo> MakeRel(const std::vector<DeclarationInfo>& inputs,
                                  const google::protobuf::Any& rel,
                                  const ExtensionSet& ext_set) override {
    if (rel.Is<arrow::substrait_ext::AsOfJoinRel>()) {
      arrow::substrait_ext::AsOfJoinRel as_of_join_rel;
      rel.UnpackTo(&as_of_join_rel);
      return MakeAsOfJoinRel(inputs, as_of_join_rel, ext_set);
    }
    return Status::NotImplemented("Unrecognized extension in Susbstrait plan: ",
                                  rel.DebugString());
  }

 private:
  Result<DeclarationInfo> MakeAsOfJoinRel(
      const std::vector<DeclarationInfo>& inputs,
      const arrow::substrait_ext::AsOfJoinRel& as_of_join_rel,
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
    ARROW_ASSIGN_OR_RAISE(auto schema,
                          compute::asofjoin::MakeOutputSchema(input_schema, input_keys));
    compute::AsofJoinNodeOptions asofjoin_node_opts{std::move(input_keys), tolerance};

    // declaration
    std::vector<compute::Declaration::Input> input_decls(inputs.size());
    for (size_t i = 0; i < inputs.size(); i++) {
      input_decls[i] = inputs[i].declaration;
    }
    return DeclarationInfo{
        compute::Declaration("asofjoin", input_decls, std::move(asofjoin_node_opts)),
        std::move(schema)};
  }
};

std::shared_ptr<ExtensionProvider> ExtensionProvider::kDefaultExtensionProvider =
    std::make_shared<DefaultExtensionProvider>();

std::shared_ptr<ExtensionProvider> default_extension_provider() {
  return ExtensionProvider::kDefaultExtensionProvider;
}

}  // namespace engine
}  // namespace arrow
