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

#include "arrow/engine/substrait/serde.h"

#include <cstdint>
#include <type_traits>
#include <utility>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/util/message_differencer.h>
#include <google/protobuf/util/type_resolver.h>
#include <google/protobuf/util/type_resolver_util.h>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/buffer.h"
#include "arrow/compute/expression.h"
#include "arrow/dataset/file_base.h"
#include "arrow/engine/substrait/expression_internal.h"
#include "arrow/engine/substrait/extended_expression_internal.h"
#include "arrow/engine/substrait/extension_set.h"
#include "arrow/engine/substrait/plan_internal.h"
#include "arrow/engine/substrait/relation.h"
#include "arrow/engine/substrait/relation_internal.h"
#include "arrow/engine/substrait/type_fwd.h"
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/engine/substrait/util.h"
#include "arrow/type.h"

namespace arrow {
namespace engine {

Status ParseFromBufferImpl(const Buffer& buf, const std::string& full_name,
                           google::protobuf::Message* message) {
  google::protobuf::io::ArrayInputStream buf_stream{buf.data(),
                                                    static_cast<int>(buf.size())};

  if (message->ParseFromZeroCopyStream(&buf_stream)) {
    return Status::OK();
  }
  return Status::IOError("ParseFromZeroCopyStream failed for ", full_name);
}

template <typename Message>
Result<Message> ParseFromBuffer(const Buffer& buf) {
  Message message;
  ARROW_RETURN_NOT_OK(
      ParseFromBufferImpl(buf, Message::descriptor()->full_name(), &message));
  return message;
}

Result<std::shared_ptr<Buffer>> SerializePlan(
    const acero::Declaration& declaration, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  ARROW_ASSIGN_OR_RAISE(auto subs_plan,
                        PlanToProto(declaration, ext_set, conversion_options));
  std::string serialized = subs_plan->SerializeAsString();
  return Buffer::FromString(std::move(serialized));
}

Result<std::shared_ptr<Buffer>> SerializeExpressions(
    const BoundExpressions& bound_expressions,
    const ConversionOptions& conversion_options, ExtensionSet* ext_set) {
  ExtensionSet throwaway_ext_set;
  if (ext_set == nullptr) {
    ext_set = &throwaway_ext_set;
  }
  ARROW_ASSIGN_OR_RAISE(
      std::unique_ptr<substrait::ExtendedExpression> extended_expression,
      ToProto(bound_expressions, ext_set, conversion_options));
  std::string serialized = extended_expression->SerializeAsString();
  return Buffer::FromString(std::move(serialized));
}

Result<std::shared_ptr<Buffer>> SerializeRelation(
    const acero::Declaration& declaration, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  ARROW_ASSIGN_OR_RAISE(auto relation, ToProto(declaration, ext_set, conversion_options));
  std::string serialized = relation->SerializeAsString();
  return Buffer::FromString(std::move(serialized));
}

Result<acero::Declaration> DeserializeRelation(
    const Buffer& buf, const ExtensionSet& ext_set,
    const ConversionOptions& conversion_options) {
  ARROW_ASSIGN_OR_RAISE(auto rel, ParseFromBuffer<substrait::Rel>(buf));
  ARROW_ASSIGN_OR_RAISE(auto decl_info, FromProto(rel, ext_set, conversion_options));
  return std::move(decl_info.declaration);
}

using DeclarationFactory = std::function<Result<acero::Declaration>(
    acero::Declaration, std::vector<std::string> names)>;

namespace {

DeclarationFactory MakeConsumingSinkDeclarationFactory(
    const ConsumerFactory& consumer_factory) {
  return [&consumer_factory](
             acero::Declaration input,
             std::vector<std::string> names) -> Result<acero::Declaration> {
    std::shared_ptr<acero::SinkNodeConsumer> consumer = consumer_factory();
    if (consumer == nullptr) {
      return Status::Invalid("consumer factory is exhausted");
    }
    std::shared_ptr<acero::ExecNodeOptions> options =
        std::make_shared<acero::ConsumingSinkNodeOptions>(
            acero::ConsumingSinkNodeOptions{std::move(consumer), std::move(names)});
    return acero::Declaration::Sequence({std::move(input), {"consuming_sink", options}});
  };
}

DeclarationFactory MakeWriteDeclarationFactory(
    const WriteOptionsFactory& write_options_factory) {
  return [&write_options_factory](
             acero::Declaration input,
             std::vector<std::string> names) -> Result<acero::Declaration> {
    std::shared_ptr<dataset::WriteNodeOptions> options = write_options_factory();
    if (options == nullptr) {
      return Status::Invalid("write options factory is exhausted");
    }
    return acero::Declaration::Sequence(
        {std::move(input), {"write", std::move(*options)}});
  };
}

Result<std::vector<acero::Declaration>> DeserializePlans(
    const Buffer& buf, DeclarationFactory declaration_factory,
    const ExtensionIdRegistry* registry, ExtensionSet* ext_set_out,
    const ConversionOptions& conversion_options) {
  ARROW_ASSIGN_OR_RAISE(auto plan, ParseFromBuffer<substrait::Plan>(buf));

  ARROW_RETURN_NOT_OK(
      CheckVersion(plan.version().major_number(), plan.version().minor_number()));

  ARROW_ASSIGN_OR_RAISE(auto ext_set,
                        GetExtensionSetFromPlan(plan, conversion_options, registry));

  std::vector<acero::Declaration> sink_decls;
  for (const substrait::PlanRel& plan_rel : plan.relations()) {
    ARROW_ASSIGN_OR_RAISE(
        auto decl_info,
        FromProto(plan_rel.has_root() ? plan_rel.root().input() : plan_rel.rel(), ext_set,
                  conversion_options));
    std::vector<std::string> names;
    if (plan_rel.has_root()) {
      names.assign(plan_rel.root().names().begin(), plan_rel.root().names().end());
    }
    if (names.size() > 0) {
      if (decl_info.output_schema->num_fields() != plan_rel.root().names_size()) {
        return Status::Invalid("Substrait plan has ", plan_rel.root().names_size(),
                               " names that cannot be applied to extension schema:\n",
                               decl_info.output_schema->ToString(false));
      }
    }

    // pipe each relation
    ARROW_ASSIGN_OR_RAISE(
        auto sink_decl,
        declaration_factory(std::move(decl_info.declaration), std::move(names)));
    sink_decls.push_back(std::move(sink_decl));
  }

  if (ext_set_out) {
    *ext_set_out = std::move(ext_set);
  }
  return sink_decls;
}

}  // namespace

Result<std::vector<acero::Declaration>> DeserializePlans(
    const Buffer& buf, const ConsumerFactory& consumer_factory,
    const ExtensionIdRegistry* registry, ExtensionSet* ext_set_out,
    const ConversionOptions& conversion_options) {
  return DeserializePlans(buf, MakeConsumingSinkDeclarationFactory(consumer_factory),
                          registry, ext_set_out, conversion_options);
}

Result<std::vector<acero::Declaration>> DeserializePlans(
    const Buffer& buf, const WriteOptionsFactory& write_options_factory,
    const ExtensionIdRegistry* registry, ExtensionSet* ext_set_out,
    const ConversionOptions& conversion_options) {
  return DeserializePlans(buf, MakeWriteDeclarationFactory(write_options_factory),
                          registry, ext_set_out, conversion_options);
}

ARROW_ENGINE_EXPORT Result<PlanInfo> DeserializePlan(
    const Buffer& buf, const ExtensionIdRegistry* registry, ExtensionSet* ext_set_out,
    const ConversionOptions& conversion_options) {
  ARROW_ASSIGN_OR_RAISE(auto plan, ParseFromBuffer<substrait::Plan>(buf));
  ARROW_RETURN_NOT_OK(
      CheckVersion(plan.version().major_number(), plan.version().minor_number()));

  ARROW_ASSIGN_OR_RAISE(auto ext_set,
                        GetExtensionSetFromPlan(plan, conversion_options, registry));

  if (plan.relations_size() == 0) {
    return Status::Invalid("Plan has no relations");
  }
  if (plan.relations_size() > 1) {
    return Status::NotImplemented("Common sub-plans");
  }
  const substrait::PlanRel& root_rel = plan.relations(0);

  ARROW_ASSIGN_OR_RAISE(
      auto decl_info,
      FromProto(root_rel.has_root() ? root_rel.root().input() : root_rel.rel(), ext_set,
                conversion_options));

  std::vector<std::string> names;
  if (root_rel.has_root()) {
    names.assign(root_rel.root().names().begin(), root_rel.root().names().end());
    ARROW_ASSIGN_OR_RAISE(decl_info.output_schema,
                          decl_info.output_schema->WithNames(names));
  }

  if (ext_set_out) {
    *ext_set_out = std::move(ext_set);
  }

  return PlanInfo{std::move(decl_info), std::move(names)};
}

Result<BoundExpressions> DeserializeExpressions(
    const Buffer& buf, const ExtensionIdRegistry* registry,
    const ConversionOptions& conversion_options, ExtensionSet* ext_set_out) {
  ARROW_ASSIGN_OR_RAISE(auto extended_expression,
                        ParseFromBuffer<substrait::ExtendedExpression>(buf));
  return FromProto(extended_expression, ext_set_out, conversion_options, registry);
}

namespace {

Result<std::shared_ptr<acero::ExecPlan>> MakeSingleDeclarationPlan(
    std::vector<acero::Declaration> declarations) {
  if (declarations.size() > 1) {
    return Status::Invalid("DeserializePlan does not support multiple root relations");
  } else {
    ARROW_ASSIGN_OR_RAISE(auto plan, acero::ExecPlan::Make());
    ARROW_RETURN_NOT_OK(declarations[0].AddToPlan(plan.get()));
    return plan;
  }
}

}  // namespace

Result<std::shared_ptr<acero::ExecPlan>> DeserializePlan(
    const Buffer& buf, const std::shared_ptr<acero::SinkNodeConsumer>& consumer,
    const ExtensionIdRegistry* registry, ExtensionSet* ext_set_out,
    const ConversionOptions& conversion_options) {
  struct SingleConsumer {
    std::shared_ptr<acero::SinkNodeConsumer> operator()() {
      if (factory_done) {
        Status::Invalid("SingleConsumer invoked more than once").Warn();
        return std::shared_ptr<acero::SinkNodeConsumer>{};
      }
      factory_done = true;
      return consumer;
    }
    bool factory_done;
    std::shared_ptr<acero::SinkNodeConsumer> consumer;
  };
  ARROW_ASSIGN_OR_RAISE(auto declarations,
                        DeserializePlans(buf, SingleConsumer{false, consumer}, registry,
                                         ext_set_out, conversion_options));
  return MakeSingleDeclarationPlan(declarations);
}

Result<std::shared_ptr<acero::ExecPlan>> DeserializePlan(
    const Buffer& buf, const std::shared_ptr<dataset::WriteNodeOptions>& write_options,
    const ExtensionIdRegistry* registry, ExtensionSet* ext_set_out,
    const ConversionOptions& conversion_options) {
  bool factory_done = false;
  auto single_write_options = [&factory_done, &write_options] {
    if (factory_done) {
      return std::shared_ptr<dataset::WriteNodeOptions>{};
    }
    factory_done = true;
    return write_options;
  };
  ARROW_ASSIGN_OR_RAISE(auto declarations,
                        DeserializePlans(buf, single_write_options, registry, ext_set_out,
                                         conversion_options));
  return MakeSingleDeclarationPlan(declarations);
}

Result<std::shared_ptr<Schema>> DeserializeSchema(
    const Buffer& buf, const ExtensionSet& ext_set,
    const ConversionOptions& conversion_options) {
  ARROW_ASSIGN_OR_RAISE(auto named_struct, ParseFromBuffer<substrait::NamedStruct>(buf));
  return FromProto(named_struct, ext_set, conversion_options);
}

Result<std::shared_ptr<Buffer>> SerializeSchema(
    const Schema& schema, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  ARROW_ASSIGN_OR_RAISE(auto named_struct, ToProto(schema, ext_set, conversion_options));
  std::string serialized = named_struct->SerializeAsString();
  return Buffer::FromString(std::move(serialized));
}

Result<std::shared_ptr<DataType>> DeserializeType(
    const Buffer& buf, const ExtensionSet& ext_set,
    const ConversionOptions& conversion_options) {
  ARROW_ASSIGN_OR_RAISE(auto type, ParseFromBuffer<substrait::Type>(buf));
  ARROW_ASSIGN_OR_RAISE(auto type_nullable, FromProto(type, ext_set, conversion_options));
  return std::move(type_nullable.first);
}

Result<std::shared_ptr<Buffer>> SerializeType(
    const DataType& type, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  ARROW_ASSIGN_OR_RAISE(auto st_type,
                        ToProto(type, /*nullable=*/true, ext_set, conversion_options));
  std::string serialized = st_type->SerializeAsString();
  return Buffer::FromString(std::move(serialized));
}

Result<compute::Expression> DeserializeExpression(
    const Buffer& buf, const ExtensionSet& ext_set,
    const ConversionOptions& conversion_options) {
  ARROW_ASSIGN_OR_RAISE(auto expr, ParseFromBuffer<substrait::Expression>(buf));
  return FromProto(expr, ext_set, conversion_options);
}

Result<std::shared_ptr<Buffer>> SerializeExpression(
    const compute::Expression& expr, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  ARROW_ASSIGN_OR_RAISE(auto st_expr, ToProto(expr, ext_set, conversion_options));
  std::string serialized = st_expr->SerializeAsString();
  return Buffer::FromString(std::move(serialized));
}

namespace internal {

template <typename Message>
static Status CheckMessagesEquivalent(const Buffer& l_buf, const Buffer& r_buf) {
  ARROW_ASSIGN_OR_RAISE(auto l, ParseFromBuffer<Message>(l_buf));
  ARROW_ASSIGN_OR_RAISE(auto r, ParseFromBuffer<Message>(r_buf));

  using google::protobuf::util::MessageDifferencer;

  std::string out;
  google::protobuf::io::StringOutputStream out_stream{&out};
  MessageDifferencer::StreamReporter reporter{&out_stream};

  MessageDifferencer differencer;
  differencer.set_message_field_comparison(MessageDifferencer::EQUIVALENT);
  differencer.ReportDifferencesTo(&reporter);

  if (differencer.Compare(l, r)) {
    return Status::OK();
  }
  return Status::Invalid("Messages were not equivalent: ", out);
}

Status CheckMessagesEquivalent(std::string_view message_name, const Buffer& l_buf,
                               const Buffer& r_buf) {
  if (message_name == "Type") {
    return CheckMessagesEquivalent<substrait::Type>(l_buf, r_buf);
  }

  if (message_name == "NamedStruct") {
    return CheckMessagesEquivalent<substrait::NamedStruct>(l_buf, r_buf);
  }

  if (message_name == "Schema") {
    return Status::Invalid(
        "There is no substrait message named Schema. The substrait message type which "
        "corresponds to Schemas is NamedStruct");
  }

  if (message_name == "Expression") {
    return CheckMessagesEquivalent<substrait::Expression>(l_buf, r_buf);
  }

  if (message_name == "Rel") {
    return CheckMessagesEquivalent<substrait::Rel>(l_buf, r_buf);
  }

  if (message_name == "Relation") {
    return Status::Invalid(
        "There is no substrait message named Relation. You probably meant \"Rel\"");
  }

  return Status::Invalid("Unsupported message name ", message_name,
                         " for CheckMessagesEquivalent");
}

inline google::protobuf::util::TypeResolver* GetGeneratedTypeResolver() {
  static std::unique_ptr<google::protobuf::util::TypeResolver> type_resolver;
  if (!type_resolver) {
    type_resolver.reset(google::protobuf::util::NewTypeResolverForDescriptorPool(
        /*url_prefix=*/"", google::protobuf::DescriptorPool::generated_pool()));
  }
  return type_resolver.get();
}

Result<std::shared_ptr<Buffer>> SubstraitFromJSON(std::string_view type_name,
                                                  std::string_view json,
                                                  bool ignore_unknown_fields) {
  std::string type_url = "/substrait." + std::string(type_name);

  google::protobuf::io::ArrayInputStream json_stream{json.data(),
                                                     static_cast<int>(json.size())};

  std::string out;
  google::protobuf::io::StringOutputStream out_stream{&out};
  google::protobuf::util::JsonParseOptions json_opts;
  json_opts.ignore_unknown_fields = ignore_unknown_fields;
  auto status = google::protobuf::util::JsonToBinaryStream(
      GetGeneratedTypeResolver(), type_url, &json_stream, &out_stream,
      std::move(json_opts));

  if (!status.ok()) {
    return Status::Invalid("JsonToBinaryStream returned ", status);
  }
  return Buffer::FromString(std::move(out));
}

Result<std::string> SubstraitToJSON(std::string_view type_name, const Buffer& buf) {
  std::string type_url = "/substrait." + std::string(type_name);

  google::protobuf::io::ArrayInputStream buf_stream{buf.data(),
                                                    static_cast<int>(buf.size())};

  std::string out;
  google::protobuf::io::StringOutputStream out_stream{&out};

  auto status = google::protobuf::util::BinaryToJsonStream(
      GetGeneratedTypeResolver(), type_url, &buf_stream, &out_stream);
  if (!status.ok()) {
    return Status::Invalid("BinaryToJsonStream returned ", status);
  }
  return out;
}

}  // namespace internal
}  // namespace engine
}  // namespace arrow
