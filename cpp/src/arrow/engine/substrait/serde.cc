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

#include "arrow/engine/substrait/expression_internal.h"
#include "arrow/engine/substrait/plan_internal.h"
#include "arrow/engine/substrait/relation_internal.h"
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/util/string_view.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/util/message_differencer.h>
#include <google/protobuf/util/type_resolver_util.h>

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

Result<compute::Declaration> DeserializeRelation(const Buffer& buf,
                                                 const ExtensionSet& ext_set) {
  ARROW_ASSIGN_OR_RAISE(auto rel, ParseFromBuffer<substrait::Rel>(buf));
  return FromProto(rel, ext_set);
}

Result<std::vector<compute::Declaration>> DeserializePlan(
    const Buffer& buf, const ConsumerFactory& consumer_factory,
    ExtensionSet* ext_set_out) {
  ARROW_ASSIGN_OR_RAISE(auto plan, ParseFromBuffer<substrait::Plan>(buf));

  ARROW_ASSIGN_OR_RAISE(auto ext_set, GetExtensionSetFromPlan(plan));

  std::vector<compute::Declaration> sink_decls;
  for (const substrait::PlanRel& plan_rel : plan.relations()) {
    if (plan_rel.has_root()) {
      return Status::NotImplemented("substrait::PlanRel with custom output field names");
    }
    ARROW_ASSIGN_OR_RAISE(auto decl, FromProto(plan_rel.rel(), ext_set));

    // pipe each relation into a consuming_sink node
    auto sink_decl = compute::Declaration::Sequence({
        std::move(decl),
        {"consuming_sink", compute::ConsumingSinkNodeOptions{consumer_factory()}},
    });
    sink_decls.push_back(std::move(sink_decl));
  }

  if (ext_set_out) {
    *ext_set_out = std::move(ext_set);
  }
  return sink_decls;
}

Result<std::shared_ptr<Schema>> DeserializeSchema(const Buffer& buf,
                                                  const ExtensionSet& ext_set) {
  ARROW_ASSIGN_OR_RAISE(auto named_struct, ParseFromBuffer<substrait::NamedStruct>(buf));
  return FromProto(named_struct, ext_set);
}

Result<std::shared_ptr<Buffer>> SerializeSchema(const Schema& schema,
                                                ExtensionSet* ext_set) {
  ARROW_ASSIGN_OR_RAISE(auto named_struct, ToProto(schema, ext_set));
  std::string serialized = named_struct->SerializeAsString();
  return Buffer::FromString(std::move(serialized));
}

Result<std::shared_ptr<DataType>> DeserializeType(const Buffer& buf,
                                                  const ExtensionSet& ext_set) {
  ARROW_ASSIGN_OR_RAISE(auto type, ParseFromBuffer<substrait::Type>(buf));
  ARROW_ASSIGN_OR_RAISE(auto type_nullable, FromProto(type, ext_set));
  return std::move(type_nullable.first);
}

Result<std::shared_ptr<Buffer>> SerializeType(const DataType& type,
                                              ExtensionSet* ext_set) {
  ARROW_ASSIGN_OR_RAISE(auto st_type, ToProto(type, /*nullable=*/true, ext_set));
  std::string serialized = st_type->SerializeAsString();
  return Buffer::FromString(std::move(serialized));
}

Result<compute::Expression> DeserializeExpression(const Buffer& buf,
                                                  const ExtensionSet& ext_set) {
  ARROW_ASSIGN_OR_RAISE(auto expr, ParseFromBuffer<substrait::Expression>(buf));
  return FromProto(expr, ext_set);
}

Result<std::shared_ptr<Buffer>> SerializeExpression(const compute::Expression& expr,
                                                    ExtensionSet* ext_set) {
  ARROW_ASSIGN_OR_RAISE(auto st_expr, ToProto(expr, ext_set));
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

Status CheckMessagesEquivalent(util::string_view message_name, const Buffer& l_buf,
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

Result<std::shared_ptr<Buffer>> SubstraitFromJSON(util::string_view type_name,
                                                  util::string_view json) {
  std::string type_url = "/substrait." + type_name.to_string();

  google::protobuf::io::ArrayInputStream json_stream{json.data(),
                                                     static_cast<int>(json.size())};

  std::string out;
  google::protobuf::io::StringOutputStream out_stream{&out};

  auto status = google::protobuf::util::JsonToBinaryStream(
      GetGeneratedTypeResolver(), type_url, &json_stream, &out_stream);

  if (!status.ok()) {
    return Status::Invalid("JsonToBinaryStream returned ", status);
  }
  return Buffer::FromString(std::move(out));
}

Result<std::string> SubstraitToJSON(util::string_view type_name, const Buffer& buf) {
  std::string type_url = "/substrait." + type_name.to_string();

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
