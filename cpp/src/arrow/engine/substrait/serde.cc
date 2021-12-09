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
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/util/string_view.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "google/protobuf/message.h"

#include "generated/substrait/plan.pb.h"

namespace google {
namespace protobuf {

class Message;

}  // namespace protobuf
}  // namespace google

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

Result<compute::Declaration> Convert(const st::PlanRel& relation) {
  return Status::NotImplemented("");
}

Result<std::vector<compute::Declaration>> ConvertPlan(const Buffer& buf) {
  ARROW_ASSIGN_OR_RAISE(auto plan, ParseFromBuffer<st::Plan>(buf));

  std::vector<compute::Declaration> decls;
  for (const auto& relation : plan.relations()) {
    ARROW_ASSIGN_OR_RAISE(auto decl, Convert(relation));
    decls.push_back(std::move(decl));
  }

  return decls;
}

Result<std::shared_ptr<Schema>> DeserializeSchema(const Buffer& buf) {
  ARROW_ASSIGN_OR_RAISE(auto named_struct, ParseFromBuffer<st::NamedStruct>(buf));
  return FromProto(named_struct);
}

Result<std::shared_ptr<Buffer>> SerializeSchema(const Schema& schema) {
  ARROW_ASSIGN_OR_RAISE(auto named_struct, ToProto(schema));
  std::string serialized = named_struct->SerializeAsString();
  return Buffer::FromString(std::move(serialized));
}

Result<std::shared_ptr<DataType>> DeserializeType(const Buffer& buf) {
  ARROW_ASSIGN_OR_RAISE(auto type, ParseFromBuffer<st::Type>(buf));
  ARROW_ASSIGN_OR_RAISE(auto type_nullable, FromProto(type));
  return std::move(type_nullable.first);
}

Result<std::shared_ptr<Buffer>> SerializeType(const DataType& type,
                                              ExtensionSet* ext_set) {
  ARROW_ASSIGN_OR_RAISE(auto st_type, ToProto(type, /*nullable=*/true, ext_set));
  std::string serialized = st_type->SerializeAsString();
  return Buffer::FromString(std::move(serialized));
}

Result<compute::Expression> DeserializeExpression(const Buffer& buf) {
  ARROW_ASSIGN_OR_RAISE(auto expr, ParseFromBuffer<st::Expression>(buf));
  return FromProto(expr);
}

Result<std::shared_ptr<Buffer>> SerializeExpression(const compute::Expression& expr) {
  ARROW_ASSIGN_OR_RAISE(auto st_expr, ToProto(expr));
  std::string serialized = st_expr->SerializeAsString();
  return Buffer::FromString(std::move(serialized));
}

}  // namespace engine
}  // namespace arrow
