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

#include "arrow/engine/substrait_consumer.h"

#include "arrow/engine/protocol_internal.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace engine {

Result<compute::Declaration> Convert(const st::Rel& relation) {
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

Result<std::shared_ptr<DataType>> DeserializeType(const Buffer& buf) {
  ARROW_ASSIGN_OR_RAISE(auto type, ParseFromBuffer<st::Type>(buf));
  ARROW_ASSIGN_OR_RAISE(auto type_nullable, FromProto(type));
  return std::move(type_nullable.first);
}

Result<std::shared_ptr<Buffer>> SerializeType(const DataType& type) {
  ARROW_ASSIGN_OR_RAISE(auto st_type, ToProto(type));
  std::string serialized = st_type->SerializeAsString();
  return Buffer::FromString(std::move(serialized));
}

}  // namespace engine
}  // namespace arrow
