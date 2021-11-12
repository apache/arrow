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

// This API is EXPERIMENTAL.

#pragma once

#include <string>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/engine/visibility.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"

#include "generated/substrait/plan.pb.h"  // IWYU pragma: export

namespace st = io::substrait;

namespace google {
namespace protobuf {

class Message;

}  // namespace protobuf
}  // namespace google

namespace arrow {
namespace engine {

ARROW_ENGINE_EXPORT
Status ParseFromBufferImpl(const Buffer& buf, const std::string& full_name,
                           google::protobuf::Message* message);

template <typename Message>
Result<Message> ParseFromBuffer(const Buffer& buf) {
  Message message;
  ARROW_RETURN_NOT_OK(
      ParseFromBufferImpl(buf, Message::descriptor()->full_name(), &message));
  return message;
}

ARROW_ENGINE_EXPORT
Result<std::pair<std::shared_ptr<DataType>, bool>> FromProto(const st::Type&);

ARROW_ENGINE_EXPORT
Result<std::unique_ptr<st::Type>> ToProto(const DataType&, bool nullable = true);

}  // namespace engine
}  // namespace arrow
