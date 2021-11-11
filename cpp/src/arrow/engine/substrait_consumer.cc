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

#include "arrow/util/string_view.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"

#include "generated/substrait/plan.pb.h"

namespace st = io::substrait;

namespace arrow {
namespace engine {

Result<std::vector<compute::Declaration>> ConvertPlan(const Buffer& buf) {
  st::Plan plan;

  google::protobuf::io::ArrayInputStream istream{buf.data(),
                                                 static_cast<int>(buf.size())};
  if (!plan.ParseFromZeroCopyStream(&istream)) {
    return Status::Invalid("Not a valid plan");
  }

  return Status::NotImplemented("");
}

}  // namespace engine
}  // namespace arrow
