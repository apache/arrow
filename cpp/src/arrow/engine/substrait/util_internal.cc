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

#include "arrow/engine/substrait/util_internal.h"

#include "arrow/config.h"
#include "arrow/engine/substrait/util.h"

namespace arrow {

namespace engine {

std::string EnumToString(int value, const google::protobuf::EnumDescriptor& descriptor) {
  const google::protobuf::EnumValueDescriptor* value_desc =
      descriptor.FindValueByNumber(value);
  if (value_desc == nullptr) {
    return "unknown";
  }
  return value_desc->name();
}

std::unique_ptr<substrait::Version> CreateVersion() {
  auto version = std::make_unique<substrait::Version>();
  version->set_major_number(kSubstraitMajorVersion);
  version->set_minor_number(kSubstraitMinorVersion);
  version->set_patch_number(kSubstraitPatchVersion);
  version->set_producer("Acero " + GetBuildInfo().version_string);
  return version;
}

}  // namespace engine

}  // namespace arrow
