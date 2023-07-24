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

#include <gtest/gtest.h>

#include <google/protobuf/message_lite.h>
#include <google/protobuf/stubs/common.h>

namespace arrow {
namespace engine {

// A global test "environment", to ensure that the Protobuf API is finalized after
// running unit tests by invoking google::protobuf::ShutdownProtobufLibrary.
// This will prevent leaks in valgrind tests because it will delete the global objects
// that were allocated by the Protocol Buffer library (see
// "protobuf::ShutdownProtobufLibrary" in
// https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.message_lite#ShutdownProtobufLibrary.details)

class ProtobufEnvironment : public ::testing::Environment {
 public:
  void SetUp() override { GOOGLE_PROTOBUF_VERIFY_VERSION; }
  void TearDown() override { google::protobuf::ShutdownProtobufLibrary(); }
};

::testing::Environment* protobuf_env =
    ::testing::AddGlobalTestEnvironment(new ProtobufEnvironment);

}  // namespace engine
}  // namespace arrow
