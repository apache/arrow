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

#include "arrow/extension/uuid_array.h"

#include "arrow/testing/matchers.h"

#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/tensor.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/key_value_metadata.h"

#include "arrow/testing/extension_type.h"

namespace arrow {

using extension::uuid;

class TestUuuidExtensionType : public ::testing::Test {};

TEST_F(TestUuuidExtensionType, ExtensionTypeTest) {
  auto type = uuid();
  ASSERT_EQ(type->id(), Type::EXTENSION);

  const auto& ext_type = static_cast<const ExtensionType&>(*type);
  std::string serialized = ext_type.Serialize();

  ASSERT_OK_AND_ASSIGN(auto deserialized,
                       ext_type.Deserialize(fixed_size_binary(16), serialized));
  ASSERT_TRUE(deserialized->Equals(*type));
  ASSERT_FALSE(deserialized->Equals(*fixed_size_binary(16)));
}

}  // namespace arrow
