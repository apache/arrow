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

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace engine {

TEST(SubstraitConsumption, BasicTypeRoundTrip) {
  for (auto type : {
           boolean(),
           int8(),
           int16(),
           int32(),
           int64(),
           float32(),
           float64(),
           struct_({
               field("", int64()),
               field("", list(utf8())),
           }),
       }) {
    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeType(*type));
    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeType(*serialized));
    ASSERT_EQ(*roundtripped, *type);
  }
}

}  // namespace engine
}  // namespace arrow

