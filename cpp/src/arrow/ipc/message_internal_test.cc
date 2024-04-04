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

#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <memory>

#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/metadata_internal.cc"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace ipc {
namespace internal {

using FBB = flatbuffers::FlatBufferBuilder;

// GH-40361: Test that Flatbuffer serialization matches a known output
// byte-for-byte.
//
// Our Flatbuffers code should not depend on argument evaluation order as it's
// undefined (https://en.cppreference.com/w/cpp/language/eval_order) and may
// lead to unnecessary platform- or toolchain-specific differences in
// serialization.
TEST(TestMessageInternal, TestByteIdentical) {
  FBB fbb;
  flatbuffers::Offset<flatbuf::Schema> fb_schema;
  DictionaryFieldMapper mapper;

  // Create a simple Schema
  auto f0 = field("f0", int64());
  std::vector<std::shared_ptr<Field>> fields = {f0};
  auto schema = ::arrow::schema({f0});

  // Serialize the Schema to a Buffer
  std::shared_ptr<Buffer> outBuffer;
  ASSERT_OK(WriteSchemaMessage(*schema, mapper, IpcWriteOptions::Defaults(), &outBuffer));

  // This is example output from macOS+ARM+LLVM
  const int8_t len = 120;
  const uint8_t expected[] = {
      0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x0C, 0x00, 0x06, 0x00, 0x05, 0x00,
      0x08, 0x00, 0x0A, 0x00, 0x00, 0x00, 0x00, 0x01, 0x04, 0x00, 0x0C, 0x00, 0x00, 0x00,
      0x08, 0x00, 0x08, 0x00, 0x00, 0x00, 0x04, 0x00, 0x08, 0x00, 0x00, 0x00, 0x04, 0x00,
      0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x10, 0x00, 0x14, 0x00,
      0x08, 0x00, 0x06, 0x00, 0x07, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x10, 0x00, 0x10, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x10, 0x00, 0x00, 0x00, 0x1C, 0x00, 0x00, 0x00,
      0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x66, 0x30,
      0x00, 0x00, 0x08, 0x00, 0x0C, 0x00, 0x08, 0x00, 0x07, 0x00, 0x08, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x01, 0x40, 0x00, 0x00, 0x00};
  Buffer expectedBuffer(expected, len);

  ASSERT_TRUE(expectedBuffer.Equals(*outBuffer));
}
}  // namespace internal
}  // namespace ipc
}  // namespace arrow
