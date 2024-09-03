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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/extension/fixed_shape_tensor.h"
#include "arrow/extension/opaque.h"
#include "arrow/extension_type.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

TEST(OpaqueType, Basics) {
  auto type = internal::checked_pointer_cast<extension::OpaqueType>(
      extension::opaque(null(), "type", "vendor"));
  auto type2 = internal::checked_pointer_cast<extension::OpaqueType>(
      extension::opaque(null(), "type2", "vendor"));
  ASSERT_EQ("arrow.opaque", type->extension_name());
  ASSERT_EQ(*type, *type);
  ASSERT_NE(*arrow::null(), *type);
  ASSERT_NE(*type, *type2);
  ASSERT_EQ(*arrow::null(), *type->storage_type());
  ASSERT_THAT(type->Serialize(), ::testing::Not(::testing::IsEmpty()));
  ASSERT_EQ(R"({"type_name":"type","vendor_name":"vendor"})", type->Serialize());
  ASSERT_EQ("type", type->type_name());
  ASSERT_EQ("vendor", type->vendor_name());
  ASSERT_EQ(
      "extension<arrow.opaque[storage_type=null, type_name=type, vendor_name=vendor]>",
      type->ToString(false));
}

TEST(OpaqueType, Equals) {
  auto type = internal::checked_pointer_cast<extension::OpaqueType>(
      extension::opaque(null(), "type", "vendor"));
  auto type2 = internal::checked_pointer_cast<extension::OpaqueType>(
      extension::opaque(null(), "type2", "vendor"));
  auto type3 = internal::checked_pointer_cast<extension::OpaqueType>(
      extension::opaque(null(), "type", "vendor2"));
  auto type4 = internal::checked_pointer_cast<extension::OpaqueType>(
      extension::opaque(int64(), "type", "vendor"));
  auto type5 = internal::checked_pointer_cast<extension::OpaqueType>(
      extension::opaque(null(), "type", "vendor"));
  auto type6 = internal::checked_pointer_cast<extension::FixedShapeTensorType>(
      extension::fixed_shape_tensor(float64(), {1}));

  ASSERT_EQ(*type, *type);
  ASSERT_EQ(*type2, *type2);
  ASSERT_EQ(*type3, *type3);
  ASSERT_EQ(*type4, *type4);
  ASSERT_EQ(*type5, *type5);

  ASSERT_EQ(*type, *type5);

  ASSERT_NE(*type, *type2);
  ASSERT_NE(*type, *type3);
  ASSERT_NE(*type, *type4);
  ASSERT_NE(*type, *type6);

  ASSERT_NE(*type2, *type);
  ASSERT_NE(*type2, *type3);
  ASSERT_NE(*type2, *type4);
  ASSERT_NE(*type2, *type6);

  ASSERT_NE(*type3, *type);
  ASSERT_NE(*type3, *type2);
  ASSERT_NE(*type3, *type4);
  ASSERT_NE(*type3, *type6);

  ASSERT_NE(*type4, *type);
  ASSERT_NE(*type4, *type2);
  ASSERT_NE(*type4, *type3);
  ASSERT_NE(*type4, *type6);
  ASSERT_NE(*type6, *type4);
}

TEST(OpaqueType, CreateFromArray) {
  auto type = internal::checked_pointer_cast<extension::OpaqueType>(
      extension::opaque(binary(), "geometry", "adbc.postgresql"));
  auto storage = ArrayFromJSON(binary(), R"(["foobar", null])");
  auto array = ExtensionType::WrapArray(type, storage);
  ASSERT_EQ(2, array->length());
  ASSERT_EQ(1, array->null_count());
}

void CheckDeserialize(const std::string& serialized,
                      const std::shared_ptr<DataType>& expected) {
  auto type = internal::checked_pointer_cast<extension::OpaqueType>(expected);
  ASSERT_OK_AND_ASSIGN(auto deserialized,
                       type->Deserialize(type->storage_type(), serialized));
  ASSERT_EQ(*expected, *deserialized);
}

TEST(OpaqueType, Deserialize) {
  ASSERT_NO_FATAL_FAILURE(
      CheckDeserialize(R"({"type_name": "type", "vendor_name": "vendor"})",
                       extension::opaque(null(), "type", "vendor")));
  ASSERT_NO_FATAL_FAILURE(
      CheckDeserialize(R"({"type_name": "long name", "vendor_name": "long name"})",
                       extension::opaque(null(), "long name", "long name")));
  ASSERT_NO_FATAL_FAILURE(
      CheckDeserialize(R"({"type_name": "名前", "vendor_name": "名字"})",
                       extension::opaque(null(), "名前", "名字")));
  ASSERT_NO_FATAL_FAILURE(CheckDeserialize(
      R"({"type_name": "type", "vendor_name": "vendor", "extra_field": 2})",
      extension::opaque(null(), "type", "vendor")));

  auto type = internal::checked_pointer_cast<extension::OpaqueType>(
      extension::opaque(null(), "type", "vendor"));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("The document is empty"),
                                  type->Deserialize(null(), R"()"));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  testing::HasSubstr("Missing a name for object member"),
                                  type->Deserialize(null(), R"({)"));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("not an object"),
                                  type->Deserialize(null(), R"([])"));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("missing type_name"),
                                  type->Deserialize(null(), R"({})"));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("type_name is not a string"),
      type->Deserialize(null(), R"({"type_name": 2, "vendor_name": ""})"));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("type_name is not a string"),
      type->Deserialize(null(), R"({"type_name": null, "vendor_name": ""})"));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("vendor_name is not a string"),
      type->Deserialize(null(), R"({"vendor_name": 2, "type_name": ""})"));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("vendor_name is not a string"),
      type->Deserialize(null(), R"({"vendor_name": null, "type_name": ""})"));
}

TEST(OpaqueType, MetadataRoundTrip) {
  for (const auto& type : {
           extension::opaque(null(), "foo", "bar"),
           extension::opaque(binary(), "geometry", "postgis"),
           extension::opaque(fixed_size_list(int64(), 4), "foo", "bar"),
           extension::opaque(utf8(), "foo", "bar"),
       }) {
    auto opaque = internal::checked_pointer_cast<extension::OpaqueType>(type);
    std::string serialized = opaque->Serialize();
    ASSERT_OK_AND_ASSIGN(auto deserialized,
                         opaque->Deserialize(opaque->storage_type(), serialized));
    ASSERT_EQ(*type, *deserialized);
  }
}

TEST(OpaqueType, BatchRoundTrip) {
  auto type = internal::checked_pointer_cast<extension::OpaqueType>(
      extension::opaque(binary(), "geometry", "adbc.postgresql"));

  auto storage = ArrayFromJSON(binary(), R"(["foobar", null])");
  auto array = ExtensionType::WrapArray(type, storage);
  auto batch =
      RecordBatch::Make(schema({field("field", type)}), array->length(), {array});

  std::shared_ptr<RecordBatch> written;
  {
    ASSERT_OK_AND_ASSIGN(auto out_stream, io::BufferOutputStream::Create());
    ASSERT_OK(ipc::WriteRecordBatchStream({batch}, ipc::IpcWriteOptions::Defaults(),
                                          out_stream.get()));

    ASSERT_OK_AND_ASSIGN(auto complete_ipc_stream, out_stream->Finish());

    io::BufferReader reader(complete_ipc_stream);
    std::shared_ptr<RecordBatchReader> batch_reader;
    ASSERT_OK_AND_ASSIGN(batch_reader, ipc::RecordBatchStreamReader::Open(&reader));
    ASSERT_OK(batch_reader->ReadNext(&written));
  }

  ASSERT_EQ(*batch->schema(), *written->schema());
  ASSERT_BATCHES_EQUAL(*batch, *written);
}

}  // namespace arrow
