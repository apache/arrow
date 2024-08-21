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

#include "arrow/extension/bool8.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {

TEST(Bool8Type, Basics) {
  auto type = internal::checked_pointer_cast<extension::Bool8Type>(extension::bool8());
  auto type2 = internal::checked_pointer_cast<extension::Bool8Type>(extension::bool8());
  ASSERT_EQ("arrow.bool8", type->extension_name());
  ASSERT_EQ(*type, *type);
  ASSERT_NE(*arrow::null(), *type);
  ASSERT_EQ(*type, *type2);
  ASSERT_EQ(*arrow::int8(), *type->storage_type());
  ASSERT_EQ("", type->Serialize());
  ASSERT_EQ("extension<arrow.bool8>", type->ToString(false));
}

TEST(Bool8Type, CreateFromArray) {
  auto type = internal::checked_pointer_cast<extension::Bool8Type>(extension::bool8());
  auto storage = ArrayFromJSON(int8(), "[-1,0,1,2,null]");
  auto array = ExtensionType::WrapArray(type, storage);
  ASSERT_EQ(5, array->length());
  ASSERT_EQ(1, array->null_count());
}

TEST(Bool8Type, Deserialize) {
  auto type = internal::checked_pointer_cast<extension::Bool8Type>(extension::bool8());
  ASSERT_OK_AND_ASSIGN(auto deserialized, type->Deserialize(type->storage_type(), ""));
  ASSERT_EQ(*type, *deserialized);
  ASSERT_NOT_OK(type->Deserialize(type->storage_type(), "must be empty"));
  ASSERT_EQ(*type, *deserialized);
  ASSERT_NOT_OK(type->Deserialize(uint8(), ""));
  ASSERT_EQ(*type, *deserialized);
}

TEST(Bool8Type, MetadataRoundTrip) {
  auto type = internal::checked_pointer_cast<extension::Bool8Type>(extension::bool8());
  std::string serialized = type->Serialize();
  ASSERT_OK_AND_ASSIGN(auto deserialized,
                       type->Deserialize(type->storage_type(), serialized));
  ASSERT_EQ(*type, *deserialized);
}

TEST(Bool8Type, BatchRoundTrip) {
  auto type = internal::checked_pointer_cast<extension::Bool8Type>(extension::bool8());

  auto storage = ArrayFromJSON(int8(), "[-1,0,1,2,null]");
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
