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

#include "gtest/gtest.h"

#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/key_value_metadata.h"

#include "parquet/api/writer.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_writer.h"
#include "parquet/test_util.h"

namespace parquet::arrow {

TEST(Metadata, AppendMetadata) {
  // A sample table, type and structure does not matter in this test case
  auto schema = ::arrow::schema({::arrow::field("f", ::arrow::utf8())});
  auto table = ::arrow::Table::Make(
      schema, {::arrow::ArrayFromJSON(::arrow::utf8(), R"(["a", "b", "c"])")});

  auto sink = CreateOutputStream();
  ArrowWriterProperties::Builder builder;
  builder.store_schema();
  ASSERT_OK_AND_ASSIGN(auto writer,
                       parquet::arrow::FileWriter::Open(
                           *schema, ::arrow::default_memory_pool(), sink,
                           parquet::default_writer_properties(), builder.build()));

  auto kv_meta = std::make_shared<KeyValueMetadata>();
  kv_meta->Append("test_key_1", "test_value_1");
  // <test_key_2, test_value_2_temp> would be overwritten later.
  kv_meta->Append("test_key_2", "test_value_2_temp");
  ASSERT_OK(writer->AddKeyValueMetadata(kv_meta));

  // Key value metadata that will be added to the file.
  auto kv_meta_added = std::make_shared<::arrow::KeyValueMetadata>();
  kv_meta_added->Append("test_key_2", "test_value_2");
  kv_meta_added->Append("test_key_3", "test_value_3");

  ASSERT_OK(writer->AddKeyValueMetadata(kv_meta_added));
  ASSERT_OK(writer->Close());

  // return error if the file is closed
  ASSERT_RAISES(IOError, writer->AddKeyValueMetadata(kv_meta_added));

  auto verify_key_value_metadata =
      [&](const std::shared_ptr<const KeyValueMetadata>& key_value_metadata) {
        ASSERT_TRUE(nullptr != key_value_metadata);

        // Verify keys that were added before file writer was closed are present.
        for (int i = 1; i <= 3; ++i) {
          auto index = std::to_string(i);
          PARQUET_ASSIGN_OR_THROW(auto value,
                                  key_value_metadata->Get("test_key_" + index));
          EXPECT_EQ("test_value_" + index, value);
        }
        EXPECT_TRUE(key_value_metadata->Contains("ARROW:schema"));
      };
  // verify the metadata in writer
  verify_key_value_metadata(writer->metadata()->key_value_metadata());

  ASSERT_OK(writer->Close());

  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());
  // verify the metadata in reader
  {
    std::unique_ptr<FileReader> reader;
    FileReaderBuilder reader_builder;
    ASSERT_OK_NO_THROW(
        reader_builder.Open(std::make_shared<::arrow::io::BufferReader>(buffer)));
    ASSERT_OK(
        reader_builder.properties(default_arrow_reader_properties())->Build(&reader));

    verify_key_value_metadata(reader->parquet_reader()->metadata()->key_value_metadata());
  }
}

}  // namespace parquet::arrow
