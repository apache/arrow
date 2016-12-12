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

#include <memory>
#include <sstream>
#include <string>

#include "gtest/gtest.h"

#include "arrow/io/memory.h"
#include "arrow/ipc/metadata.h"
#include "arrow/schema.h"
#include "arrow/status.h"
#include "arrow/test-util.h"
#include "arrow/type.h"

namespace arrow {

class Buffer;

namespace ipc {

static inline void assert_schema_equal(const Schema* lhs, const Schema* rhs) {
  if (!lhs->Equals(*rhs)) {
    std::stringstream ss;
    ss << "left schema: " << lhs->ToString() << std::endl
       << "right schema: " << rhs->ToString() << std::endl;
    FAIL() << ss.str();
  }
}

class TestSchemaMetadata : public ::testing::Test {
 public:
  void SetUp() {}

  void CheckRoundtrip(const Schema* schema) {
    std::shared_ptr<Buffer> buffer;
    ASSERT_OK(WriteSchema(schema, &buffer));

    std::shared_ptr<Message> message;
    ASSERT_OK(Message::Open(buffer, 0, &message));

    ASSERT_EQ(Message::SCHEMA, message->type());

    auto schema_msg = std::make_shared<SchemaMetadata>(message);
    ASSERT_EQ(schema->num_fields(), schema_msg->num_fields());

    std::shared_ptr<Schema> schema2;
    ASSERT_OK(schema_msg->GetSchema(&schema2));

    assert_schema_equal(schema, schema2.get());
  }
};

const std::shared_ptr<DataType> INT32 = std::make_shared<Int32Type>();

TEST_F(TestSchemaMetadata, PrimitiveFields) {
  auto f0 = std::make_shared<Field>("f0", std::make_shared<Int8Type>());
  auto f1 = std::make_shared<Field>("f1", std::make_shared<Int16Type>(), false);
  auto f2 = std::make_shared<Field>("f2", std::make_shared<Int32Type>());
  auto f3 = std::make_shared<Field>("f3", std::make_shared<Int64Type>());
  auto f4 = std::make_shared<Field>("f4", std::make_shared<UInt8Type>());
  auto f5 = std::make_shared<Field>("f5", std::make_shared<UInt16Type>());
  auto f6 = std::make_shared<Field>("f6", std::make_shared<UInt32Type>());
  auto f7 = std::make_shared<Field>("f7", std::make_shared<UInt64Type>());
  auto f8 = std::make_shared<Field>("f8", std::make_shared<FloatType>());
  auto f9 = std::make_shared<Field>("f9", std::make_shared<DoubleType>(), false);
  auto f10 = std::make_shared<Field>("f10", std::make_shared<BooleanType>());

  Schema schema({f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10});
  CheckRoundtrip(&schema);
}

TEST_F(TestSchemaMetadata, NestedFields) {
  auto type = std::make_shared<ListType>(std::make_shared<Int32Type>());
  auto f0 = std::make_shared<Field>("f0", type);

  std::shared_ptr<StructType> type2(new StructType({std::make_shared<Field>("k1", INT32),
      std::make_shared<Field>("k2", INT32), std::make_shared<Field>("k3", INT32)}));
  auto f1 = std::make_shared<Field>("f1", type2);

  Schema schema({f0, f1});
  CheckRoundtrip(&schema);
}

class TestFileFooter : public ::testing::Test {
 public:
  void SetUp() {}

  void CheckRoundtrip(const Schema* schema, const std::vector<FileBlock>& dictionaries,
      const std::vector<FileBlock>& record_batches) {
    auto buffer = std::make_shared<PoolBuffer>();
    io::BufferOutputStream stream(buffer);

    ASSERT_OK(WriteFileFooter(schema, dictionaries, record_batches, &stream));

    std::unique_ptr<FileFooter> footer;
    ASSERT_OK(FileFooter::Open(buffer, &footer));

    ASSERT_EQ(MetadataVersion::V2, footer->version());

    // Check schema
    std::shared_ptr<Schema> schema2;
    ASSERT_OK(footer->GetSchema(&schema2));
    assert_schema_equal(schema, schema2.get());

    // Check blocks
    ASSERT_EQ(dictionaries.size(), footer->num_dictionaries());
    ASSERT_EQ(record_batches.size(), footer->num_record_batches());

    for (int i = 0; i < footer->num_dictionaries(); ++i) {
      CheckBlocks(dictionaries[i], footer->dictionary(i));
    }

    for (int i = 0; i < footer->num_record_batches(); ++i) {
      CheckBlocks(record_batches[i], footer->record_batch(i));
    }
  }

  void CheckBlocks(const FileBlock& left, const FileBlock& right) {
    ASSERT_EQ(left.offset, right.offset);
    ASSERT_EQ(left.metadata_length, right.metadata_length);
    ASSERT_EQ(left.body_length, right.body_length);
  }

 private:
  std::shared_ptr<Schema> example_schema_;
};

TEST_F(TestFileFooter, Basics) {
  auto f0 = std::make_shared<Field>("f0", std::make_shared<Int8Type>());
  auto f1 = std::make_shared<Field>("f1", std::make_shared<Int16Type>());
  Schema schema({f0, f1});

  std::vector<FileBlock> dictionaries;
  dictionaries.emplace_back(8, 92, 900);
  dictionaries.emplace_back(1000, 100, 1900);
  dictionaries.emplace_back(3000, 100, 2900);

  std::vector<FileBlock> record_batches;
  record_batches.emplace_back(6000, 100, 900);
  record_batches.emplace_back(7000, 100, 1900);
  record_batches.emplace_back(9000, 100, 2900);
  record_batches.emplace_back(12000, 100, 3900);

  CheckRoundtrip(&schema, dictionaries, record_batches);
}

}  // namespace ipc
}  // namespace arrow
