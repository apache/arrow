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

#include "arrow/dataset/dataset_rados.h"
#include "arrow/api.h"
#include "arrow/dataset/api.h"
#include "arrow/dataset/rados.h"
#include "arrow/dataset/rados_utils.h"
#include "arrow/dataset/test_util.h"
#include "arrow/dataset/visibility.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/api.h"
#include "arrow/testing/generator.h"
#include "arrow/util/macros.h"
#include "arrow/util/optional.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace arrow {
namespace dataset {

std::shared_ptr<RecordBatch> generate_test_record_batch() {
  // Initialize random seed
  srand(time(NULL));

  // The number of rows that the Record Batch will contain
  int64_t row_count = 100;

  // Define a schema
  auto schema_ = schema({field("f1", int64()), field("f2", int64())});
  unsigned int seed = 1u;

  // Build the `f1` column
  auto f1_builder = std::make_shared<Int64Builder>();
  f1_builder->Reset();
  for (auto i = 0; i < row_count; i++) {
    f1_builder->Append(rand_r(&seed));
  }
  std::shared_ptr<Array> batch_size_array;
  f1_builder->Finish(&batch_size_array);

  // Build the `f2` column
  auto f2_builder = std::make_shared<Int64Builder>();
  f2_builder->Reset();
  for (auto i = 0; i < row_count; i++) {
    f2_builder->Append(rand_r(&seed));
  }
  std::shared_ptr<Array> seq_num_array;
  f2_builder->Finish(&seq_num_array);

  // Build the Record Batch
  std::vector<std::shared_ptr<Array>> columns = {batch_size_array, seq_num_array};
  return RecordBatch::Make(schema_, row_count, columns);
}

std::shared_ptr<Table> generate_test_table() {
  RecordBatchVector batches;
  for (int i = 0; i < 8; i++) {
    batches.push_back(generate_test_record_batch());
  }
  // Build a Table having 8 Record Batches
  auto table = Table::FromRecordBatches(batches).ValueOrDie();
  return table;
}

/// \brief A Mock for the IoCtxInterface
class ARROW_DS_EXPORT MockIoCtx : public IoCtxInterface {
 public:
  MockIoCtx() {
    this->setup();
    testing::Mock::AllowLeak(this);
  }
  ~MockIoCtx() {}
  MOCK_METHOD2(write_full, int(const std::string& oid, librados::bufferlist& bl));
  MOCK_METHOD4(read, int(const std::string& oid, librados::bufferlist& bl, size_t len,
                         uint64_t offset));
  MOCK_METHOD5(exec, int(const std::string& oid, const char* cls, const char* method,
                         librados::bufferlist& in, librados::bufferlist& out));
  MOCK_METHOD1(setIoCtx, void(librados::IoCtx* ioCtx_));

 private:
  Status setup() {
    EXPECT_CALL(*this, write_full(testing::_, testing::_)).WillOnce(testing::Return(0));

    // Generate a random table and write it to a bufferlist
    librados::bufferlist result;
    auto table = generate_test_table();
    serialize_table_to_bufferlist(table, result);

    EXPECT_CALL(*this, read(testing::_, testing::_, testing::_, testing::_))
        .WillOnce(DoAll(testing::SetArgReferee<1>(result), testing::Return(0)));

    EXPECT_CALL(*this, exec(testing::_, testing::_, testing::_, testing::_, testing::_))
        .WillRepeatedly(DoAll(testing::SetArgReferee<4>(result), testing::Return(0)));
    return Status::OK();
  }
};

/// \brief A Mock for the RadosInterface
class ARROW_DS_EXPORT MockRados : public RadosInterface {
 public:
  MockRados() {
    this->setup();
    testing::Mock::AllowLeak(this);
  }
  ~MockRados() {}
  MOCK_METHOD3(init2, int(const char* const name, const char* const clustername,
                          uint64_t flags));
  MOCK_METHOD2(ioctx_create, int(const char* name, IoCtxInterface* pioctx));
  MOCK_METHOD1(conf_read_file, int(const char* const path));
  MOCK_METHOD0(connect, int());
  MOCK_METHOD0(shutdown, void());

 private:
  Status setup() {
    EXPECT_CALL(*this, init2(testing::_, testing::_, testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(*this, ioctx_create(testing::_, testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(*this, conf_read_file(testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(*this, connect()).WillOnce(testing::Return(0));
    EXPECT_CALL(*this, shutdown()).WillRepeatedly(testing::Return());
    return Status::OK();
  }
};

class TestRadosScanTask : public DatasetFixtureMixin {};

TEST_F(TestRadosScanTask, Execute) {
  constexpr int64_t kNumberBatches = 8;

  SetSchema({field("f1", int64()), field("f2", int64())});
  auto batch = generate_test_record_batch();
  auto reader = ConstantArrayGenerator::Repeat(kNumberBatches, batch);

  auto object = std::make_shared<RadosObject>("object.1");
  auto rados_options = RadosOptions::FromPoolName("test_pool");

  auto mock_rados_interface = new MockRados();
  auto mock_ioctx_interface = new MockIoCtx();

  rados_options->rados_interface_ = mock_rados_interface;
  rados_options->io_ctx_interface_ = mock_ioctx_interface;

  std::shared_ptr<RadosScanTask> task = std::make_shared<RadosScanTask>(
      options_, ctx_, std::move(object), std::move(rados_options));

  AssertScanTaskEquals(reader.get(), task.get(), false);
}

class TestRadosFragment : public DatasetFixtureMixin {};

TEST_F(TestRadosFragment, Scan) {
  constexpr int64_t kNumberBatches = 8;

  SetSchema({field("f1", int64()), field("f2", int64())});
  auto batch = generate_test_record_batch();
  auto reader = ConstantArrayGenerator::Repeat(kNumberBatches, batch);

  auto object = std::make_shared<RadosObject>("object.1");
  auto rados_options = RadosOptions::FromPoolName("test_pool");

  auto mock_rados_interface = new MockRados();
  auto mock_ioctx_interface = new MockIoCtx();

  rados_options->rados_interface_ = mock_rados_interface;
  rados_options->io_ctx_interface_ = mock_ioctx_interface;

  RadosFragment fragment(schema_, object, rados_options);

  AssertFragmentEquals(reader.get(), &fragment, 4);
}

class TestRadosDataset : public DatasetFixtureMixin {};

TEST_F(TestRadosDataset, GetFragments) {
  constexpr int64_t kNumberBatches = 24;

  SetSchema({field("f1", int64()), field("f2", int64())});

  RadosObjectVector object_vector{std::make_shared<RadosObject>("object.1"),
                                  std::make_shared<RadosObject>("object.2"),
                                  std::make_shared<RadosObject>("object.3")};

  auto batch = generate_test_record_batch();
  auto reader = ConstantArrayGenerator::Repeat(kNumberBatches, batch);

  auto rados_options = RadosOptions::FromPoolName("test_pool");

  auto mock_rados_interface = new MockRados();
  auto mock_ioctx_interface = new MockIoCtx();

  rados_options->rados_interface_ = mock_rados_interface;
  rados_options->io_ctx_interface_ = mock_ioctx_interface;

  auto dataset = std::make_shared<RadosDataset>(schema_, object_vector, rados_options);

  AssertDatasetEquals(reader.get(), dataset.get());
}

TEST_F(TestRadosDataset, ReplaceSchema) {
  SetSchema({field("i32", int32()), field("f64", float64())});

  RadosObjectVector object_vector{std::make_shared<RadosObject>("object.1"),
                                  std::make_shared<RadosObject>("object.2")};

  auto rados_options = RadosOptions::FromPoolName("test_pool");

  auto mock_rados_interface = new MockRados();
  auto mock_ioctx_interface = new MockIoCtx();

  rados_options->rados_interface_ = mock_rados_interface;
  rados_options->io_ctx_interface_ = mock_ioctx_interface;

  auto dataset = std::make_shared<RadosDataset>(schema_, object_vector, rados_options);

  // drop field
  ASSERT_OK(dataset->ReplaceSchema(schema({field("i32", int32())})).status());
  // add field (will be materialized as null during projection)
  ASSERT_OK(dataset->ReplaceSchema(schema({field("str", utf8())})).status());
  // incompatible type
  ASSERT_RAISES(TypeError,
                dataset->ReplaceSchema(schema({field("i32", utf8())})).status());
  // incompatible nullability
  ASSERT_RAISES(
      TypeError,
      dataset->ReplaceSchema(schema({field("f64", float64(), /*nullable=*/false)}))
          .status());
  // add non-nullable field
  ASSERT_RAISES(TypeError,
                dataset->ReplaceSchema(schema({field("str", utf8(), /*nullable=*/false)}))
                    .status());
}

TEST_F(TestRadosDataset, IntToCharAndCharToInt) {
  int64_t value = 12345678;
  char* result = new char[8];
  int64_to_char(result, value);

  char* result_ = result;
  int64_t value_ = 0;
  char_to_int64(1, result_, value_);

  ASSERT_EQ(value, value_);
}

TEST_F(TestRadosDataset, SerializeDeserializeScanRequest) {
  auto filter = std::make_shared<OrExpression>("b"_ == 3 or "b"_ == 4);
  auto schema = arrow::schema({field("i32", int32()), field("f64", float64())});
  librados::bufferlist bl;
  serialize_scan_request_to_bufferlist(filter, schema, bl);

  librados::bufferlist bl__ = std::move(bl);
  std::shared_ptr<Expression> filter__;
  std::shared_ptr<Schema> schema__;
  deserialize_scan_request_from_bufferlist(&filter__, &schema__, bl__);

  ASSERT_TRUE(filter__->Equals(*filter));
  ASSERT_TRUE(schema__->Equals(schema));
}

TEST_F(TestRadosDataset, SerializeDeserializeTable) {
  auto table = generate_test_table();
  librados::bufferlist bl;
  serialize_table_to_bufferlist(table, bl);

  librados::bufferlist bl__(bl);
  std::shared_ptr<Table> table__;
  deserialize_table_from_bufferlist(&table__, bl__);

  ASSERT_TRUE(table__->Equals(*table));
}

TEST_F(TestRadosDataset, EndToEnd) {
  constexpr int64_t kNumberBatches = 24;

  SetSchema({field("f1", int64()), field("f2", int64())});

  RadosObjectVector object_vector{std::make_shared<RadosObject>("object.1"),
                                  std::make_shared<RadosObject>("object.2"),
                                  std::make_shared<RadosObject>("object.3")};

  auto batch = generate_test_record_batch();
  auto reader = ConstantArrayGenerator::Repeat(kNumberBatches, batch);

  auto rados_options = RadosOptions::FromPoolName("test_pool");

  auto mock_rados_interface = new MockRados();
  auto mock_ioctx_interface = new MockIoCtx();

  rados_options->rados_interface_ = mock_rados_interface;
  rados_options->io_ctx_interface_ = mock_ioctx_interface;

  auto dataset = std::make_shared<RadosDataset>(schema_, object_vector, rados_options);
  auto context = std::make_shared<ScanContext>();
  auto builder = std::make_shared<ScannerBuilder>(dataset, context);
  auto scanner = builder->Finish().ValueOrDie();

  std::shared_ptr<Table> table;
  reader->ReadAll(&table);

  auto table_ = scanner->ToTable().ValueOrDie();
  ASSERT_TRUE(table->Equals(*table_));
}

}  // namespace dataset
}  // namespace arrow
