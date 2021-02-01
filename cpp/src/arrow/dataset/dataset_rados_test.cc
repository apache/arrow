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
#include "arrow/adapters/arrow-rados-cls/cls_arrow_test_utils.h"
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

#include <rados/objclass.h>
#include <rados/librados.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

// namespace arrow {
// namespace dataset {

// std::shared_ptr<RecordBatch> generate_test_record_batch() {
//   // Initialize random seed
//   srand(time(NULL));

//   // The number of rows that the Record Batch will contain
//   int64_t row_count = 100;

//   // Define a schema
//   auto schema_ = schema({field("f1", int64()), field("f2", int64())});
//   unsigned int seed = 1u;

//   // Build the `f1` column
//   auto f1_builder = std::make_shared<Int64Builder>();
//   f1_builder->Reset();
//   for (auto i = 0; i < row_count; i++) {
//     f1_builder->Append(rand_r(&seed));
//   }
//   std::shared_ptr<Array> batch_size_array;
//   f1_builder->Finish(&batch_size_array);

//   // Build the `f2` column
//   auto f2_builder = std::make_shared<Int64Builder>();
//   f2_builder->Reset();
//   for (auto i = 0; i < row_count; i++) {
//     f2_builder->Append(rand_r(&seed));
//   }
//   std::shared_ptr<Array> seq_num_array;
//   f2_builder->Finish(&seq_num_array);

//   // Build the Record Batch
//   std::vector<std::shared_ptr<Array>> columns = {batch_size_array, seq_num_array};
//   return RecordBatch::Make(schema_, row_count, columns);
// }

// std::shared_ptr<Table> generate_test_table() {
//   RecordBatchVector batches;
//   for (int i = 0; i < 8; i++) {
//     batches.push_back(generate_test_record_batch());
//   }
//   // Build a Table having 8 Record Batches
//   auto table = Table::FromRecordBatches(batches).ValueOrDie();
//   return table;
// }

// /// \brief A Mock for the IoCtxInterface
// class ARROW_DS_EXPORT MockIoCtx : public IoCtxInterface {
//  public:
//   MockIoCtx() {
//     this->setup();
//     testing::Mock::AllowLeak(this);
//   }
//   ~MockIoCtx() {}
//   MOCK_METHOD2(write_full, int(const std::string& oid, librados::bufferlist& bl));
//   MOCK_METHOD4(read, int(const std::string& oid, librados::bufferlist& bl, size_t len,
//                          uint64_t offset));
//   MOCK_METHOD5(exec, int(const std::string& oid, const char* cls, const char* method,
//                          librados::bufferlist& in, librados::bufferlist& out));
//   MOCK_METHOD1(setIoCtx, void(librados::IoCtx* ioCtx_));
//   MOCK_METHOD0(list, std::vector<std::string>());

//  private:
//   Status setup() {
//     EXPECT_CALL(*this, write_full(testing::_,
//     testing::_)).WillOnce(testing::Return(0));

//     // Generate a random table and write it to a bufferlist
//     librados::bufferlist result;
//     auto table = generate_test_table();
//     SerializeTableToIPCStream(table, result);

//     EXPECT_CALL(*this, read(testing::_, testing::_, testing::_, testing::_))
//         .WillOnce(DoAll(testing::SetArgReferee<1>(result), testing::Return(0)));

//     EXPECT_CALL(*this, exec(testing::_, testing::_, testing::_, testing::_,
//     testing::_))
//         .WillRepeatedly(DoAll(testing::SetArgReferee<4>(result), testing::Return(0)));
//     EXPECT_CALL(*this, list())
//         .WillOnce(testing::Return(std::vector<std::string>{"sample"}));
//     return Status::OK();
//   }
// };

// /// \brief A Mock for the RadosInterface
// class ARROW_DS_EXPORT MockRados : public RadosInterface {
//  public:
//   MockRados() {
//     this->setup();
//     testing::Mock::AllowLeak(this);
//   }
//   ~MockRados() {}
//   MOCK_METHOD3(init2, int(const char* const name, const char* const clustername,
//                           uint64_t flags));
//   MOCK_METHOD2(ioctx_create, int(const char* name, IoCtxInterface* pioctx));
//   MOCK_METHOD1(conf_read_file, int(const char* const path));
//   MOCK_METHOD0(connect, int());
//   MOCK_METHOD0(shutdown, void());

//  private:
//   Status setup() {
//     EXPECT_CALL(*this, init2(testing::_, testing::_, testing::_))
//         .WillOnce(testing::Return(0));
//     EXPECT_CALL(*this, ioctx_create(testing::_,
//     testing::_)).WillOnce(testing::Return(0)); EXPECT_CALL(*this,
//     conf_read_file(testing::_)).WillOnce(testing::Return(0)); EXPECT_CALL(*this,
//     connect()).WillOnce(testing::Return(0)); EXPECT_CALL(*this,
//     shutdown()).WillRepeatedly(testing::Return()); return Status::OK();
//   }
// };

// std::shared_ptr<arrow::Table> CreateTestTable() {
//   // Create a memory pool
//   arrow::MemoryPool* pool = arrow::default_memory_pool();

//   // An Arrow array builder for each table column
//   arrow::Int32Builder id_builder(pool);
//   arrow::DoubleBuilder cost_builder(pool);
//   arrow::ListBuilder components_builder(pool,
//                                         std::make_shared<arrow::DoubleBuilder>(pool));

//   // The following builder is owned by components_builder.
//   arrow::DoubleBuilder& cost_components_builder =
//       *(static_cast<arrow::DoubleBuilder*>(components_builder.value_builder()));

//   // Append some fake data
//   for (int i = 0; i < 10; ++i) {
//     id_builder.Append(i);
//     cost_builder.Append(i + 1.0);
//     // Indicate the start of a new list row. This will memorise the current
//     // offset in the values builder.
//     components_builder.Append();
//     std::vector<double> nums;
//     nums.push_back(i + 1.0);
//     nums.push_back(i + 2.0);
//     nums.push_back(i + 3.0);
//     cost_components_builder.AppendValues(nums.data(), nums.size());
//   }

//   // At the end, we finalise the arrays, declare the (type) schema and combine
//   // them into a single `arrow::Table`:
//   std::shared_ptr<arrow::Int32Array> id_array;
//   id_builder.Finish(&id_array);
//   std::shared_ptr<arrow::DoubleArray> cost_array;
//   cost_builder.Finish(&cost_array);

//   // No need to invoke cost_components_builder.Finish because it is implied by
//   // the parent builder's Finish invocation.
//   std::shared_ptr<arrow::ListArray> cost_components_array;
//   components_builder.Finish(&cost_components_array);

//   // Create table schema and make table from col arrays and schema
//   std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
//       arrow::field("id", arrow::int32()), arrow::field("cost", arrow::float64()),
//       arrow::field("cost_components", arrow::list(arrow::float64()))};
//   auto schema = std::make_shared<arrow::Schema>(schema_vector);
//   return arrow::Table::Make(schema, {id_array, cost_array, cost_components_array});
// }

// arrow::RecordBatchVector CreateTestRecordBatches() {
//   auto table = CreateTestTable();
//   arrow::TableBatchReader table_reader(*table);
//   arrow::RecordBatchVector batches;
//   table_reader.ReadAll(&batches);
//   return batches;
// }

// std::shared_ptr<arrow::dataset::RadosCluster> CreateTestClusterHandle() {
//   librados::Rados cluster_;
//   librados::create_one_pool_pp("test-pool", cluster_);
//   auto cluster =
//       std::make_shared<arrow::dataset::RadosCluster>("test-pool",
//       "/etc/ceph/ceph.conf");
//   cluster->Connect();
//   return cluster;
// }

// arrow::dataset::RadosDatasetFactoryOptions CreateTestRadosFactoryOptions() {
//   auto cluster = CreateTestClusterHandle();
//   arrow::dataset::RadosDatasetFactoryOptions factory_options;
//   factory_options.ceph_config_path = cluster->ceph_config_path;
//   factory_options.cls_name = cluster->cls_name;
//   factory_options.cluster_name = cluster->cluster_name;
//   factory_options.flags = cluster->flags;
//   factory_options.pool_name = cluster->pool_name;
//   factory_options.user_name = cluster->user_name;
//   return factory_options;
// }

// std::shared_ptr<arrow::dataset::RadosFileSystem> CreateTestRadosFileSystem() {
//   auto cluster = CreateTestClusterHandle();
//   auto fs = std::make_shared<arrow::dataset::RadosFileSystem>();
//   fs->Init(cluster);
//   return fs;
// }

// class TestRadosScanTask : public DatasetFixtureMixin {};

// TEST_F(TestRadosScanTask, Execute) {
//   constexpr int64_t kNumberBatches = 8;

//   SetSchema({field("f1", int64()), field("f2", int64())});
//   auto batch = generate_test_record_batch();
//   auto reader = ConstantArrayGenerator::Repeat(kNumberBatches, batch);

//   auto cluster = std::make_shared<RadosCluster>("test-pool", "/etc/ceph/ceph.conf");

//   auto mock_rados_interface = new MockRados();
//   auto mock_ioctx_interface = new MockIoCtx();

//   cluster->rados = mock_rados_interface;
//   cluster->ioCtx = mock_ioctx_interface;

//   auto fs = std::make_shared<RadosFileSystem>();
//   fs->Init(cluster);

//   std::shared_ptr<RadosScanTask> task = std::make_shared<RadosScanTask>(
//       options_, ctx_, "/path/to/file", std::move(fs));

//   AssertScanTaskEquals(reader.get(), task.get(), false);
// }

// // class TestRadosFragment : public DatasetFixtureMixin {};

// // TEST_F(TestRadosFragment, Scan) {
// //   constexpr int64_t kNumberBatches = 8;

// //   SetSchema({field("f1", int64()), field("f2", int64())});
// //   auto batch = generate_test_record_batch();
// //   auto reader = ConstantArrayGenerator::Repeat(kNumberBatches, batch);

// //   auto object = std::make_shared<RadosObject>("object.1");
// //   auto cluster = std::make_shared<RadosCluster>("test-pool", "/etc/ceph/ceph.conf");

// //   auto mock_rados_interface = new MockRados();
// //   auto mock_ioctx_interface = new MockIoCtx();

// //   cluster->rados = mock_rados_interface;
// //   cluster->ioCtx = mock_ioctx_interface;

// //   RadosFragment fragment(schema_, object, cluster, 1);

// //   AssertFragmentEquals(reader.get(), &fragment, 4);
// // }

// // class TestRadosDataset : public DatasetFixtureMixin {};

// // TEST_F(TestRadosDataset, GetFragments) {
// //   constexpr int64_t kNumberBatches = 24;

// //   SetSchema({field("f1", int64()), field("f2", int64())});

// //   auto cluster = std::make_shared<RadosCluster>("test-pool", "/etc/ceph/ceph.conf");
// //   auto mock_rados_interface = new MockRados();
// //   auto mock_ioctx_interface = new MockIoCtx();
// //   cluster->rados = mock_rados_interface;
// //   cluster->ioCtx = mock_ioctx_interface;

// //   RadosObjectVector object_vector{std::make_shared<RadosObject>("object.1"),
// //                                   std::make_shared<RadosObject>("object.2"),
// //                                   std::make_shared<RadosObject>("object.3")};

// //   RadosFragmentVector fragments;
// //   for (int i = 0; i < 3; i++) {
// //     fragments.push_back(
// //         std::make_shared<RadosFragment>(schema_, object_vector[i], cluster, 1));
// //   }

// //   auto batch = generate_test_record_batch();
// //   auto reader = ConstantArrayGenerator::Repeat(kNumberBatches, batch);
// //   auto dataset = RadosDataset::Make(schema_, fragments, cluster).ValueOrDie();

// //   AssertDatasetEquals(reader.get(), dataset.get());
// // }

// // TEST_F(TestRadosDataset, ReplaceSchema) {
// //   SetSchema({field("i32", int32()), field("f64", float64())});

// //   auto cluster = std::make_shared<RadosCluster>("test-pool", "/etc/ceph/ceph.conf");
// //   auto mock_rados_interface = new MockRados();
// //   auto mock_ioctx_interface = new MockIoCtx();
// //   cluster->rados = mock_rados_interface;
// //   cluster->ioCtx = mock_ioctx_interface;

// //   RadosObjectVector object_vector{std::make_shared<RadosObject>("object.1"),
// //                                   std::make_shared<RadosObject>("object.2")};

// //   RadosFragmentVector fragments;
// //   for (int i = 0; i < 2; i++) {
// //     fragments.push_back(
// //         std::make_shared<RadosFragment>(schema_, object_vector[i], cluster, 1));
// //   }

// //   auto dataset = RadosDataset::Make(schema_, fragments, cluster).ValueOrDie();

// //   // drop field
// //   ASSERT_OK(dataset->ReplaceSchema(schema({field("i32", int32())})).status());
// //   // add field (will be materialized as null during projection)
// //   ASSERT_OK(dataset->ReplaceSchema(schema({field("str", utf8())})).status());
// //   // incompatible type
// //   ASSERT_RAISES(TypeError,
// //                 dataset->ReplaceSchema(schema({field("i32", utf8())})).status());
// //   // incompatible nullability
// //   ASSERT_RAISES(
// //       TypeError,
// //       dataset->ReplaceSchema(schema({field("f64", float64(), /*nullable=*/false)}))
// //           .status());
// //   // add non-nullable field
// //   ASSERT_RAISES(TypeError,
// //                 dataset->ReplaceSchema(schema({field("str", utf8(),
// /*nullable=*/false)}))
// //                     .status());
// // }

// // TEST_F(TestRadosDataset, IntToCharAndCharToInt) {
// //   int64_t value = 12345678;
// //   char* result = new char[8];
// //   int64_to_char(result, value);

// //   char* result_ = result;
// //   int64_t value_ = 0;
// //   char_to_int64(result_, value_);

// //   ASSERT_EQ(value, value_);
// // }

// // TEST_F(TestRadosDataset, SerializeDeserializeScanRequest) {
// //   auto filter = std::make_shared<OrExpression>("b"_ == 3 or "b"_ == 4);
// //   auto partition_expr = std::make_shared<OrExpression>("c"_ == 10 or "c"_ == 12);
// //   auto schema = arrow::schema({field("i32", int32()), field("f64", float64())});
// //   librados::bufferlist bl;
// //   SerializeScanRequestToBufferlist(filter, partition_expr, schema, 2, bl);

// //   librados::bufferlist bl__ = std::move(bl);
// //   std::shared_ptr<Expression> filter__;
// //   std::shared_ptr<Expression> partition_expr__;
// //   std::shared_ptr<Schema> schema__;
// //   int64_t format__;
// //   DeserializeScanRequestFromBufferlist(&filter__, &partition_expr__, &schema__,
// &format__,
// //                                        bl__);

// //   ASSERT_TRUE(filter__->Equals(*filter));
// //   ASSERT_TRUE(partition_expr__->Equals(*partition_expr));
// //   ASSERT_TRUE(schema__->Equals(schema));
// // }

// // TEST_F(TestRadosDataset, SerializeDeserializeTable) {
// //   auto table = generate_test_table();
// //   librados::bufferlist bl;
// //   SerializeTableToIPCStream(table, bl);

// //   librados::bufferlist bl__(bl);
// //   std::shared_ptr<Table> table__;
// //   DeserializeTableFromBufferlist(&table__, bl__);

// //   ASSERT_TRUE(table__->Equals(*table));
// // }

// // TEST_F(TestRadosDataset, EndToEnd) {
// //   constexpr int64_t kNumberBatches = 24;

// //   SetSchema({field("f1", int64()), field("f2", int64())});

// //   auto cluster = std::make_shared<RadosCluster>("test-pool", "/etc/ceph/ceph.conf");
// //   auto mock_rados_interface = new MockRados();
// //   auto mock_ioctx_interface = new MockIoCtx();
// //   cluster->rados = mock_rados_interface;
// //   cluster->ioCtx = mock_ioctx_interface;

// //   RadosObjectVector object_vector{std::make_shared<RadosObject>("object.1"),
// //                                   std::make_shared<RadosObject>("object.2"),
// //                                   std::make_shared<RadosObject>("object.3")};
// //   RadosFragmentVector fragments;
// //   for (int i = 0; i < 3; i++) {
// //     fragments.push_back(
// //         std::make_shared<RadosFragment>(schema_, object_vector[i], cluster, 1));
// //   }

// //   auto batch = generate_test_record_batch();
// //   auto reader = ConstantArrayGenerator::Repeat(kNumberBatches, batch);

// //   auto dataset = RadosDataset::Make(schema_, fragments, cluster).ValueOrDie();
// //   auto context = std::make_shared<ScanContext>();
// //   auto builder = std::make_shared<ScannerBuilder>(dataset, context);
// //   auto scanner = builder->Finish().ValueOrDie();

// //   std::shared_ptr<Table> table;
// //   reader->ReadAll(&table);

// //   auto table_ = scanner->ToTable().ValueOrDie();

// //   ASSERT_TRUE(table->Equals(*table_));
// // }

// }  // namespace dataset
// }  // namespace arrow
