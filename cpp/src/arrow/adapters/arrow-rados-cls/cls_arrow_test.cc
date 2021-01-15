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

#include <errno.h>
#include <iostream>

#include <rados/objclass.h>
#include <rados/librados.hpp>

#include "arrow/adapters/arrow-rados-cls/cls_arrow_test_utils.h"
#include "arrow/api.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/dataset_rados.h"
#include "arrow/dataset/rados_utils.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/util/iterator.h"
#include "gtest/gtest.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

using arrow::dataset::string_literals::operator"" _;

std::shared_ptr<arrow::Table> CreateTestTable() {
  // Create a memory pool
  arrow::MemoryPool* pool = arrow::default_memory_pool();

  // An Arrow array builder for each table column
  arrow::Int32Builder id_builder(pool);
  arrow::DoubleBuilder cost_builder(pool);
  arrow::ListBuilder components_builder(pool,
                                        std::make_shared<arrow::DoubleBuilder>(pool));

  // The following builder is owned by components_builder.
  arrow::DoubleBuilder& cost_components_builder =
      *(static_cast<arrow::DoubleBuilder*>(components_builder.value_builder()));

  // Append some fake data
  for (int i = 0; i < 10; ++i) {
    id_builder.Append(i);
    cost_builder.Append(i + 1.0);
    // Indicate the start of a new list row. This will memorise the current
    // offset in the values builder.
    components_builder.Append();
    std::vector<double> nums;
    nums.push_back(i + 1.0);
    nums.push_back(i + 2.0);
    nums.push_back(i + 3.0);
    cost_components_builder.AppendValues(nums.data(), nums.size());
  }

  // At the end, we finalise the arrays, declare the (type) schema and combine
  // them into a single `arrow::Table`:
  std::shared_ptr<arrow::Int32Array> id_array;
  id_builder.Finish(&id_array);
  std::shared_ptr<arrow::DoubleArray> cost_array;
  cost_builder.Finish(&cost_array);

  // No need to invoke cost_components_builder.Finish because it is implied by
  // the parent builder's Finish invocation.
  std::shared_ptr<arrow::ListArray> cost_components_array;
  components_builder.Finish(&cost_components_array);

  // Create table schema and make table from col arrays and schema
  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("id", arrow::int32()), arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))};
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  return arrow::Table::Make(schema, {id_array, cost_array, cost_components_array});
}

arrow::RecordBatchVector CreateTestRecordBatches() {
  auto table = CreateTestTable();
  arrow::TableBatchReader table_reader(*table);
  arrow::RecordBatchVector batches;
  table_reader.ReadAll(&batches);
  return batches;
}

std::shared_ptr<arrow::dataset::RadosObject> CreateTestObject(std::string id) {
  auto object = std::make_shared<arrow::dataset::RadosObject>(id);
  return object;
}

std::shared_ptr<arrow::dataset::RadosCluster> CreateTestClusterHandle() {
  librados::Rados cluster_;
  librados::create_one_pool_pp("test-pool", cluster_);
  auto cluster =
      std::make_shared<arrow::dataset::RadosCluster>("test-pool", "/etc/ceph/ceph.conf");
  cluster->Connect();
  return cluster;
}

arrow::dataset::RadosDatasetFactoryOptions CreateTestRadosFactoryOptions() {
  auto cluster = CreateTestClusterHandle();
  arrow::dataset::RadosDatasetFactoryOptions factory_options;
  factory_options.ceph_config_path = cluster->ceph_config_path_;
  factory_options.cls_name = cluster->cls_name_;
  factory_options.cluster_name = cluster->cluster_name_;
  factory_options.flags = cluster->flags_;
  factory_options.pool_name = cluster->pool_name_;
  factory_options.user_name = cluster->user_name_;
  return factory_options;
}

TEST(TestClsSDK, WriteAndScanTableIPC) {
  auto batches = CreateTestRecordBatches();
  auto table = CreateTestTable();
  auto object = CreateTestObject("test.obj.1");
  auto factory_options = CreateTestRadosFactoryOptions();
  factory_options.format = 1;

  /// Write the Fragment.
  arrow::dataset::RadosDataset::Write(batches, factory_options, object->id());

  /// Build the Dataset.
  arrow::dataset::FinishOptions finish_options;
  auto factory = arrow::dataset::RadosDatasetFactory::Make(
                     factory_options,
                     std::vector<std::shared_ptr<arrow::dataset::RadosObject>>{object})
                     .ValueOrDie();
  auto dataset = factory->Finish(finish_options).ValueOrDie();

  /// Build the Scanner.
  auto scanner_builder = dataset->NewScan().ValueOrDie();
  scanner_builder->Filter(arrow::dataset::scalar(true));
  scanner_builder->Project(std::vector<std::string>{"id", "cost", "cost_components"});
  auto scanner = scanner_builder->Finish().ValueOrDie();

  /// Execute Scan and Validate.
  auto result_table = scanner->ToTable().ValueOrDie();
  ASSERT_EQ(table->Equals(*result_table), 1);
}

TEST(TestClsSDK, WriteAndScanTableParquet) {
  auto batches = CreateTestRecordBatches();
  auto table = CreateTestTable();
  auto object = CreateTestObject("test.obj.1");
  auto factory_options = CreateTestRadosFactoryOptions();
  factory_options.format = 2;

  /// Write the Fragment.
  arrow::dataset::RadosDataset::Write(batches, factory_options, object->id());

  /// Build the Dataset.
  arrow::dataset::FinishOptions finish_options;
  auto factory = arrow::dataset::RadosDatasetFactory::Make(
                     factory_options,
                     std::vector<std::shared_ptr<arrow::dataset::RadosObject>>{object})
                     .ValueOrDie();
  auto dataset = factory->Finish(finish_options).ValueOrDie();

  /// Build the Scanner.
  auto scanner_builder = dataset->NewScan().ValueOrDie();
  scanner_builder->Filter(arrow::dataset::scalar(true));
  scanner_builder->Project(std::vector<std::string>{"id", "cost", "cost_components"});
  auto scanner = scanner_builder->Finish().ValueOrDie();

  /// Execute Scan and Validate.
  auto result_table = scanner->ToTable().ValueOrDie();
  ASSERT_EQ(table->Equals(*result_table), 1);
}

TEST(TestClsSDK, ProjectionIPC) {
  auto batches = CreateTestRecordBatches();
  auto table = CreateTestTable();
  auto object = CreateTestObject("test.obj.2");
  auto factory_options = CreateTestRadosFactoryOptions();
  factory_options.format = 1;

  /// Write the Fragment.
  arrow::dataset::RadosDataset::Write(batches, factory_options, object->id());

  /// Build the Dataset.
  arrow::dataset::FinishOptions finish_options;
  auto factory = arrow::dataset::RadosDatasetFactory::Make(
                     factory_options,
                     std::vector<std::shared_ptr<arrow::dataset::RadosObject>>{object})
                     .ValueOrDie();
  auto dataset = factory->Finish(finish_options).ValueOrDie();

  /// Build the Scanner.
  auto scanner_builder = dataset->NewScan().ValueOrDie();
  scanner_builder->Filter(arrow::dataset::scalar(true));
  scanner_builder->Project(std::vector<std::string>{"id", "cost_components"});
  auto scanner = scanner_builder->Finish().ValueOrDie();

  /// Execute Scan and Validate.
  auto result_table = scanner->ToTable().ValueOrDie();
  auto table_projected = table->RemoveColumn(1).ValueOrDie();
  ASSERT_EQ(table->Equals(*result_table), 0);
  ASSERT_EQ(table_projected->Equals(*result_table), 1);
  ASSERT_EQ(result_table->num_columns(), 2);
}

TEST(TestClsSDK, ProjectionParquet) {
  auto batches = CreateTestRecordBatches();
  auto table = CreateTestTable();
  auto object = CreateTestObject("test.obj.2");
  auto factory_options = CreateTestRadosFactoryOptions();
  factory_options.format = 2;

  /// Write the Fragment.
  arrow::dataset::RadosDataset::Write(batches, factory_options, object->id());

  /// Build the Dataset.
  arrow::dataset::FinishOptions finish_options;
  auto factory = arrow::dataset::RadosDatasetFactory::Make(
                     factory_options,
                     std::vector<std::shared_ptr<arrow::dataset::RadosObject>>{object})
                     .ValueOrDie();
  auto dataset = factory->Finish(finish_options).ValueOrDie();

  /// Build the Scanner.
  auto scanner_builder = dataset->NewScan().ValueOrDie();
  scanner_builder->Filter(arrow::dataset::scalar(true));
  scanner_builder->Project(std::vector<std::string>{"id", "cost_components"});
  auto scanner = scanner_builder->Finish().ValueOrDie();

  /// Execute Scan and Validate.
  auto result_table = scanner->ToTable().ValueOrDie();
  auto table_projected = table->RemoveColumn(1).ValueOrDie();
  ASSERT_EQ(table->Equals(*result_table), 0);
  ASSERT_EQ(table_projected->Equals(*result_table), 1);
  ASSERT_EQ(result_table->num_columns(), 2);
}

TEST(TestClsSDK, SelectionIPC) {
  auto batches = CreateTestRecordBatches();
  auto table = CreateTestTable();
  auto object = CreateTestObject("test.obj.3");
  auto factory_options = CreateTestRadosFactoryOptions();
  factory_options.format = 1;

  /// Write the Fragment.
  arrow::dataset::RadosDataset::Write(batches, factory_options, object->id());

  /// Build the Dataset.
  arrow::dataset::FinishOptions finish_options;
  auto factory = arrow::dataset::RadosDatasetFactory::Make(
                     factory_options,
                     std::vector<std::shared_ptr<arrow::dataset::RadosObject>>{object})
                     .ValueOrDie();
  auto dataset = factory->Finish(finish_options).ValueOrDie();

  /// Build the Scanner.
  auto scanner_builder = dataset->NewScan().ValueOrDie();
  auto filter = ("id"_ == int32_t(8) || "id"_ == int32_t(7)).Copy();
  scanner_builder->Filter(filter);
  scanner_builder->Project(std::vector<std::string>{"id", "cost", "cost_components"});
  auto scanner = scanner_builder->Finish().ValueOrDie();

  /// Execute Scan and Validate.
  auto result_table = scanner->ToTable().ValueOrDie();
  ASSERT_EQ(result_table->num_rows(), 2);
}

TEST(TestClsSDK, SelectionParquet) {
  auto batches = CreateTestRecordBatches();
  auto table = CreateTestTable();
  auto object = CreateTestObject("test.obj.3");
  auto factory_options = CreateTestRadosFactoryOptions();
  factory_options.format = 2;

  /// Write the Fragment.
  arrow::dataset::RadosDataset::Write(batches, factory_options, object->id());

  /// Build the Dataset.
  arrow::dataset::FinishOptions finish_options;
  auto factory = arrow::dataset::RadosDatasetFactory::Make(
                     factory_options,
                     std::vector<std::shared_ptr<arrow::dataset::RadosObject>>{object})
                     .ValueOrDie();
  auto dataset = factory->Finish(finish_options).ValueOrDie();

  /// Build the Scanner.
  auto scanner_builder = dataset->NewScan().ValueOrDie();
  auto filter = ("id"_ == int32_t(8) || "id"_ == int32_t(7)).Copy();
  scanner_builder->Filter(filter);
  scanner_builder->Project(std::vector<std::string>{"id", "cost", "cost_components"});
  auto scanner = scanner_builder->Finish().ValueOrDie();

  /// Execute Scan and Validate.
  auto result_table = scanner->ToTable().ValueOrDie();
  ASSERT_EQ(result_table->num_rows(), 2);
}

TEST(TestClsSDK, EndToEndIPC) {
  auto batches = CreateTestRecordBatches();
  auto factory_options = CreateTestRadosFactoryOptions();
  factory_options.format = 1;

  /// Prepare RecordBatches and Write the fragments.
  for (int i = 0; i < 4; i++) {
    std::string object_id = "/ds/test.obj." + std::to_string(i);
    arrow::dataset::RadosDataset::Write(batches, factory_options, object_id);
  }

  /// Create a RadosDataset and apply Scan operations.
  arrow::dataset::FinishOptions finish_options;
  factory_options.partition_base_dir = "/ds";
  auto rados_ds_factory =
      arrow::dataset::RadosDatasetFactory::Make(factory_options).ValueOrDie();
  auto rados_ds = rados_ds_factory->Finish(finish_options).ValueOrDie();
  auto rados_scanner_builder = rados_ds->NewScan().ValueOrDie();
  rados_scanner_builder->Filter(("id"_ > int32_t(5)).Copy());
  rados_scanner_builder->Project(std::vector<std::string>{"cost", "id"});
  auto rados_scanner = rados_scanner_builder->Finish().ValueOrDie();
  auto result_table = rados_scanner->ToTable().ValueOrDie();

  /// Create an InMemoryDataset and apply Scan operations.
  arrow::RecordBatchVector batches_;
  for (auto batch : batches) {
    batches_.push_back(batch);
    batches_.push_back(batch);
    batches_.push_back(batch);
    batches_.push_back(batch);
  }

  auto schema = arrow::schema(
      {arrow::field("id", arrow::int32()), arrow::field("cost", arrow::float64()),
       arrow::field("cost_components", arrow::list(arrow::float64()))});

  auto inmemory_ds = std::make_shared<arrow::dataset::InMemoryDataset>(schema, batches_);
  auto inmemory_scanner_builder = inmemory_ds->NewScan().ValueOrDie();
  inmemory_scanner_builder->Filter(("id"_ > int32_t(5)).Copy());
  inmemory_scanner_builder->Project(std::vector<std::string>{"cost", "id"});
  auto inmemory_scanner = inmemory_scanner_builder->Finish().ValueOrDie();
  auto expected_table = inmemory_scanner->ToTable().ValueOrDie();

  /// Check if both the Tables are same or not.
  ASSERT_EQ(result_table->Equals(*expected_table), 1);
}

TEST(TestClsSDK, EndToEndParquet) {
  auto batches = CreateTestRecordBatches();
  auto factory_options = CreateTestRadosFactoryOptions();
  factory_options.format = 1;

  /// Prepare RecordBatches and Write the fragments.
  for (int i = 0; i < 4; i++) {
    std::string object_id = "/ds/test.obj." + std::to_string(i);
    arrow::dataset::RadosDataset::Write(batches, factory_options, object_id);
  }

  /// Create a RadosDataset and apply Scan operations.
  arrow::dataset::FinishOptions finish_options;
  factory_options.partition_base_dir = "/ds";
  auto rados_ds_factory =
      arrow::dataset::RadosDatasetFactory::Make(factory_options).ValueOrDie();
  auto rados_ds = rados_ds_factory->Finish(finish_options).ValueOrDie();
  auto rados_scanner_builder = rados_ds->NewScan().ValueOrDie();
  rados_scanner_builder->Filter(("id"_ > int32_t(5)).Copy());
  rados_scanner_builder->Project(std::vector<std::string>{"cost", "id"});
  auto rados_scanner = rados_scanner_builder->Finish().ValueOrDie();
  auto result_table = rados_scanner->ToTable().ValueOrDie();

  /// Create an InMemoryDataset and apply Scan operations.
  arrow::RecordBatchVector batches_;
  for (auto batch : batches) {
    batches_.push_back(batch);
    batches_.push_back(batch);
    batches_.push_back(batch);
    batches_.push_back(batch);
  }

  auto schema = arrow::schema(
      {arrow::field("id", arrow::int32()), arrow::field("cost", arrow::float64()),
       arrow::field("cost_components", arrow::list(arrow::float64()))});

  auto inmemory_ds = std::make_shared<arrow::dataset::InMemoryDataset>(schema, batches_);
  auto inmemory_scanner_builder = inmemory_ds->NewScan().ValueOrDie();
  inmemory_scanner_builder->Filter(("id"_ > int32_t(5)).Copy());
  inmemory_scanner_builder->Project(std::vector<std::string>{"cost", "id"});
  auto inmemory_scanner = inmemory_scanner_builder->Finish().ValueOrDie();
  auto expected_table = inmemory_scanner->ToTable().ValueOrDie();

  /// Check if both the Tables are same or not.
  ASSERT_EQ(result_table->Equals(*expected_table), 1);
}

TEST(TestClsSDK, EndToEndWithPartitioning) {
  auto batches = CreateTestRecordBatches();
  auto factory_options = CreateTestRadosFactoryOptions();
  factory_options.format = 2;

  factory_options.partition_base_dir = "nyc/";
  factory_options.partitioning = std::make_shared<arrow::dataset::HivePartitioning>(
      arrow::schema({arrow::field("payment_type", arrow::int64()),
                     arrow::field("VendorID", arrow::int64())}));

  /// Create a RadosDataset and apply Scan operations.
  arrow::dataset::FinishOptions finish_options;
  auto factory = arrow::dataset::RadosDatasetFactory::Make(factory_options).ValueOrDie();
  auto ds = factory->Finish(finish_options).ValueOrDie();

  auto builder = ds->NewScan().ValueOrDie();

  auto projection = std::vector<std::string>{"DOLocationID", "PULocationID",
                                             "passenger_count", "payment_type"};
  auto filter = ("payment_type"_ == int64_t(1) && "passenger_count"_ > int64_t(4)).Copy();

  builder->Project(projection);
  builder->Filter(filter);
  auto scanner = builder->Finish().ValueOrDie();

  auto table = scanner->ToTable().ValueOrDie();
  std::cout << table->ToString() << "\n";

  ASSERT_EQ(table->num_columns(), 4);
  ASSERT_EQ(table->num_rows(), 5651);
}
