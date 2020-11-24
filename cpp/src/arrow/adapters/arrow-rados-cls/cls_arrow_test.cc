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

#include <rados/librados.hpp>

#include "arrow/api.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/dataset_rados.h"
#include "arrow/dataset/rados_utils.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/util/iterator.h"

#include "arrow/adapters/arrow-rados-cls/cls_arrow_test_utils.h"
#include "gtest/gtest.h"

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

arrow::dataset::RadosObjectVector CreateTestObjectVector(std::string id) {
  auto object = std::make_shared<arrow::dataset::RadosObject>(id);
  arrow::dataset::RadosObjectVector vector;
  vector.push_back(object);
  return vector;
}

std::shared_ptr<arrow::dataset::RadosCluster> CreateTestClusterHandle() {
  librados::Rados cluster_;
  librados::create_one_pool_pp("test-pool", cluster_);
  auto cluster =
      std::make_shared<arrow::dataset::RadosCluster>("test-pool", "/etc/ceph/ceph.conf");
  cluster->Connect();
  return cluster;
}

TEST(TestClsSDK, WriteAndScanTable) {
  auto cluster = CreateTestClusterHandle();
  auto batches = CreateTestRecordBatches();
  auto table = CreateTestTable();
  auto objects = CreateTestObjectVector("test.obj.1");

  /// Write the Fragment.
  arrow::dataset::RadosFragment::WriteFragment(batches, cluster, objects[0]);

  /// Build the Dataset.
  arrow::dataset::FinishOptions finish_options;
  auto factory = arrow::dataset::RadosDatasetFactory::Make(objects, cluster).ValueOrDie();
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

TEST(TestClsSDK, Projection) {
  auto cluster = CreateTestClusterHandle();
  auto batches = CreateTestRecordBatches();
  auto table = CreateTestTable();
  auto objects = CreateTestObjectVector("test.obj.2");

  /// Write the Fragment.
  arrow::dataset::RadosFragment::WriteFragment(batches, cluster, objects[0]);

  /// Build the Dataset.
  arrow::dataset::FinishOptions finish_options;
  auto factory = arrow::dataset::RadosDatasetFactory::Make(objects, cluster).ValueOrDie();
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

TEST(TestClsSDK, Selection) {
  auto cluster = CreateTestClusterHandle();
  auto batches = CreateTestRecordBatches();
  auto table = CreateTestTable();
  auto objects = CreateTestObjectVector("test.obj.3");

  /// Write the Fragment.
  arrow::dataset::RadosFragment::WriteFragment(batches, cluster, objects[0]);

  /// Build the Dataset.
  arrow::dataset::FinishOptions finish_options;
  auto factory = arrow::dataset::RadosDatasetFactory::Make(objects, cluster).ValueOrDie();
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

TEST(TestClsSDK, EndToEnd) {
  auto cluster = CreateTestClusterHandle();
  auto batches = CreateTestRecordBatches();

  /// Prepare RecordBatches and Write the fragments.
  arrow::dataset::RadosObjectVector objects;
  for (int i = 0; i < 4; i++) {
    std::string id = "test.obj." + std::to_string(i);
    auto object = CreateTestObjectVector(id)[0];
    objects.push_back(object);
    arrow::dataset::RadosFragment::WriteFragment(batches, cluster, object);
  }

  /// Create a RadosDataset and apply Scan operations.
  arrow::dataset::FinishOptions finish_options;
  auto rados_ds_factory =
      arrow::dataset::RadosDatasetFactory::Make(objects, cluster).ValueOrDie();
  auto rados_ds = rados_ds_factory->Finish(finish_options).ValueOrDie();
  auto rados_scanner_builder = rados_ds->NewScan().ValueOrDie();
  rados_scanner_builder->Filter(("id"_ > int32_t(7)).Copy());
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
  inmemory_scanner_builder->Filter(("id"_ > int32_t(7)).Copy());
  inmemory_scanner_builder->Project(std::vector<std::string>{"cost", "id"});
  auto inmemory_scanner = inmemory_scanner_builder->Finish().ValueOrDie();
  auto expected_table = inmemory_scanner->ToTable().ValueOrDie();

  /// Check if both the Tables are same or not.
  ASSERT_EQ(result_table->Equals(*expected_table), 1);
}
