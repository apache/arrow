#include <iostream>
#include <errno.h>

#include "cls_arrow_utils.h"
#include "test_utils.h"
#include "gtest/gtest.h"

using arrow::dataset::string_literals::operator"" _;

TEST(ClsSDK, TestWriteAndReadTable) {
  librados::Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  librados::IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  
  // WTITE PATH
  librados::bufferlist in, out;
  std::shared_ptr<arrow::Table> table;
  create_test_arrow_table(&table);
  write_table_to_bufferlist(table, in);
  ASSERT_EQ(0, ioctx.exec("test_object_1", "arrow", "write", in, out));

  // READ PATH
  librados::bufferlist in_, out_;
  auto filter = arrow::dataset::scalar(true);
  auto schema = arrow::schema({
      arrow::field("id", arrow::int32()),
      arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))});
  serialize_scan_request_to_bufferlist(filter, schema, in_);
  ASSERT_EQ(0, ioctx.exec("test_object_1", "arrow", "read", in_, out_));
  std::shared_ptr<arrow::Table> table_;
  read_table_from_bufferlist(&table_, out_);
  ASSERT_EQ(table->Equals(*table_), 1);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsSDK, TestProjection) {
  librados::Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  librados::IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  
  // WTITE PATH
  librados::bufferlist in, out;
  std::shared_ptr<arrow::Table> table;
  create_test_arrow_table(&table);
  write_table_to_bufferlist(table, in);
  ASSERT_EQ(0, ioctx.exec("test_object_2", "arrow", "write", in, out));

  // READ PATH
  librados::bufferlist in_, out_;
  auto filter = arrow::dataset::scalar(true);
  auto schema = arrow::schema({
      arrow::field("id", arrow::int32()),
      arrow::field("cost_components", arrow::list(arrow::float64()))});
  
  auto table_projected = table->RemoveColumn(1).ValueOrDie();
  serialize_scan_request_to_bufferlist(filter, schema, in_);
  ASSERT_EQ(0, ioctx.exec("test_object_2", "arrow", "read", in_, out_));
  std::shared_ptr<arrow::Table> table_;
  read_table_from_bufferlist(&table_, out_);
  
  ASSERT_EQ(table->Equals(*table_), 0);
  ASSERT_EQ(table_projected->Equals(*table_), 1);
  ASSERT_EQ(table_->num_columns(), 2);
  ASSERT_EQ(table_->schema()->Equals(*schema), 1);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsSDK, TestSelection) {
  librados::Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  librados::IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  
  // WTITE PATH
  librados::bufferlist in, out;
  std::shared_ptr<arrow::Table> table;
  create_test_arrow_table(&table);
  write_table_to_bufferlist(table, in);
  ASSERT_EQ(0, ioctx.exec("test_object_3", "arrow", "write", in, out));

  // READ PATH
  librados::bufferlist in_, out_;
  std::shared_ptr<arrow::dataset::Expression> filter = ("id"_ == int32_t(8) || "id"_ == int32_t(7)).Copy();
  auto schema = arrow::schema({
      arrow::field("id", arrow::int32()),
      arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))});
  serialize_scan_request_to_bufferlist(filter, schema, in_);
  ASSERT_EQ(0, ioctx.exec("test_object_3", "arrow", "read", in_, out_));
  std::shared_ptr<arrow::Table> table_;
  read_table_from_bufferlist(&table_, out_);
  ASSERT_EQ(table_->num_rows(), 2);
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsSDK, TestEndToEnd) {
  librados::Rados cluster;
  std::string pool_name = "test-pool";
  create_one_pool_pp(pool_name, cluster);
  librados::IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  
  // create and write table 
  librados::bufferlist in, out;
  std::shared_ptr<arrow::Table> table;
  create_test_arrow_table(&table);
  write_table_to_bufferlist(table, in);

  arrow::TableBatchReader table_reader(*table);
  arrow::RecordBatchVector batches;
  table_reader.ReadAll(&batches);

  for (int i = 0; i < 4; i++) {
    std::string obj_id = "obj." + std::to_string(i);
    ASSERT_EQ(0, ioctx.exec(obj_id, "arrow", "write", in, out));
  }

  auto schema = arrow::schema({
      arrow::field("id", arrow::int32()),
      arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))
    });

    arrow::dataset::ObjectVector objects;
    for (int i = 0; i < 1; i++) objects.push_back(std::make_shared<arrow::dataset::Object>("obj." + std::to_string(i)));
 
    auto rados_options = arrow::dataset::RadosOptions::FromPoolName("test-pool");

    auto rados_ds = std::make_shared<arrow::dataset::RadosDataset>(schema, objects, rados_options);
    auto inmemory_ds = std::make_shared<arrow::dataset::InMemoryDataset>(schema, batches);

    auto context = std::make_shared<arrow::dataset::ScanContext>();

    auto rados_scanner_builder = std::make_shared<arrow::dataset::ScannerBuilder>(rados_ds, context);
    auto inmemory_scanner_builder = std::make_shared<arrow::dataset::ScannerBuilder>(inmemory_ds, context);

    rados_scanner_builder->Filter(("id"_ > int32_t(7)).Copy());
    rados_scanner_builder->Project(std::vector<std::string>{"cost", "id"});
    auto rados_scanner = rados_scanner_builder->Finish().ValueOrDie();

    inmemory_scanner_builder->Filter(("id"_ > int32_t(7)).Copy());
    inmemory_scanner_builder->Project(std::vector<std::string>{"cost", "id"});
    auto inmemory_scanner = inmemory_scanner_builder->Finish().ValueOrDie();

    auto expected_table = inmemory_scanner->ToTable().ValueOrDie();
    auto result_table = rados_scanner->ToTable().ValueOrDie();

    ASSERT_EQ(result_table->Equals(*expected_table), 1);
}
