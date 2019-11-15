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

#include "arrow/dataset/dataset.h"

#include <tuple>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/test_util.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/stl.h"
#include "arrow/testing/generator.h"

namespace arrow {
namespace dataset {

class TestSimpleDataFragment : public DatasetFixtureMixin {};

TEST_F(TestSimpleDataFragment, Scan) {
  constexpr int64_t kBatchSize = 1024;
  constexpr int64_t kNumberBatches = 16;

  auto s = schema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, s);
  auto reader = ConstantArrayGenerator::Repeat(kNumberBatches, batch);

  // Creates a SimpleDataFragment of the same repeated batch.
  auto fragment = SimpleDataFragment({kNumberBatches, batch});

  AssertFragmentEquals(reader.get(), &fragment);
}

class TestSimpleDataSource : public DatasetFixtureMixin {};

TEST_F(TestSimpleDataSource, GetFragments) {
  constexpr int64_t kNumberFragments = 4;
  constexpr int64_t kBatchSize = 1024;
  constexpr int64_t kNumberBatches = 16;

  auto s = schema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, s);
  auto reader = ConstantArrayGenerator::Repeat(kNumberBatches * kNumberFragments, batch);

  std::vector<std::shared_ptr<RecordBatch>> batches{kNumberBatches, batch};
  auto fragment = std::make_shared<SimpleDataFragment>(batches);
  // It is safe to copy fragment multiple time since Scan() does not consume
  // the internal array.
  auto source = SimpleDataSource({kNumberFragments, fragment});

  AssertDataSourceEquals(reader.get(), &source);
}

class TestTreeDataSource : public DatasetFixtureMixin {};

TEST_F(TestTreeDataSource, GetFragments) {
  constexpr int64_t kBatchSize = 1024;
  constexpr int64_t kNumberBatches = 16;
  constexpr int64_t kChildPerNode = 2;
  constexpr int64_t kCompleteBinaryTreeDepth = 4;

  auto s = schema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, s);

  auto n_leaves = 1U << kCompleteBinaryTreeDepth;
  auto reader = ConstantArrayGenerator::Repeat(kNumberBatches * n_leaves, batch);

  std::vector<std::shared_ptr<RecordBatch>> batches{kNumberBatches, batch};
  auto fragment = std::make_shared<SimpleDataFragment>(batches);

  // Creates a complete binary tree of depth kCompleteBinaryTreeDepth where the
  // leaves are SimpleDataSource containing kChildPerNode fragments.

  auto l1_leaf_source =
      std::make_shared<SimpleDataSource>(DataFragmentVector{kChildPerNode, fragment});

  auto l2_leaf_tree_source =
      std::make_shared<TreeDataSource>(DataSourceVector{kChildPerNode, l1_leaf_source});

  auto l3_middle_tree_source = std::make_shared<TreeDataSource>(
      DataSourceVector{kChildPerNode, l2_leaf_tree_source});

  auto root_source = std::make_shared<TreeDataSource>(
      DataSourceVector{kChildPerNode, l3_middle_tree_source});

  AssertDataSourceEquals(reader.get(), root_source.get());
}

class TestDataset : public DatasetFixtureMixin {};

TEST_F(TestDataset, TrivialScan) {
  constexpr int64_t kNumberFragments = 4;
  constexpr int64_t kNumberBatches = 16;
  constexpr int64_t kBatchSize = 1024;

  auto s = schema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, s);

  std::vector<std::shared_ptr<RecordBatch>> batches{kNumberBatches, batch};
  auto fragment = std::make_shared<SimpleDataFragment>(batches);
  DataFragmentVector fragments{kNumberFragments, fragment};

  std::vector<std::shared_ptr<DataSource>> sources = {
      std::make_shared<SimpleDataSource>(fragments),
      std::make_shared<SimpleDataSource>(fragments),
  };

  const int64_t total_batches = sources.size() * kNumberFragments * kNumberBatches;
  auto reader = ConstantArrayGenerator::Repeat(total_batches, batch);

  std::shared_ptr<Dataset> dataset;
  ASSERT_OK(Dataset::Make(sources, s, &dataset));

  AssertDatasetEquals(reader.get(), dataset.get());
}

TEST(TestProjector, MismatchedType) {
  constexpr int64_t kBatchSize = 1024;

  auto from_schema = schema({field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, from_schema);
  auto to_schema = schema({field("f64", int32())});

  RecordBatchProjector projector(default_memory_pool(), to_schema);

  std::shared_ptr<RecordBatch> reconciled_batch;
  ASSERT_RAISES(TypeError, projector.Project(*batch, &reconciled_batch));
}

TEST(TestProjector, AugmentWithNull) {
  constexpr int64_t kBatchSize = 1024;

  auto from_schema = schema({field("f64", float64()), field("b", boolean())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, from_schema);
  auto to_schema = schema({field("i32", int32()), field("f64", float64())});

  RecordBatchProjector projector(default_memory_pool(), to_schema);

  std::shared_ptr<Array> null_i32;
  ASSERT_OK(MakeArrayOfNull(int32(), batch->num_rows(), &null_i32));
  auto expected_batch =
      RecordBatch::Make(to_schema, batch->num_rows(), {null_i32, batch->column(0)});

  std::shared_ptr<RecordBatch> reconciled_batch;
  ASSERT_OK(projector.Project(*batch, &reconciled_batch));

  AssertBatchesEqual(*expected_batch, *reconciled_batch);
}

TEST(TestProjector, AugmentWithScalar) {
  constexpr int64_t kBatchSize = 1024;
  constexpr int32_t kScalarValue = 3;

  auto from_schema = schema({field("f64", float64()), field("b", boolean())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, from_schema);
  auto to_schema = schema({field("i32", int32()), field("f64", float64())});

  auto scalar_i32 = std::make_shared<Int32Scalar>(kScalarValue);

  RecordBatchProjector projector(default_memory_pool(), to_schema, {scalar_i32, nullptr});

  ASSERT_OK_AND_ASSIGN(auto array_i32,
                       ArrayFromBuilderVisitor(int32(), kBatchSize, [](Int32Builder* b) {
                         b->UnsafeAppend(kScalarValue);
                       }));

  auto expected_batch =
      RecordBatch::Make(to_schema, batch->num_rows(), {array_i32, batch->column(0)});

  std::shared_ptr<RecordBatch> reconciled_batch;
  ASSERT_OK(projector.Project(*batch, &reconciled_batch));

  AssertBatchesEqual(*expected_batch, *reconciled_batch);
}

TEST(TestProjector, NonTrivial) {
  constexpr int64_t kBatchSize = 1024;

  constexpr float kScalarValue = 3.14f;

  auto from_schema =
      schema({field("i8", int8()), field("u8", uint8()), field("i16", int16()),
              field("u16", uint16()), field("i32", int32()), field("u32", uint32())});

  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, from_schema);

  auto to_schema =
      schema({field("i32", int32()), field("f64", float64()), field("u16", uint16()),
              field("u8", uint8()), field("b", boolean()), field("u32", uint32()),
              field("f32", float32())});

  auto scalar_f32 = std::make_shared<FloatScalar>(kScalarValue);
  auto scalar_f64 = std::make_shared<DoubleScalar>(kScalarValue);

  RecordBatchProjector projector(
      default_memory_pool(), to_schema,
      {nullptr /* i32 */, scalar_f64, nullptr /* u16 */, nullptr /* u8 */,
       nullptr /* b */, nullptr /* u32 */, scalar_f32});

  ASSERT_OK_AND_ASSIGN(
      auto array_f32, ArrayFromBuilderVisitor(float32(), kBatchSize, [](FloatBuilder* b) {
        b->UnsafeAppend(kScalarValue);
      }));
  ASSERT_OK_AND_ASSIGN(auto array_f64, ArrayFromBuilderVisitor(
                                           float64(), kBatchSize, [](DoubleBuilder* b) {
                                             b->UnsafeAppend(kScalarValue);
                                           }));
  ASSERT_OK_AND_ASSIGN(
      auto null_b, ArrayFromBuilderVisitor(boolean(), kBatchSize, [](BooleanBuilder* b) {
        b->UnsafeAppendNull();
      }));

  auto expected_batch = RecordBatch::Make(
      to_schema, batch->num_rows(),
      {batch->GetColumnByName("i32"), array_f64, batch->GetColumnByName("u16"),
       batch->GetColumnByName("u8"), null_b, batch->GetColumnByName("u32"), array_f32});

  std::shared_ptr<RecordBatch> reconciled_batch;
  ASSERT_OK(projector.Project(*batch, &reconciled_batch));

  AssertBatchesEqual(*expected_batch, *reconciled_batch);
}

class TestEndToEnd : public TestDataset {
  void SetUp() {
    bool nullable = false;
    schema_ = schema({
        field("country", utf8(), nullable),
        field("region", utf8(), nullable),
        field("model", utf8(), nullable),
        field("year", int32(), nullable),
        field("sales", int32(), nullable),
    });

    using PathAndContent = std::vector<std::pair<std::string, std::string>>;
    auto files = PathAndContent{
        {"/dataset/2018/01/US.json", R"([
        {"country": "US", "region": "NY", "year": 2018, "model": "3", "sales": 742},
        {"country": "US", "region": "NY", "year": 2018, "model": "S", "sales": 304},
        {"country": "US", "region": "NY", "year": 2018, "model": "X", "sales": 136},
        {"country": "US", "region": "NY", "year": 2018, "model": "Y", "sales": 27}
      ])"},
        {"/dataset/2018/01/CA.json", R"([
        {"country": "US", "region": "CA", "year": 2018, "model": "3", "sales": 512},
        {"country": "US", "region": "CA", "year": 2018, "model": "S", "sales": 978},
        {"country": "US", "region": "CA", "year": 2018, "model": "X", "sales": 1},
        {"country": "US", "region": "CA", "year": 2018, "model": "Y", "sales": 69}
      ])"},
        {"/dataset/2019/01/US.json", R"([
        {"country": "CA", "region": "QC", "year": 2019, "model": "3", "sales": 273},
        {"country": "CA", "region": "QC", "year": 2019, "model": "S", "sales": 13},
        {"country": "CA", "region": "QC", "year": 2019, "model": "X", "sales": 54},
        {"country": "CA", "region": "QC", "year": 2019, "model": "Y", "sales": 21}
      ])"},
        {"/dataset/2019/01/CA.json", R"([
        {"country": "CA", "region": "QC", "year": 2019, "model": "3", "sales": 152},
        {"country": "CA", "region": "QC", "year": 2019, "model": "S", "sales": 10},
        {"country": "CA", "region": "QC", "year": 2019, "model": "X", "sales": 42},
        {"country": "CA", "region": "QC", "year": 2019, "model": "Y", "sales": 37}
      ])"},
        {"/dataset/.pesky", "garbage content"},
    };

    auto mock_fs = std::make_shared<fs::internal::MockFileSystem>(fs::kNoTime);
    for (const auto& f : files) {
      ARROW_EXPECT_OK(mock_fs->CreateFile(f.first, f.second, /* recursive */ true));
    }

    fs_ = mock_fs;
  }

 protected:
  std::shared_ptr<fs::FileSystem> fs_;
  std::shared_ptr<Schema> schema_;
};

TEST_F(TestEndToEnd, EndToEndSingleSource) {
  // The dataset API is divided in 3 parts:
  //  - Creation
  //  - Querying
  //  - Consuming

  // Creation.
  //
  // A Dataset is the union of one or more DataSources with the same schema.
  // Example of DataSource, FileSystemDataSource, OdbcDataSource,
  // FlightDataSource.

  // A DataSource is composed of DataFragments. Each DataFragment can yield
  // multiple RecordBatches. DataSources can be created manually or "discovered"
  // via the DataSourceDiscovery interface.
  //
  // Each DataSourceDiscovery may have different options.
  std::shared_ptr<DataSourceDiscovery> discovery;

  // The user must specify which FileFormat is used to create FileFragments.
  // This option is specific to FileSystemBasedDataSource (and the builder).
  //
  // Note the JSONRecordBatchFileFormat requires a schema before creating the Discovery
  // class which is used to discover the schema. This is a chicken-and-egg problem
  // which only applies to the JSONRecordBatchFileFormat.
  auto format = std::make_shared<JSONRecordBatchFileFormat>(schema_);
  // A selector is used to crawl files and directories of a
  // filesystem. If the options in Selector are not enough, the
  // FileSystemDataSourceDiscovery class also supports an explicit list of
  // fs::FileStats instead of the selector.
  fs::Selector s;
  s.base_dir = "/dataset";
  s.recursive = true;

  // Further options can be given to the discovery mechanism via the
  // FileSystemDiscoveryOptions configuration class. See the docstring for more
  // information.
  FileSystemDiscoveryOptions options;
  options.ignore_prefixes = {"."};

  ASSERT_OK(
      FileSystemDataSourceDiscovery::Make(fs_.get(), s, format, options, &discovery));

  // DataFragments might have compatible but slightly different schemas, e.g.
  // schema evolved by adding/renaming columns. In this case, the schema is
  // passed to the dataset constructor.
  std::shared_ptr<Schema> inspected_schema;
  ASSERT_OK(discovery->Inspect(&inspected_schema));
  EXPECT_EQ(*schema_, *inspected_schema);

  // Partitions expressions can be discovered for DataSource and DataFragments.
  // This metadata is then used in conjuction with the query filter to apply
  // the pushdown predicate optimization.
  auto partition_schema =
      schema({field("year", int32()), field("month", int32()), field("country", utf8())});
  // The SchemaPartitionScheme is a simple scheme where the path is split with
  // the directory separator character and the components are typed and named
  // with the equivalent index in the schema, e.g.
  // (with the previous defined schema):
  //
  // - "/2019" -> {"year": 2019}
  // - "/2019/01 -> {"year": 2019, "month": 1}
  // - "/2019/01/CA -> {"year": 2019, "month": 1, "country": "CA"}
  // - "/2019/01/CA/a_file.json -> {"year": 2019, "month": 1, "country": "CA"}
  auto partition_scheme = std::make_shared<SchemaPartitionScheme>(partition_schema);
  ASSERT_OK(discovery->SetPartitionScheme(partition_scheme));

  // Build the DataSource where partitions are attached to fragments (files).
  std::shared_ptr<DataSource> datasource;
  ASSERT_OK(discovery->Finish(&datasource));

  // Create the Dataset from our single DataSource.
  std::shared_ptr<Dataset> dataset =
      std::make_shared<Dataset>(DataSourceVector{datasource}, inspected_schema);

  // Querying.
  //
  // The Scan operator materializes data from io into memory. Avoiding data
  // transfer is a critical optimization done by analytical engine. Thus, a
  // Scan can take multiple options, notably a subset of columns and a filter
  // expression.
  std::unique_ptr<ScannerBuilder> scanner_builder;
  ASSERT_OK(dataset->NewScan(&scanner_builder));

  // An optional subset of columns can be provided. This will trickle to
  // DataFragment drivers. The net effect is that only columns of interest will
  // be materialized if the DataFragment supports it. This is the major benefit
  // of using a column-major format versus a row-major format.
  //
  // This API decouples the DataSource/DataFragment implementation and column
  // projection from the query part.
  //
  // For example, a ParquetFileDataFragment may read the necessary byte ranges
  // exclusively, ranges, or an OdbcDataFragment could convert the projection to a SELECT
  // statement. The CsvFileDataFragment wouldn't benefit from this as much, but
  // can still benefit from skipping conversion of unneeded columns.
  std::vector<std::string> columns{"sales", "model"};
  ASSERT_OK(scanner_builder->Project(columns));

  // An optional filter expression may also be specified. The filter expression
  // is evaluated against input rows. Only rows for which the filter evauates to true are
  // yielded. Predicate pushdown optimizations are applied using partition information if
  // available.
  //
  // This API decouples predicate pushdown from the DataSource implementation
  // and partition discovery.
  //
  // The following filter tests both predicate pushdown and post filtering
  // without partition information because `year` is a partition and `sales` is
  // not.
  auto filter = ("year"_ == 2019 && "sales"_ > 100);
  ASSERT_OK(scanner_builder->Filter(filter));

  std::unique_ptr<Scanner> scanner;
  ASSERT_OK(scanner_builder->Finish(&scanner));

  // In the simplest case, consumption is simply conversion to a Table.
  std::shared_ptr<Table> table;
  ASSERT_OK(scanner->ToTable(&table));

  using row_type = std::tuple<int32_t, std::string>;
  std::vector<row_type> rows{
      row_type{152, "3"},
      row_type{273, "3"},
  };
  std::shared_ptr<Table> expected;
  ASSERT_OK(stl::TableFromTupleRange(default_memory_pool(), rows, columns, &expected));
  AssertTablesEqual(*expected, *table, false, true);
}

}  // namespace dataset
}  // namespace arrow
