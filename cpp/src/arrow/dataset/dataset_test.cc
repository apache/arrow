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

#include <optional>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/test_util_internal.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/stl.h"
#include "arrow/testing/generator.h"

namespace arrow {
namespace dataset {

class TestInMemoryFragment : public DatasetFixtureMixin {};

using RecordBatchVector = std::vector<std::shared_ptr<RecordBatch>>;

TEST_F(TestInMemoryFragment, Scan) {
  constexpr int64_t kBatchSize = 1024;
  constexpr int64_t kNumberBatches = 16;

  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);
  auto reader = ConstantArrayGenerator::Repeat(kNumberBatches, batch);

  // Creates a InMemoryFragment of the same repeated batch.
  RecordBatchVector batches = {static_cast<size_t>(kNumberBatches), batch};
  auto fragment = std::make_shared<InMemoryFragment>(batches);

  AssertFragmentEquals(reader.get(), fragment.get());
}

class TestInMemoryDataset : public DatasetFixtureMixin {};

TEST_F(TestInMemoryDataset, ReplaceSchema) {
  constexpr int64_t kBatchSize = 1;
  constexpr int64_t kNumberBatches = 1;

  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);
  auto reader = ConstantArrayGenerator::Repeat(kNumberBatches, batch);

  auto dataset = std::make_shared<InMemoryDataset>(
      schema_, RecordBatchVector{static_cast<size_t>(kNumberBatches), batch});

  // drop field
  auto new_schema = schema({field("i32", int32())});
  ASSERT_OK_AND_ASSIGN(auto new_dataset, dataset->ReplaceSchema(new_schema));
  AssertDatasetHasSchema(new_dataset, new_schema);
  // add field (will be materialized as null during projection)
  new_schema = schema({field("str", utf8())});
  ASSERT_OK_AND_ASSIGN(new_dataset, dataset->ReplaceSchema(new_schema));
  AssertDatasetHasSchema(new_dataset, new_schema);
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

TEST_F(TestInMemoryDataset, GetFragments) {
  constexpr int64_t kBatchSize = 1024;
  constexpr int64_t kNumberBatches = 16;

  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);
  auto reader = ConstantArrayGenerator::Repeat(kNumberBatches, batch);

  auto dataset = std::make_shared<InMemoryDataset>(
      schema_, RecordBatchVector{static_cast<size_t>(kNumberBatches), batch});

  AssertDatasetEquals(reader.get(), dataset.get());
}

TEST_F(TestInMemoryDataset, InMemoryFragment) {
  constexpr int64_t kBatchSize = 1024;

  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);
  RecordBatchVector batches{batch};

  // Regression test: previously this constructor relied on undefined behavior (order of
  // evaluation of arguments) leading to fragments being constructed with empty schemas
  auto fragment = std::make_shared<InMemoryFragment>(batches);
  ASSERT_OK_AND_ASSIGN(auto schema, fragment->ReadPhysicalSchema());
  AssertSchemaEqual(batch->schema(), schema);
}

TEST_F(TestInMemoryDataset, HandlesDifferingSchemas) {
  constexpr int64_t kBatchSize = 1024;

  // These schemas can be merged
  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch1 = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);
  SetSchema({field("i32", int32())});
  auto batch2 = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);
  RecordBatchVector batches{batch1, batch2};

  auto dataset = std::make_shared<InMemoryDataset>(schema_, batches);

  ASSERT_OK_AND_ASSIGN(auto scanner_builder, dataset->NewScan());
  ASSERT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());
  ASSERT_OK_AND_ASSIGN(auto table, scanner->ToTable());
  ASSERT_EQ(*table->schema(), *schema_);
  ASSERT_EQ(table->num_rows(), 2 * kBatchSize);

  // These cannot be merged
  SetSchema({field("i32", int32()), field("f64", float64())});
  batch1 = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);
  SetSchema({field("i32", struct_({field("x", date32())}))});
  batch2 = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);
  batches = RecordBatchVector({batch1, batch2});

  dataset = std::make_shared<InMemoryDataset>(schema_, batches);

  ASSERT_OK_AND_ASSIGN(scanner_builder, dataset->NewScan());
  ASSERT_OK_AND_ASSIGN(scanner, scanner_builder->Finish());
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError, testing::HasSubstr("fields had matching names but differing types"),
      scanner->ToTable());
}

TEST_F(TestInMemoryDataset, GetFragmentsSync) {
  constexpr int64_t kBatchSize = 1024;
  constexpr int64_t kNumberBatches = 16;

  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);
  auto reader = ConstantArrayGenerator::Repeat(kNumberBatches, batch);

  auto dataset = std::make_shared<InMemoryDataset>(
      schema_, RecordBatchVector{static_cast<size_t>(kNumberBatches), batch});

  AssertDatasetFragmentsEqual(reader.get(), dataset.get());
}

TEST_F(TestInMemoryDataset, GetFragmentsAsync) {
  constexpr int64_t kBatchSize = 1024;
  constexpr int64_t kNumberBatches = 16;

  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);
  auto reader = ConstantArrayGenerator::Repeat(kNumberBatches, batch);

  auto dataset = std::make_shared<InMemoryDataset>(
      schema_, RecordBatchVector{static_cast<size_t>(kNumberBatches), batch});

  AssertDatasetAsyncFragmentsEqual(reader.get(), dataset.get());
}

class TestUnionDataset : public DatasetFixtureMixin {};

TEST_F(TestUnionDataset, ReplaceSchema) {
  constexpr int64_t kBatchSize = 1;
  constexpr int64_t kNumberBatches = 1;

  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);

  std::vector<std::shared_ptr<RecordBatch>> batches{static_cast<size_t>(kNumberBatches),
                                                    batch};

  DatasetVector children = {
      std::make_shared<InMemoryDataset>(schema_, batches),
      std::make_shared<InMemoryDataset>(schema_, batches),
  };

  const int64_t total_batches = children.size() * kNumberBatches;
  auto reader = ConstantArrayGenerator::Repeat(total_batches, batch);

  ASSERT_OK_AND_ASSIGN(auto dataset, UnionDataset::Make(schema_, children));
  AssertDatasetEquals(reader.get(), dataset.get());

  // drop field
  auto new_schema = schema({field("i32", int32())});
  ASSERT_OK_AND_ASSIGN(auto new_dataset, dataset->ReplaceSchema(new_schema));
  AssertDatasetHasSchema(new_dataset, new_schema);
  // add nullable field (will be materialized as null during projection)
  new_schema = schema({field("str", utf8())});
  ASSERT_OK_AND_ASSIGN(new_dataset, dataset->ReplaceSchema(new_schema));
  AssertDatasetHasSchema(new_dataset, new_schema);
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

TEST_F(TestUnionDataset, GetFragments) {
  constexpr int64_t kBatchSize = 1024;
  constexpr int64_t kChildPerNode = 2;
  constexpr int64_t kCompleteBinaryTreeDepth = 4;

  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);

  auto n_leaves = 1U << kCompleteBinaryTreeDepth;
  auto reader = ConstantArrayGenerator::Repeat(n_leaves, batch);

  // Creates a complete binary tree of depth kCompleteBinaryTreeDepth where the
  // leaves are InMemoryDataset containing kChildPerNode fragments.

  auto l1_leaf_dataset = std::make_shared<InMemoryDataset>(
      schema_, RecordBatchVector{static_cast<size_t>(kChildPerNode), batch});

  ASSERT_OK_AND_ASSIGN(
      auto l2_leaf_tree_dataset,
      UnionDataset::Make(
          schema_, DatasetVector{static_cast<size_t>(kChildPerNode), l1_leaf_dataset}));

  ASSERT_OK_AND_ASSIGN(
      auto l3_middle_tree_dataset,
      UnionDataset::Make(schema_, DatasetVector{static_cast<size_t>(kChildPerNode),
                                                l2_leaf_tree_dataset}));

  ASSERT_OK_AND_ASSIGN(
      auto root_dataset,
      UnionDataset::Make(schema_, DatasetVector{static_cast<size_t>(kChildPerNode),
                                                l3_middle_tree_dataset}));

  AssertDatasetEquals(reader.get(), root_dataset.get());
}

TEST_F(TestUnionDataset, TrivialScan) {
  constexpr int64_t kNumberBatches = 16;
  constexpr int64_t kBatchSize = 1024;

  SetSchema({field("i32", int32()), field("f64", float64())});
  auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);

  std::vector<std::shared_ptr<RecordBatch>> batches{static_cast<size_t>(kNumberBatches),
                                                    batch};

  DatasetVector children = {
      std::make_shared<InMemoryDataset>(schema_, batches),
      std::make_shared<InMemoryDataset>(schema_, batches),
  };

  const int64_t total_batches = children.size() * kNumberBatches;
  auto reader = ConstantArrayGenerator::Repeat(total_batches, batch);

  ASSERT_OK_AND_ASSIGN(auto dataset, UnionDataset::Make(schema_, children));
  AssertDatasetEquals(reader.get(), dataset.get());
}

TEST(TestProjector, CheckProjectable) {
  struct Assert {
    explicit Assert(FieldVector from) : from_(from) {}
    Schema from_;

    void ProjectableTo(FieldVector to) {
      ARROW_EXPECT_OK(CheckProjectable(from_, Schema(to)));
    }

    void NotProjectableTo(FieldVector to, std::string substr = "") {
      EXPECT_RAISES_WITH_MESSAGE_THAT(TypeError, testing::HasSubstr(substr),
                                      CheckProjectable(from_, Schema(to)));
    }
  };

  auto i8 = field("i8", int8());
  auto u16 = field("u16", uint16());
  auto str = field("str", utf8());
  auto i8_req = field("i8", int8(), false);
  auto u16_req = field("u16", uint16(), false);
  auto str_req = field("str", utf8(), false);
  auto str_nil = field("str", null());

  // trivial
  Assert({}).ProjectableTo({});
  Assert({i8}).ProjectableTo({i8});
  Assert({i8, u16_req}).ProjectableTo({i8, u16_req});

  // reorder
  Assert({i8, u16}).ProjectableTo({u16, i8});
  Assert({i8, str, u16}).ProjectableTo({u16, i8, str});

  // drop field(s)
  Assert({i8}).ProjectableTo({});

  // add field(s)
  Assert({}).ProjectableTo({i8});
  Assert({}).ProjectableTo({i8, u16});
  Assert({}).NotProjectableTo({u16_req},
                              "is not nullable and does not exist in origin schema");
  Assert({i8}).NotProjectableTo({u16_req, i8});

  // change nullability
  Assert({i8}).NotProjectableTo({i8_req},
                                "not nullable but is not required in origin schema");
  Assert({i8_req}).ProjectableTo({i8});
  Assert({str_nil}).ProjectableTo({str});
  Assert({str_nil}).NotProjectableTo({str_req});

  // change field type
  Assert({i8}).NotProjectableTo({field("i8", utf8())},
                                "fields had matching names but differing types");
}

class TestEndToEnd : public TestUnionDataset {
  void SetUp() override {
    bool nullable = false;
    SetSchema({
        field("region", utf8(), nullable),
        field("model", utf8(), nullable),
        field("sales", float64(), nullable),
        // partition columns
        field("year", int32()),
        field("month", int32()),
        field("country", utf8()),
    });

    using PathAndContent = std::vector<std::pair<std::string, std::string>>;
    auto files = PathAndContent{
        {"/dataset/2018/01/US/dat.json", R"([
        {"region": "NY", "model": "3", "sales": 742.0},
        {"region": "NY", "model": "S", "sales": 304.125},
        {"region": "NY", "model": "X", "sales": 136.25},
        {"region": "NY", "model": "Y", "sales": 27.5}
      ])"},
        {"/dataset/2018/01/CA/dat.json", R"([
        {"region": "CA", "model": "3", "sales": 512},
        {"region": "CA", "model": "S", "sales": 978},
        {"region": "CA", "model": "X", "sales": 1.0},
        {"region": "CA", "model": "Y", "sales": 69}
      ])"},
        {"/dataset/2019/01/US/dat.json", R"([
        {"region": "QC", "model": "3", "sales": 273.5},
        {"region": "QC", "model": "S", "sales": 13},
        {"region": "QC", "model": "X", "sales": 54},
        {"region": "QC", "model": "Y", "sales": 21}
      ])"},
        {"/dataset/2019/01/CA/dat.json", R"([
        {"region": "QC", "model": "3", "sales": 152.25},
        {"region": "QC", "model": "S", "sales": 10},
        {"region": "QC", "model": "X", "sales": 42},
        {"region": "QC", "model": "Y", "sales": 37}
      ])"},
        {"/dataset/.pesky", "garbage content"},
    };

    auto mock_fs = std::make_shared<fs::internal::MockFileSystem>(fs::kNoTime);
    for (const auto& f : files) {
      ARROW_EXPECT_OK(mock_fs->CreateFile(f.first, f.second, /*recursive=*/true));
    }

    fs_ = mock_fs;
  }

 protected:
  std::shared_ptr<fs::FileSystem> fs_;
};

TEST_F(TestEndToEnd, EndToEndSingleDataset) {
  // The dataset API is divided in 3 parts:
  //  - Creation
  //  - Querying
  //  - Consuming

  // Creation.
  //
  // A Dataset is the union of one or more Datasets with the same schema.
  // Example of Dataset, FileSystemDataset, OdbcDataset,
  // FlightDataset.

  // A Dataset is composed of Fragments. Each Fragment can yield
  // multiple RecordBatches. Datasets can be created manually or "discovered"
  // via the DatasetFactory interface.
  std::shared_ptr<DatasetFactory> factory;

  // The user must specify which FileFormat is used to create FileFragments.
  // This option is specific to FileSystemDataset (and the builder).
  auto format_schema = SchemaFromColumnNames(schema_, {"region", "model", "sales"});
  auto format = std::make_shared<JSONRecordBatchFileFormat>(format_schema);

  // A selector is used to crawl files and directories of a
  // filesystem. If the options in FileSelector are not enough, the
  // FileSystemDatasetFactory class also supports an explicit list of
  // fs::FileInfo instead of the selector.
  fs::FileSelector s;
  s.base_dir = "/dataset";
  s.recursive = true;

  // Further options can be given to the factory mechanism via the
  // FileSystemFactoryOptions configuration class. See the docstring for more
  // information.
  FileSystemFactoryOptions options;
  options.selector_ignore_prefixes = {"."};

  // Partitions expressions can be discovered for Dataset and Fragments.
  // This metadata is then used in conjunction with the query filter to apply
  // the pushdown predicate optimization.
  //
  // The DirectoryPartitioning is a partitioning where the path is split with
  // the directory separator character and the components are parsed as values
  // of the corresponding fields in its schema.
  //
  // Since a PartitioningFactory is specified instead of an explicit
  // Partitioning, the types of partition fields will be inferred.
  //
  // - "/2019" -> {"year": 2019}
  // - "/2019/01 -> {"year": 2019, "month": 1}
  // - "/2019/01/CA -> {"year": 2019, "month": 1, "country": "CA"}
  // - "/2019/01/CA/a_file.json -> {"year": 2019, "month": 1, "country": "CA"}
  options.partitioning = DirectoryPartitioning::MakeFactory({"year", "month", "country"});

  ASSERT_OK_AND_ASSIGN(factory, FileSystemDatasetFactory::Make(fs_, s, format, options));

  // Fragments might have compatible but slightly different schemas, e.g.
  // schema evolved by adding/renaming columns. In this case, the schema is
  // passed to the dataset constructor.
  // The inspected_schema may optionally be modified before being finalized.
  InspectOptions inspect_options;
  inspect_options.fragments = InspectOptions::kInspectAllFragments;
  ASSERT_OK_AND_ASSIGN(auto inspected_schema, factory->Inspect(inspect_options));
  EXPECT_EQ(*schema_, *inspected_schema);

  // Build the Dataset where partitions are attached to fragments (files).
  ASSERT_OK_AND_ASSIGN(auto source, factory->Finish(inspected_schema));

  // Create the Dataset from our single Dataset.
  ASSERT_OK_AND_ASSIGN(auto dataset, UnionDataset::Make(inspected_schema, {source}));

  // Querying.
  //
  // The Scan operator materializes data from io into memory. Avoiding data
  // transfer is a critical optimization done by analytical engine. Thus, a
  // Scan can take multiple options, notably a subset of columns and a filter
  // expression.
  ASSERT_OK_AND_ASSIGN(auto scanner_builder, dataset->NewScan());

  // An optional subset of columns can be provided. This will trickle to
  // Fragment drivers. The net effect is that only columns of interest will
  // be materialized if the Fragment supports it. This is the major benefit
  // of using a column-major format versus a row-major format.
  //
  // This API decouples the Dataset/Fragment implementation and column
  // projection from the query part.
  //
  // For example, a ParquetFileFragment may read the necessary byte ranges
  // exclusively, ranges, or an OdbcFragment could convert the projection to a SELECT
  // statement. The CsvFileFragment wouldn't benefit from this as much, but
  // can still benefit from skipping conversion of unneeded columns.
  std::vector<std::string> columns{"sales", "model", "country"};
  ASSERT_OK(scanner_builder->Project(columns));

  // An optional filter expression may also be specified. The filter expression
  // is evaluated against input rows. Only rows for which the filter evaluates to true
  // are yielded. Predicate pushdown optimizations are applied using partition
  // information if available.
  //
  // This API decouples predicate pushdown from the Dataset implementation
  // and partition discovery.
  //
  // The following filter tests both predicate pushdown and post filtering
  // without partition information because `year` is a partition and `sales` is
  // not.
  auto filter = and_(equal(field_ref("year"), literal(2019)),
                     greater(field_ref("sales"), literal(100.0)));
  ASSERT_OK(scanner_builder->Filter(filter));

  ASSERT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());
  // In the simplest case, consumption is simply conversion to a Table.
  ASSERT_OK_AND_ASSIGN(auto table, scanner->ToTable());

  auto expected = TableFromJSON(scanner_builder->projected_schema(), {R"([
    {"sales": 152.25, "model": "3", "country": "CA"},
    {"sales": 273.5, "model": "3", "country": "US"}
  ])"});
  AssertTablesEqual(*expected, *table, false, true);
}

inline std::shared_ptr<Schema> SchemaFromNames(const std::vector<std::string> names) {
  std::vector<std::shared_ptr<Field>> fields;
  for (const auto& name : names) {
    fields.push_back(field(name, int32()));
  }

  return schema(fields);
}

class TestSchemaUnification : public TestUnionDataset {
 public:
  using i32 = std::optional<int32_t>;
  using PathAndContent = std::vector<std::pair<std::string, std::string>>;

  void SetUp() override {
    // The following test creates 2 sources with divergent but compatible
    // schemas. Each source have a common partitioning where the
    // fields are not materialized in the data fragments.
    //
    // Each data is composed of 2 data fragments with divergent but
    // compatible schemas. The data fragment within a source share at
    // least one column.
    //
    // Thus, the fixture helps verifying various scenarios where the Scanner
    // must fix the RecordBatches to align with the final unified schema exposed
    // to the consumer.
    static constexpr auto ds1_df1 = "/dataset/alpha/part_ds=1/part_df=1/data.json";
    static constexpr auto ds1_df2 = "/dataset/alpha/part_ds=1/part_df=2/data.json";
    static constexpr auto ds2_df1 = "/dataset/beta/part_ds=2/part_df=1/data.json";
    static constexpr auto ds2_df2 = "/dataset/beta/part_ds=2/part_df=2/data.json";
    auto files = PathAndContent{
        // First Dataset
        {ds1_df1, R"([{"phy_1": 111, "phy_2": 211}])"},
        {ds1_df2, R"([{"phy_2": 212, "phy_3": 312}])"},
        // Second Dataset
        {ds2_df1, R"([{"phy_3": 321, "phy_4": 421}])"},
        {ds2_df2, R"([{"phy_4": 422, "phy_2": 222}])"},
    };

    auto mock_fs = std::make_shared<fs::internal::MockFileSystem>(fs::kNoTime);
    for (const auto& f : files) {
      ARROW_EXPECT_OK(mock_fs->CreateFile(f.first, f.second, /* recursive */ true));
    }
    fs_ = mock_fs;

    auto get_source =
        [this](std::string base,
               std::vector<std::string> paths) -> Result<std::shared_ptr<Dataset>> {
      auto resolver = [](const FileSource& source) -> std::shared_ptr<Schema> {
        auto path = source.path();
        // A different schema for each data fragment.
        if (path == ds1_df1) {
          return SchemaFromNames({"phy_1", "phy_2"});
        } else if (path == ds1_df2) {
          return SchemaFromNames({"phy_2", "phy_3"});
        } else if (path == ds2_df1) {
          return SchemaFromNames({"phy_3", "phy_4"});
        } else if (path == ds2_df2) {
          return SchemaFromNames({"phy_4", "phy_2"});
        }

        return nullptr;
      };

      auto format = std::make_shared<JSONRecordBatchFileFormat>(resolver);

      FileSystemFactoryOptions options;
      options.partition_base_dir = base;
      options.partitioning =
          std::make_shared<HivePartitioning>(SchemaFromNames({"part_ds", "part_df"}));

      ARROW_ASSIGN_OR_RAISE(auto factory,
                            FileSystemDatasetFactory::Make(fs_, paths, format, options));

      ARROW_ASSIGN_OR_RAISE(auto schema, factory->Inspect());

      return factory->Finish(schema);
    };

    schema_ = SchemaFromNames({"phy_1", "phy_2", "phy_3", "phy_4", "part_ds", "part_df"});
    ASSERT_OK_AND_ASSIGN(auto ds1, get_source("/dataset/alpha", {ds1_df1, ds1_df2}));
    ASSERT_OK_AND_ASSIGN(auto ds2, get_source("/dataset/beta", {ds2_df1, ds2_df2}));

    // FIXME(bkietz) this is a hack: allow differing schemas for the purposes of this
    // test
    class DisparateSchemasUnionDataset : public UnionDataset {
     public:
      DisparateSchemasUnionDataset(std::shared_ptr<Schema> schema, DatasetVector children)
          : UnionDataset(std::move(schema), std::move(children)) {}
    };
    dataset_ =
        std::make_shared<DisparateSchemasUnionDataset>(schema_, DatasetVector{ds1, ds2});
  }

  template <typename TupleType>
  void AssertScanEquals(std::shared_ptr<Scanner> scanner,
                        const std::vector<TupleType>& expected_rows) {
    std::vector<std::string> columns;
    for (const auto& field : scanner->options()->projected_schema->fields()) {
      columns.push_back(field->name());
    }

    ASSERT_OK_AND_ASSIGN(auto actual, scanner->ToTable());
    std::shared_ptr<Table> expected;
    ASSERT_OK(stl::TableFromTupleRange(default_memory_pool(), expected_rows, columns,
                                       &expected));
    AssertTablesEqual(*expected, *actual, false, true);
  }

  template <typename TupleType>
  void AssertBuilderEquals(std::shared_ptr<ScannerBuilder> builder,
                           const std::vector<TupleType>& expected_rows) {
    ASSERT_OK_AND_ASSIGN(auto scanner, builder->Finish());
    AssertScanEquals(scanner, expected_rows);
  }

 protected:
  std::shared_ptr<fs::FileSystem> fs_;
  std::shared_ptr<Dataset> dataset_;
};

using std::nullopt;

TEST_F(TestSchemaUnification, SelectStar) {
  // This is a `SELECT * FROM dataset` where it ensures:
  //
  // - proper re-ordering of columns
  // - materializing missing physical columns in Fragments
  // - materializing missing partition columns extracted from Partitioning
  ASSERT_OK_AND_ASSIGN(auto scan_builder, dataset_->NewScan());

  using TupleType = std::tuple<i32, i32, i32, i32, i32, i32>;
  std::vector<TupleType> rows = {
      TupleType(111, 211, nullopt, nullopt, 1, 1),
      TupleType(nullopt, 212, 312, nullopt, 1, 2),
      TupleType(nullopt, nullopt, 321, 421, 2, 1),
      TupleType(nullopt, 222, nullopt, 422, 2, 2),
  };

  AssertBuilderEquals(scan_builder, rows);
}

TEST_F(TestSchemaUnification, SelectPhysicalColumns) {
  // Same as above, but scoped to physical columns.
  ASSERT_OK_AND_ASSIGN(auto scan_builder, dataset_->NewScan());
  ASSERT_OK(scan_builder->Project({"phy_1", "phy_2", "phy_3", "phy_4"}));

  using TupleType = std::tuple<i32, i32, i32, i32>;
  std::vector<TupleType> rows = {
      TupleType(111, 211, nullopt, nullopt),
      TupleType(nullopt, 212, 312, nullopt),
      TupleType(nullopt, nullopt, 321, 421),
      TupleType(nullopt, 222, nullopt, 422),
  };

  AssertBuilderEquals(scan_builder, rows);
}

TEST_F(TestSchemaUnification, SelectSomeReorderedPhysicalColumns) {
  // Select physical columns in a different order than physical Fragments
  ASSERT_OK_AND_ASSIGN(auto scan_builder, dataset_->NewScan());
  ASSERT_OK(scan_builder->Project({"phy_2", "phy_1", "phy_4"}));

  using TupleType = std::tuple<i32, i32, i32>;
  std::vector<TupleType> rows = {
      TupleType(211, 111, nullopt),
      TupleType(212, nullopt, nullopt),
      TupleType(nullopt, nullopt, 421),
      TupleType(222, nullopt, 422),
  };

  AssertBuilderEquals(scan_builder, rows);
}

TEST_F(TestSchemaUnification, SelectPhysicalColumnsFilterPartitionColumn) {
  // Select a subset of physical column with a filter on a missing physical
  // column and a partition column, it ensures:
  //
  // - Can filter on virtual and physical columns with a non-trivial filter
  //   when some of the columns may not be materialized
  ASSERT_OK_AND_ASSIGN(auto scan_builder, dataset_->NewScan());
  ASSERT_OK(scan_builder->Project({"phy_2", "phy_3", "phy_4"}));
  ASSERT_OK(scan_builder->Filter(or_(and_(equal(field_ref("part_df"), literal(1)),
                                          equal(field_ref("phy_2"), literal(211))),
                                     and_(equal(field_ref("part_ds"), literal(2)),
                                          not_equal(field_ref("phy_4"), literal(422))))));

  using TupleType = std::tuple<i32, i32, i32>;
  std::vector<TupleType> rows = {
      TupleType(211, nullopt, nullopt),
      TupleType(nullopt, 321, 421),
  };

  AssertBuilderEquals(scan_builder, rows);
}

TEST_F(TestSchemaUnification, SelectSyntheticColumn) {
  // Select only a synthetic column
  ASSERT_OK_AND_ASSIGN(auto scan_builder, dataset_->NewScan());
  ASSERT_OK(scan_builder->Project(
      {call("add", {field_ref("phy_1"), field_ref("part_df")})}, {"phy_1 + part_df"}));

  ASSERT_OK_AND_ASSIGN(auto scanner, scan_builder->Finish());
  AssertSchemaEqual(Schema({field("phy_1 + part_df", int32())}),
                    *scanner->options()->projected_schema);

  using TupleType = std::tuple<i32>;
  std::vector<TupleType> rows = {
      TupleType(111 + 1),
      TupleType(nullopt),
      TupleType(nullopt),
      TupleType(nullopt),
  };

  AssertBuilderEquals(scan_builder, rows);
}

TEST_F(TestSchemaUnification, SelectPartitionColumns) {
  // Selects partition (virtual) columns, it ensures:
  //
  // - virtual column are materialized
  // - Fragment yield the right number of rows even if no column is selected
  ASSERT_OK_AND_ASSIGN(auto scan_builder, dataset_->NewScan());
  ASSERT_OK(scan_builder->Project({"part_ds", "part_df"}));
  using TupleType = std::tuple<i32, i32>;
  std::vector<TupleType> rows = {
      TupleType(1, 1),
      TupleType(1, 2),
      TupleType(2, 1),
      TupleType(2, 2),
  };
  AssertBuilderEquals(scan_builder, rows);
}

TEST_F(TestSchemaUnification, SelectPartitionColumnsFilterPhysicalColumn) {
  // Selects re-ordered virtual columns with a filter on a physical columns
  ASSERT_OK_AND_ASSIGN(auto scan_builder, dataset_->NewScan());
  ASSERT_OK(scan_builder->Filter(equal(field_ref("phy_1"), literal(111))));

  ASSERT_OK(scan_builder->Project({"part_df", "part_ds"}));
  using TupleType = std::tuple<i32, i32>;
  std::vector<TupleType> rows = {
      TupleType(1, 1),
  };
  AssertBuilderEquals(scan_builder, rows);
}

TEST_F(TestSchemaUnification, SelectMixedColumnsAndFilter) {
  // Selects mix of physical/virtual with a different order and uses a filter on
  // a physical column not selected.
  ASSERT_OK_AND_ASSIGN(auto scan_builder, dataset_->NewScan());
  ASSERT_OK(scan_builder->Filter(greater_equal(field_ref("phy_2"), literal(212))));
  ASSERT_OK(scan_builder->Project({"part_df", "phy_3", "part_ds", "phy_1"}));

  using TupleType = std::tuple<i32, i32, i32, i32>;
  std::vector<TupleType> rows = {
      TupleType(2, 312, 1, nullopt),
      TupleType(2, nullopt, 2, nullopt),
  };
  AssertBuilderEquals(scan_builder, rows);
}

TEST(TestDictPartitionColumn, SelectPartitionColumnFilterPhysicalColumn) {
  auto partition_field = field("part", dictionary(int32(), utf8()));
  auto path = "/dataset/part=one/data.json";
  auto dictionary = ArrayFromJSON(utf8(), R"(["one"])");

  auto mock_fs = std::make_shared<fs::internal::MockFileSystem>(fs::kNoTime);
  ARROW_EXPECT_OK(mock_fs->CreateFile(path, R"([ {"phy_1": 111, "phy_2": 211} ])",
                                      /*recursive=*/true));

  auto physical_schema = SchemaFromNames({"phy_1", "phy_2"});
  auto format = std::make_shared<JSONRecordBatchFileFormat>(
      [=](const FileSource&) { return physical_schema; });

  FileSystemFactoryOptions options;
  options.partition_base_dir = "/dataset";
  options.partitioning = std::make_shared<HivePartitioning>(schema({partition_field}),
                                                            ArrayVector{dictionary});

  ASSERT_OK_AND_ASSIGN(auto factory,
                       FileSystemDatasetFactory::Make(mock_fs, {path}, format, options));

  ASSERT_OK_AND_ASSIGN(auto schema, factory->Inspect());

  ASSERT_OK_AND_ASSIGN(auto dataset, factory->Finish(schema));

  // Selects re-ordered virtual column with a filter on a physical column
  ASSERT_OK_AND_ASSIGN(auto scan_builder, dataset->NewScan());
  ASSERT_OK(scan_builder->Filter(equal(field_ref("phy_1"), literal(111))));
  ASSERT_OK(scan_builder->Project({"part"}));

  ASSERT_OK_AND_ASSIGN(auto scanner, scan_builder->Finish());
  ASSERT_OK_AND_ASSIGN(auto table, scanner->ToTable());
  AssertArraysEqual(*table->column(0)->chunk(0),
                    *ArrayFromJSON(partition_field->type(), R"(["one"])"));
}

}  // namespace dataset
}  // namespace arrow
