// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// This example showcases various ways to work with Datasets. It's
// intended to be paired with the documentation.

#include <arrow/api.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_ipc.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/iterator.h>
#include <parquet/arrow/writer.h>

#include <iostream>
#include <vector>

namespace ds = arrow::dataset;
namespace fs = arrow::fs;
namespace cp = arrow::compute;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

// (Doc section: Reading Datasets)
// Generate some data for the rest of this example.
std::shared_ptr<arrow::Table> CreateTable() {
  auto schema =
      arrow::schema({arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64()),
                     arrow::field("c", arrow::int64())});
  std::shared_ptr<arrow::Array> array_a;
  std::shared_ptr<arrow::Array> array_b;
  std::shared_ptr<arrow::Array> array_c;
  arrow::NumericBuilder<arrow::Int64Type> builder;
  ABORT_ON_FAILURE(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
  ABORT_ON_FAILURE(builder.Finish(&array_a));
  builder.Reset();
  ABORT_ON_FAILURE(builder.AppendValues({9, 8, 7, 6, 5, 4, 3, 2, 1, 0}));
  ABORT_ON_FAILURE(builder.Finish(&array_b));
  builder.Reset();
  ABORT_ON_FAILURE(builder.AppendValues({1, 2, 1, 2, 1, 2, 1, 2, 1, 2}));
  ABORT_ON_FAILURE(builder.Finish(&array_c));
  return arrow::Table::Make(schema, {array_a, array_b, array_c});
}

// Set up a dataset by writing two Parquet files.
std::string CreateExampleParquetDataset(const std::shared_ptr<fs::FileSystem>& filesystem,
                                        const std::string& root_path) {
  auto base_path = root_path + "/parquet_dataset";
  ABORT_ON_FAILURE(filesystem->CreateDir(base_path));
  // Create an Arrow Table
  auto table = CreateTable();
  // Write it into two Parquet files
  auto output = filesystem->OpenOutputStream(base_path + "/data1.parquet").ValueOrDie();
  ABORT_ON_FAILURE(parquet::arrow::WriteTable(
      *table->Slice(0, 5), arrow::default_memory_pool(), output, /*chunk_size=*/2048));
  output = filesystem->OpenOutputStream(base_path + "/data2.parquet").ValueOrDie();
  ABORT_ON_FAILURE(parquet::arrow::WriteTable(
      *table->Slice(5), arrow::default_memory_pool(), output, /*chunk_size=*/2048));
  return base_path;
}
// (Doc section: Reading Datasets)

// (Doc section: Reading different file formats)
// Set up a dataset by writing two Feather files.
std::string CreateExampleFeatherDataset(const std::shared_ptr<fs::FileSystem>& filesystem,
                                        const std::string& root_path) {
  auto base_path = root_path + "/feather_dataset";
  ABORT_ON_FAILURE(filesystem->CreateDir(base_path));
  // Create an Arrow Table
  auto table = CreateTable();
  // Write it into two Feather files
  auto output = filesystem->OpenOutputStream(base_path + "/data1.feather").ValueOrDie();
  auto writer = arrow::ipc::MakeFileWriter(output.get(), table->schema()).ValueOrDie();
  ABORT_ON_FAILURE(writer->WriteTable(*table->Slice(0, 5)));
  ABORT_ON_FAILURE(writer->Close());
  output = filesystem->OpenOutputStream(base_path + "/data2.feather").ValueOrDie();
  writer = arrow::ipc::MakeFileWriter(output.get(), table->schema()).ValueOrDie();
  ABORT_ON_FAILURE(writer->WriteTable(*table->Slice(5)));
  ABORT_ON_FAILURE(writer->Close());
  return base_path;
}
// (Doc section: Reading different file formats)

// (Doc section: Reading and writing partitioned data)
// Set up a dataset by writing files with partitioning
std::string CreateExampleParquetHivePartitionedDataset(
    const std::shared_ptr<fs::FileSystem>& filesystem, const std::string& root_path) {
  auto base_path = root_path + "/parquet_dataset";
  ABORT_ON_FAILURE(filesystem->CreateDir(base_path));
  // Create an Arrow Table
  auto schema = arrow::schema(
      {arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64()),
       arrow::field("c", arrow::int64()), arrow::field("part", arrow::utf8())});
  std::vector<std::shared_ptr<arrow::Array>> arrays(4);
  arrow::NumericBuilder<arrow::Int64Type> builder;
  ABORT_ON_FAILURE(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
  ABORT_ON_FAILURE(builder.Finish(&arrays[0]));
  builder.Reset();
  ABORT_ON_FAILURE(builder.AppendValues({9, 8, 7, 6, 5, 4, 3, 2, 1, 0}));
  ABORT_ON_FAILURE(builder.Finish(&arrays[1]));
  builder.Reset();
  ABORT_ON_FAILURE(builder.AppendValues({1, 2, 1, 2, 1, 2, 1, 2, 1, 2}));
  ABORT_ON_FAILURE(builder.Finish(&arrays[2]));
  arrow::StringBuilder string_builder;
  ABORT_ON_FAILURE(
      string_builder.AppendValues({"a", "a", "a", "a", "a", "b", "b", "b", "b", "b"}));
  ABORT_ON_FAILURE(string_builder.Finish(&arrays[3]));
  auto table = arrow::Table::Make(schema, arrays);
  // Write it using Datasets
  auto dataset = std::make_shared<ds::InMemoryDataset>(table);
  auto scanner_builder = dataset->NewScan().ValueOrDie();
  auto scanner = scanner_builder->Finish().ValueOrDie();

  // The partition schema determines which fields are part of the partitioning.
  auto partition_schema = arrow::schema({arrow::field("part", arrow::utf8())});
  // We'll use Hive-style partitioning, which creates directories with "key=value" pairs.
  auto partitioning = std::make_shared<ds::HivePartitioning>(partition_schema);
  // We'll write Parquet files.
  auto format = std::make_shared<ds::ParquetFileFormat>();
  ds::FileSystemDatasetWriteOptions write_options;
  write_options.file_write_options = format->DefaultWriteOptions();
  write_options.filesystem = filesystem;
  write_options.base_dir = base_path;
  write_options.partitioning = partitioning;
  write_options.basename_template = "part{i}.parquet";
  ABORT_ON_FAILURE(ds::FileSystemDataset::Write(write_options, scanner));
  return base_path;
}
// (Doc section: Reading and writing partitioned data)

// (Doc section: Dataset discovery)
// Read the whole dataset with the given format, without partitioning.
std::shared_ptr<arrow::Table> ScanWholeDataset(
    const std::shared_ptr<fs::FileSystem>& filesystem,
    const std::shared_ptr<ds::FileFormat>& format, const std::string& base_dir) {
  // Create a dataset by scanning the filesystem for files
  fs::FileSelector selector;
  selector.base_dir = base_dir;
  auto factory = ds::FileSystemDatasetFactory::Make(filesystem, selector, format,
                                                    ds::FileSystemFactoryOptions())
                     .ValueOrDie();
  auto dataset = factory->Finish().ValueOrDie();
  // Print out the fragments
  for (const auto& fragment : dataset->GetFragments().ValueOrDie()) {
    std::cout << "Found fragment: " << (*fragment)->ToString() << std::endl;
  }
  // Read the entire dataset as a Table
  auto scan_builder = dataset->NewScan().ValueOrDie();
  auto scanner = scan_builder->Finish().ValueOrDie();
  return scanner->ToTable().ValueOrDie();
}
// (Doc section: Dataset discovery)

// (Doc section: Filtering data)
// Read a dataset, but select only column "b" and only rows where b < 4.
//
// This is useful when you only want a few columns from a dataset. Where possible,
// Datasets will push down the column selection such that less work is done.
std::shared_ptr<arrow::Table> FilterAndSelectDataset(
    const std::shared_ptr<fs::FileSystem>& filesystem,
    const std::shared_ptr<ds::FileFormat>& format, const std::string& base_dir) {
  fs::FileSelector selector;
  selector.base_dir = base_dir;
  auto factory = ds::FileSystemDatasetFactory::Make(filesystem, selector, format,
                                                    ds::FileSystemFactoryOptions())
                     .ValueOrDie();
  auto dataset = factory->Finish().ValueOrDie();
  // Read specified columns with a row filter
  auto scan_builder = dataset->NewScan().ValueOrDie();
  ABORT_ON_FAILURE(scan_builder->Project({"b"}));
  ABORT_ON_FAILURE(scan_builder->Filter(cp::less(cp::field_ref("b"), cp::literal(4))));
  auto scanner = scan_builder->Finish().ValueOrDie();
  return scanner->ToTable().ValueOrDie();
}
// (Doc section: Filtering data)

// (Doc section: Projecting columns)
// Read a dataset, but with column projection.
//
// This is useful to derive new columns from existing data. For example, here we
// demonstrate casting a column to a different type, and turning a numeric column into a
// boolean column based on a predicate. You could also rename columns or perform
// computations involving multiple columns.
std::shared_ptr<arrow::Table> ProjectDataset(
    const std::shared_ptr<fs::FileSystem>& filesystem,
    const std::shared_ptr<ds::FileFormat>& format, const std::string& base_dir) {
  fs::FileSelector selector;
  selector.base_dir = base_dir;
  auto factory = ds::FileSystemDatasetFactory::Make(filesystem, selector, format,
                                                    ds::FileSystemFactoryOptions())
                     .ValueOrDie();
  auto dataset = factory->Finish().ValueOrDie();
  // Read specified columns with a row filter
  auto scan_builder = dataset->NewScan().ValueOrDie();
  ABORT_ON_FAILURE(scan_builder->Project(
      {
          // Leave column "a" as-is.
          cp::field_ref("a"),
          // Cast column "b" to float32.
          cp::call("cast", {cp::field_ref("b")},
                   arrow::compute::CastOptions::Safe(arrow::float32())),
          // Derive a boolean column from "c".
          cp::equal(cp::field_ref("c"), cp::literal(1)),
      },
      {"a_renamed", "b_as_float32", "c_1"}));
  auto scanner = scan_builder->Finish().ValueOrDie();
  return scanner->ToTable().ValueOrDie();
}
// (Doc section: Projecting columns)

// (Doc section: Projecting columns #2)
// Read a dataset, but with column projection.
//
// This time, we read all original columns plus one derived column. This simply combines
// the previous two examples: selecting a subset of columns by name, and deriving new
// columns with an expression.
std::shared_ptr<arrow::Table> SelectAndProjectDataset(
    const std::shared_ptr<fs::FileSystem>& filesystem,
    const std::shared_ptr<ds::FileFormat>& format, const std::string& base_dir) {
  fs::FileSelector selector;
  selector.base_dir = base_dir;
  auto factory = ds::FileSystemDatasetFactory::Make(filesystem, selector, format,
                                                    ds::FileSystemFactoryOptions())
                     .ValueOrDie();
  auto dataset = factory->Finish().ValueOrDie();
  // Read specified columns with a row filter
  auto scan_builder = dataset->NewScan().ValueOrDie();
  std::vector<std::string> names;
  std::vector<cp::Expression> exprs;
  // Read all the original columns.
  for (const auto& field : dataset->schema()->fields()) {
    names.push_back(field->name());
    exprs.push_back(cp::field_ref(field->name()));
  }
  // Also derive a new column.
  names.emplace_back("b_large");
  exprs.push_back(cp::greater(cp::field_ref("b"), cp::literal(1)));
  ABORT_ON_FAILURE(scan_builder->Project(exprs, names));
  auto scanner = scan_builder->Finish().ValueOrDie();
  return scanner->ToTable().ValueOrDie();
}
// (Doc section: Projecting columns #2)

// (Doc section: Reading and writing partitioned data #2)
// Read an entire dataset, but with partitioning information.
std::shared_ptr<arrow::Table> ScanPartitionedDataset(
    const std::shared_ptr<fs::FileSystem>& filesystem,
    const std::shared_ptr<ds::FileFormat>& format, const std::string& base_dir) {
  fs::FileSelector selector;
  selector.base_dir = base_dir;
  selector.recursive = true;  // Make sure to search subdirectories
  ds::FileSystemFactoryOptions options;
  // We'll use Hive-style partitioning. We'll let Arrow Datasets infer the partition
  // schema.
  options.partitioning = ds::HivePartitioning::MakeFactory();
  auto factory = ds::FileSystemDatasetFactory::Make(filesystem, selector, format, options)
                     .ValueOrDie();
  auto dataset = factory->Finish().ValueOrDie();
  // Print out the fragments
  for (const auto& fragment : dataset->GetFragments().ValueOrDie()) {
    std::cout << "Found fragment: " << (*fragment)->ToString() << std::endl;
    std::cout << "Partition expression: "
              << (*fragment)->partition_expression().ToString() << std::endl;
  }
  auto scan_builder = dataset->NewScan().ValueOrDie();
  auto scanner = scan_builder->Finish().ValueOrDie();
  return scanner->ToTable().ValueOrDie();
}
// (Doc section: Reading and writing partitioned data #2)

// (Doc section: Reading and writing partitioned data #3)
// Read an entire dataset, but with partitioning information. Also, filter the dataset on
// the partition values.
std::shared_ptr<arrow::Table> FilterPartitionedDataset(
    const std::shared_ptr<fs::FileSystem>& filesystem,
    const std::shared_ptr<ds::FileFormat>& format, const std::string& base_dir) {
  fs::FileSelector selector;
  selector.base_dir = base_dir;
  selector.recursive = true;
  ds::FileSystemFactoryOptions options;
  options.partitioning = ds::HivePartitioning::MakeFactory();
  auto factory = ds::FileSystemDatasetFactory::Make(filesystem, selector, format, options)
                     .ValueOrDie();
  auto dataset = factory->Finish().ValueOrDie();
  auto scan_builder = dataset->NewScan().ValueOrDie();
  // Filter based on the partition values. This will mean that we won't even read the
  // files whose partition expressions don't match the filter.
  ABORT_ON_FAILURE(
      scan_builder->Filter(cp::equal(cp::field_ref("part"), cp::literal("b"))));
  auto scanner = scan_builder->Finish().ValueOrDie();
  return scanner->ToTable().ValueOrDie();
}
// (Doc section: Reading and writing partitioned data #3)

int main(int argc, char** argv) {
  if (argc < 3) {
    // Fake success for CI purposes.
    return EXIT_SUCCESS;
  }

  std::string uri = argv[1];
  std::string format_name = argv[2];
  std::string mode = argc > 3 ? argv[3] : "no_filter";
  std::string root_path;
  auto fs = fs::FileSystemFromUri(uri, &root_path).ValueOrDie();

  std::string base_path;
  std::shared_ptr<ds::FileFormat> format;
  if (format_name == "feather") {
    format = std::make_shared<ds::IpcFileFormat>();
    base_path = CreateExampleFeatherDataset(fs, root_path);
  } else if (format_name == "parquet") {
    format = std::make_shared<ds::ParquetFileFormat>();
    base_path = CreateExampleParquetDataset(fs, root_path);
  } else if (format_name == "parquet_hive") {
    format = std::make_shared<ds::ParquetFileFormat>();
    base_path = CreateExampleParquetHivePartitionedDataset(fs, root_path);
  } else {
    std::cerr << "Unknown format: " << format_name << std::endl;
    std::cerr << "Supported formats: feather, parquet, parquet_hive" << std::endl;
    return EXIT_FAILURE;
  }

  std::shared_ptr<arrow::Table> table;
  if (mode == "no_filter") {
    table = ScanWholeDataset(fs, format, base_path);
  } else if (mode == "filter") {
    table = FilterAndSelectDataset(fs, format, base_path);
  } else if (mode == "project") {
    table = ProjectDataset(fs, format, base_path);
  } else if (mode == "select_project") {
    table = SelectAndProjectDataset(fs, format, base_path);
  } else if (mode == "partitioned") {
    table = ScanPartitionedDataset(fs, format, base_path);
  } else if (mode == "filter_partitioned") {
    table = FilterPartitionedDataset(fs, format, base_path);
  } else {
    std::cerr << "Unknown mode: " << mode << std::endl;
    std::cerr
        << "Supported modes: no_filter, filter, project, select_project, partitioned"
        << std::endl;
    return EXIT_FAILURE;
  }
  std::cout << "Read " << table->num_rows() << " rows" << std::endl;
  std::cout << table->ToString() << std::endl;
  return EXIT_SUCCESS;
}
