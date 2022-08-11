#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/compute/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/dataset/api.h>

#include <iostream>


// Generate some data for the rest of this example.
arrow::Result<std::shared_ptr<arrow::Table>> CreateTable() {
    auto schema =
            arrow::schema({arrow::field("a", arrow::int64()),
                           arrow::field("b", arrow::int64()),
                           arrow::field("c", arrow::int64())});
    std::shared_ptr<arrow::Array> array_a;
    std::shared_ptr<arrow::Array> array_b;
    std::shared_ptr<arrow::Array> array_c;
    arrow::NumericBuilder<arrow::Int64Type> builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_a));
    builder.Reset();
    ARROW_RETURN_NOT_OK(builder.AppendValues({9, 8, 7, 6, 5, 4, 3, 2, 1, 0}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_b));
    builder.Reset();
    ARROW_RETURN_NOT_OK(builder.AppendValues({1, 2, 1, 2, 1, 2, 1, 2, 1, 2}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_c));
    return arrow::Table::Make(schema, {array_a, array_b, array_c});
}

// Set up a dataset by writing two Parquet files.
arrow::Result<std::string> CreateExampleParquetDataset(
        const std::shared_ptr<arrow::fs::FileSystem>& filesystem,
        const std::string& root_path) {
    auto base_path = root_path + "/parquet_dataset";
    ARROW_RETURN_NOT_OK(filesystem->CreateDir(base_path));
    // Create an Arrow Table
    ARROW_ASSIGN_OR_RAISE(auto table, CreateTable());
    // Write it into two Parquet files
    ARROW_ASSIGN_OR_RAISE(auto output,
                          filesystem->OpenOutputStream(base_path + "/data1.parquet"));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
            *table->Slice(0, 5), arrow::default_memory_pool(), output, 2048));
    ARROW_ASSIGN_OR_RAISE(output,
                          filesystem->OpenOutputStream(base_path + "/data2.parquet"));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
            *table->Slice(5), arrow::default_memory_pool(), output, 2048));
    return base_path;
}

arrow::Status RunMain(int argc, char** argv) {

    //Prepare dataset for reading.
    ARROW_ASSIGN_OR_RAISE(auto src_table, CreateTable())
    std::shared_ptr<arrow::fs::FileSystem> setup_fs;
    char setup_path[256];
    getcwd(setup_path, 256);
    ARROW_ASSIGN_OR_RAISE(setup_fs, arrow::fs::FileSystemFromUriOrPath(setup_path));
    ARROW_ASSIGN_OR_RAISE(auto dset_path, CreateExampleParquetDataset(setup_fs,
                                                                      setup_path));

    //Begin Example
    std::shared_ptr<arrow::fs::FileSystem> fs;
    //This feels pretty bad, but I wasn't finding great solutions that're
    //system-generic -- could use some advice on how to set this up.
    char init_path[256];
    getcwd(init_path, 256);
    ARROW_ASSIGN_OR_RAISE(fs, arrow::fs::FileSystemFromUriOrPath(init_path));
    std::string current_path = init_path;

    arrow::fs::FileSelector selector;
    selector.base_dir = current_path + "/parquet_dataset";
    selector.recursive = true;
    arrow::dataset::FileSystemFactoryOptions options;
    // We'll use Hive-style partitioning. We'll let Arrow Datasets infer the partition
    // schema.
    options.partitioning = arrow::dataset::HivePartitioning::MakeFactory();
    auto read_format =
            std::make_shared<arrow::dataset::ParquetFileFormat>();
    auto factory = arrow::dataset::FileSystemDatasetFactory::Make(fs,
                                                                  selector,
                                                                  read_format,
                                                                  options)
            .ValueOrDie();
    auto read_dataset = factory->Finish().ValueOrDie();
    // Print out the fragments
    for (const auto& fragment : read_dataset->GetFragments().ValueOrDie()) {
        std::cout << "Found fragment: " << (*fragment)->ToString() << std::endl;
    }
    // Read the entire dataset as a Table
    auto read_scan_builder = read_dataset->NewScan().ValueOrDie();
    auto read_scanner = read_scan_builder->Finish().ValueOrDie();
    std::shared_ptr<arrow::Table> table = read_scanner->ToTable().ValueOrDie();
    std::cout << read_scanner->ToTable().ValueOrDie()->ToString();

    // Write it using Datasets
    auto write_dataset = std::make_shared<arrow::dataset::InMemoryDataset>(table);
    auto write_scanner_builder = write_dataset->NewScan().ValueOrDie();
    auto write_scanner = write_scanner_builder->Finish().ValueOrDie();

    // The partition schema determines which fields are part of the partitioning.
    auto partition_schema = arrow::schema({arrow::field("a", arrow::utf8())});
    // We'll use Hive-style partitioning, which creates directories with "key=value"
    // pairs.
    auto partitioning =
            std::make_shared<arrow::dataset::HivePartitioning>(partition_schema);
    // We'll write Parquet files.
    auto write_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    arrow::dataset::FileSystemDatasetWriteOptions write_options;
    write_options.file_write_options = write_format->DefaultWriteOptions();
    write_options.filesystem = fs;
    //Can only really be run once at the moment, because it'll explode upon noticing
    // the folder has anything.
    write_options.base_dir = current_path + "/write_dataset";
    write_options.partitioning = partitioning;
    write_options.basename_template = "part{i}.parquet";
    ARROW_RETURN_NOT_OK(arrow::dataset::FileSystemDataset::Write(write_options,
                                                                 write_scanner));

    return arrow::Status::OK();
}

int main(int argc, char** argv) {
    arrow::Status st = RunMain(argc, argv);
    if (!st.ok()) {
        std::cerr << st << std::endl;
        return 1;
    }
    return 0;
}
