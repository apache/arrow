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

// This API is EXPERIMENTAL.

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/compute/expression.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/filesystem/type_fwd.h"
#include "arrow/type_fwd.h"
#include "arrow/util/future.h"

namespace parquet {
class Statistics;
class FileMetaData;

class ReaderProperties;
class ArrowReaderProperties;

class WriterProperties;
class ArrowWriterProperties;

namespace arrow {
class FileReader;
class FileWriter;
struct SchemaManifest;
}  // namespace arrow
}  // namespace parquet

namespace arrow {
namespace dataset {

/// \addtogroup dataset-file-formats
///
/// @{

constexpr char kParquetTypeName[] = "parquet";

/// \brief A FileFormat implementation that reads from Parquet files
class ARROW_DS_EXPORT ParquetFileFormat : public FileFormat {
 public:
  ParquetFileFormat();

  /// Convenience constructor which copies properties from a parquet::ReaderProperties.
  /// memory_pool will be ignored.
  explicit ParquetFileFormat(const parquet::ReaderProperties& reader_properties);

  std::string type_name() const override { return kParquetTypeName; }

  bool Equals(const FileFormat& other) const override;

  struct ReaderOptions {
    /// \defgroup parquet-file-format-arrow-reader-properties properties which correspond
    /// to members of parquet::ArrowReaderProperties.
    ///
    /// We don't embed parquet::ReaderProperties directly because column names (rather
    /// than indices) are used to indicate dictionary columns, and other options are
    /// deferred to scan time.
    ///
    /// @{
    std::unordered_set<std::string> dict_columns;
    arrow::TimeUnit::type coerce_int96_timestamp_unit = arrow::TimeUnit::NANO;
    /// @}
  } reader_options;

  Result<bool> IsSupported(const FileSource& source) const override;

  /// \brief Return the schema of the file if possible.
  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override;

  Future<std::shared_ptr<InspectedFragment>> InspectFragment(
      const FileFragment& fragment, const FileSource& source,
      const FragmentScanOptions* format_options,
      compute::ExecContext* exec_context) const override;

  Future<std::shared_ptr<FragmentScanner>> BeginScan(
      const FileSource& source, const FragmentScanRequest& request,
      InspectedFragment* inspected_fragment, const FragmentScanOptions* format_options,
      compute::ExecContext* exec_context) const override;

  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>& options,
      const std::shared_ptr<FileFragment>& file) const override;

  Future<std::optional<int64_t>> CountRows(
      const std::shared_ptr<FileFragment>& file, compute::Expression predicate,
      const std::shared_ptr<ScanOptions>& options) override;

  using FileFormat::MakeFragment;

  /// \brief Create a Fragment targeting all RowGroups.
  Result<std::shared_ptr<FileFragment>> MakeFragment(
      FileSource source, compute::Expression partition_expression,
      std::shared_ptr<Schema> physical_schema) override;

  /// \brief Create a Fragment, restricted to the specified row groups.
  Result<std::shared_ptr<ParquetFileFragment>> MakeFragment(
      FileSource source, compute::Expression partition_expression,
      std::shared_ptr<Schema> physical_schema, std::vector<int> row_groups);

  /// \brief Return a FileReader on the given source.
  Result<std::shared_ptr<parquet::arrow::FileReader>> GetReader(
      const FileSource& source, const std::shared_ptr<ScanOptions>& options) const;

  Result<std::shared_ptr<parquet::arrow::FileReader>> GetReader(
      const FileSource& source, const std::shared_ptr<ScanOptions>& options,
      const std::shared_ptr<parquet::FileMetaData>& metadata) const;

  Future<std::shared_ptr<parquet::arrow::FileReader>> GetReaderAsync(
      const FileSource& source, const std::shared_ptr<ScanOptions>& options) const;

  Future<std::shared_ptr<parquet::arrow::FileReader>> GetReaderAsync(
      const FileSource& source, const std::shared_ptr<ScanOptions>& options,
      const std::shared_ptr<parquet::FileMetaData>& metadata) const;

  Result<std::shared_ptr<FileWriter>> MakeWriter(
      std::shared_ptr<io::OutputStream> destination, std::shared_ptr<Schema> schema,
      std::shared_ptr<FileWriteOptions> options,
      fs::FileLocator destination_locator) const override;

  std::shared_ptr<FileWriteOptions> DefaultWriteOptions() override;
};

/// \brief A FileFragment with parquet logic.
///
/// ParquetFileFragment provides a lazy (with respect to IO) interface to
/// scan parquet files. Any heavy IO calls are deferred to the Scan() method.
///
/// The caller can provide an optional list of selected RowGroups to limit the
/// number of scanned RowGroups, or to partition the scans across multiple
/// threads.
///
/// Metadata can be explicitly provided, enabling pushdown predicate benefits without
/// the potentially heavy IO of loading Metadata from the file system. This can induce
/// significant performance boost when scanning high latency file systems.
class ARROW_DS_EXPORT ParquetFileFragment : public FileFragment {
 public:
  Result<FragmentVector> SplitByRowGroup(compute::Expression predicate);

  /// \brief Return the RowGroups selected by this fragment.
  const std::vector<int>& row_groups() const {
    if (row_groups_) return *row_groups_;
    static std::vector<int> empty;
    return empty;
  }

  /// \brief Return the FileMetaData associated with this fragment.
  const std::shared_ptr<parquet::FileMetaData>& metadata() const { return metadata_; }

  /// \brief Ensure this fragment's FileMetaData is in memory.
  Status EnsureCompleteMetadata(parquet::arrow::FileReader* reader = NULLPTR);

  /// \brief Return fragment which selects a filtered subset of this fragment's RowGroups.
  Result<std::shared_ptr<Fragment>> Subset(compute::Expression predicate);
  Result<std::shared_ptr<Fragment>> Subset(std::vector<int> row_group_ids);

  static std::optional<compute::Expression> EvaluateStatisticsAsExpression(
      const compute::Expression& field_expr, const std::shared_ptr<DataType>& field_type,
      const parquet::Statistics& statistics);

 private:
  ParquetFileFragment(FileSource source, std::shared_ptr<FileFormat> format,
                      compute::Expression partition_expression,
                      std::shared_ptr<Schema> physical_schema,
                      std::optional<std::vector<int>> row_groups);

  Status SetMetadata(std::shared_ptr<parquet::FileMetaData> metadata,
                     std::shared_ptr<parquet::arrow::SchemaManifest> manifest);

  // Overridden to opportunistically set metadata since a reader must be opened anyway.
  Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override {
    ARROW_RETURN_NOT_OK(EnsureCompleteMetadata());
    return physical_schema_;
  }

  /// Return a filtered subset of row group indices.
  Result<std::vector<int>> FilterRowGroups(compute::Expression predicate);
  /// Simplify the predicate against the statistics of each row group.
  Result<std::vector<compute::Expression>> TestRowGroups(compute::Expression predicate);
  /// Try to count rows matching the predicate using metadata. Expects
  /// metadata to be present, and expects the predicate to have been
  /// simplified against the partition expression already.
  Result<std::optional<int64_t>> TryCountRows(compute::Expression predicate);

  ParquetFileFormat& parquet_format_;

  /// Indices of row groups selected by this fragment,
  /// or std::nullopt if all row groups are selected.
  std::optional<std::vector<int>> row_groups_;

  std::vector<compute::Expression> statistics_expressions_;
  std::vector<bool> statistics_expressions_complete_;
  std::shared_ptr<parquet::FileMetaData> metadata_;
  std::shared_ptr<parquet::arrow::SchemaManifest> manifest_;

  friend class ParquetFileFormat;
  friend class ParquetDatasetFactory;
};

enum class ParquetScanStrategy { kLeastMemory, kMaxSpeed, kCustom };

/// \brief Per-scan options for Parquet fragments
class ARROW_DS_EXPORT ParquetFragmentScanOptions : public FragmentScanOptions {
 public:
  ParquetFragmentScanOptions();
  std::string type_name() const override { return kParquetTypeName; }

  /// There are several fine-grained parquet reader properties that are difficult
  /// to understand and configure correctly.  This often leads to poor performance
  /// or poor RAM utilization.
  ///
  /// As a result, we have created common configurations of these properties.  This
  /// option allows you to select one of these configurations.
  ///
  /// kMaxSpeed - This configuration will enable pre-buffering and caching.  Row groups
  ///             will be read in their entirety, batch size is 32Ki.  Stream
  ///             buffering is disabled.
  ///
  /// kLeastMemory - This configuration will disable pre-buffering.  Stream buffering
  ///             will be enabled.  Row groups will be read 32Ki rows at a time.
  ///
  /// kCustom - Pre-buffering, stream buffering, and batch size settings will be
  ///           configured according to reader_properties and arrow_reader_properties.
  ParquetScanStrategy scan_strategy = ParquetScanStrategy::kLeastMemory;

  /// Reader properties.
  ///
  /// Not all properties are respected:
  ///
  /// * memory_pool comes from ScanOptions.
  /// * buffered_stream will be ignored if scan_strategy != kCustom
  /// * buffer_size will be ignored if scan_strategy != kCustom
  std::shared_ptr<parquet::ReaderProperties> reader_properties;
  /// Arrow reader properties.
  ///
  /// Not all properties are respected:
  ///
  /// * batch_size will come from:
  ///   * ScanOptions if using ScanOptions
  ///   * ignored if using ScanV2Options and strategy != kCustom (we always use 32Ki)
  ///   * this arrow_reader_properties if using ScanV2Options and strategy == kCustom
  /// * dictionary columns come from ParquetFileFormat::ReaderOptions::dict_columns
  /// * pre_buffer will be ignored if using ScanV2Options and scan_strategy != kCustom
  /// * cache_options will be ignored if using ScanV2Options and scan_strategy != kCustom
  /// * use_threads will come from:
  ///   * ScanOptions if using ScanOptions
  ///   * true if using ScanV2Options and the query executor has capacity > 1
  ///   * false if using ScanV2Options and the query executor has capacity == 1
  std::shared_ptr<parquet::ArrowReaderProperties> arrow_reader_properties;

  /// \brief Are parquet files allowed to contain extremely large values
  ///
  /// The scan will read the parquet file in batches.  This is controlled by
  /// arrow_reader_properties.  It is generally safe to assume that one batch
  /// of rows will fit in a RecordBatch.  However, if the rows contain string data
  /// and the batch size is large, or if there are extremely large strings present
  /// in the data, then it may be possible that a single batch of data has more than
  /// 2GiB of data in a single column, which is not allowed, and we will need to split
  /// the batch.
  ///
  /// For example, if there is a string column, and the batch size is set to its
  /// default value of 32Ki then a batch will be a jumbo if the strings are, on average,
  /// 64KiB each.  You can avoid this, if needed,  by using a custom scan strategy and
  /// setting a smaller batch size.
  ///
  /// If this setting is true it will have a negative impact on performance.  We won't
  /// know in advance how many batches are contained in the file and this makes it
  /// difficult to both scan files in parallel and sequence batches.
  bool allow_jumbo_values = false;
};

class ARROW_DS_EXPORT ParquetFileWriteOptions : public FileWriteOptions {
 public:
  /// \brief Parquet writer properties.
  std::shared_ptr<parquet::WriterProperties> writer_properties;

  /// \brief Parquet Arrow writer properties.
  std::shared_ptr<parquet::ArrowWriterProperties> arrow_writer_properties;

 protected:
  explicit ParquetFileWriteOptions(std::shared_ptr<FileFormat> format)
      : FileWriteOptions(std::move(format)) {}

  friend class ParquetFileFormat;
};

class ARROW_DS_EXPORT ParquetFileWriter : public FileWriter {
 public:
  const std::shared_ptr<parquet::arrow::FileWriter>& parquet_writer() const {
    return parquet_writer_;
  }

  Status Write(const std::shared_ptr<RecordBatch>& batch) override;

 private:
  ParquetFileWriter(std::shared_ptr<io::OutputStream> destination,
                    std::shared_ptr<parquet::arrow::FileWriter> writer,
                    std::shared_ptr<ParquetFileWriteOptions> options,
                    fs::FileLocator destination_locator);

  Future<> FinishInternal() override;

  std::shared_ptr<parquet::arrow::FileWriter> parquet_writer_;

  friend class ParquetFileFormat;
};

/// \brief Options for making a FileSystemDataset from a Parquet _metadata file.
struct ParquetFactoryOptions {
  /// Either an explicit Partitioning or a PartitioningFactory to discover one.
  ///
  /// If a factory is provided, it will be used to infer a schema for partition fields
  /// based on file and directory paths then construct a Partitioning. The default
  /// is a Partitioning which will yield no partition information.
  ///
  /// The (explicit or discovered) partitioning will be applied to discovered files
  /// and the resulting partition information embedded in the Dataset.
  PartitioningOrFactory partitioning{Partitioning::Default()};

  /// For the purposes of applying the partitioning, paths will be stripped
  /// of the partition_base_dir. Files not matching the partition_base_dir
  /// prefix will be skipped for partition discovery. The ignored files will still
  /// be part of the Dataset, but will not have partition information.
  ///
  /// Example:
  /// partition_base_dir = "/dataset";
  ///
  /// - "/dataset/US/sales.csv" -> "US/sales.csv" will be given to the partitioning
  ///
  /// - "/home/john/late_sales.csv" -> Will be ignored for partition discovery.
  ///
  /// This is useful for partitioning which parses directory when ordering
  /// is important, e.g. DirectoryPartitioning.
  std::string partition_base_dir;

  /// Assert that all ColumnChunk paths are consistent. The parquet spec allows for
  /// ColumnChunk data to be stored in multiple files, but ParquetDatasetFactory
  /// supports only a single file with all ColumnChunk data. If this flag is set
  /// construction of a ParquetDatasetFactory will raise an error if ColumnChunk
  /// data is not resident in a single file.
  bool validate_column_chunk_paths = false;
};

/// \brief Create FileSystemDataset from custom `_metadata` cache file.
///
/// Dask and other systems will generate a cache metadata file by concatenating
/// the RowGroupMetaData of multiple parquet files into a single parquet file
/// that only contains metadata and no ColumnChunk data.
///
/// ParquetDatasetFactory creates a FileSystemDataset composed of
/// ParquetFileFragment where each fragment is pre-populated with the exact
/// number of row groups and statistics for each columns.
class ARROW_DS_EXPORT ParquetDatasetFactory : public DatasetFactory {
 public:
  /// \brief Create a ParquetDatasetFactory from a metadata path.
  ///
  /// The `metadata_path` will be read from `filesystem`. Each RowGroup
  /// contained in the metadata file will be relative to `dirname(metadata_path)`.
  ///
  /// \param[in] metadata_path path of the metadata parquet file
  /// \param[in] filesystem from which to open/read the path
  /// \param[in] format to read the file with.
  /// \param[in] options see ParquetFactoryOptions
  static Result<std::shared_ptr<DatasetFactory>> Make(
      const std::string& metadata_path, std::shared_ptr<fs::FileSystem> filesystem,
      std::shared_ptr<ParquetFileFormat> format, ParquetFactoryOptions options);

  /// \brief Create a ParquetDatasetFactory from a metadata source.
  ///
  /// Similar to the previous Make definition, but the metadata can be a Buffer
  /// and the base_path is explicited instead of inferred from the metadata
  /// path.
  ///
  /// \param[in] metadata source to open the metadata parquet file from
  /// \param[in] base_path used as the prefix of every parquet files referenced
  /// \param[in] filesystem from which to read the files referenced.
  /// \param[in] format to read the file with.
  /// \param[in] options see ParquetFactoryOptions
  static Result<std::shared_ptr<DatasetFactory>> Make(
      const FileSource& metadata, const std::string& base_path,
      std::shared_ptr<fs::FileSystem> filesystem,
      std::shared_ptr<ParquetFileFormat> format, ParquetFactoryOptions options);

  Result<std::vector<std::shared_ptr<Schema>>> InspectSchemas(
      InspectOptions options) override;

  Result<std::shared_ptr<Dataset>> Finish(FinishOptions options) override;

 protected:
  ParquetDatasetFactory(
      std::shared_ptr<fs::FileSystem> filesystem,
      std::shared_ptr<ParquetFileFormat> format,
      std::shared_ptr<parquet::FileMetaData> metadata,
      std::shared_ptr<parquet::arrow::SchemaManifest> manifest,
      std::shared_ptr<Schema> physical_schema, std::string base_path,
      ParquetFactoryOptions options,
      std::vector<std::pair<std::string, std::vector<int>>> paths_with_row_group_ids)
      : filesystem_(std::move(filesystem)),
        format_(std::move(format)),
        metadata_(std::move(metadata)),
        manifest_(std::move(manifest)),
        physical_schema_(std::move(physical_schema)),
        base_path_(std::move(base_path)),
        options_(std::move(options)),
        paths_with_row_group_ids_(std::move(paths_with_row_group_ids)) {}

  std::shared_ptr<fs::FileSystem> filesystem_;
  std::shared_ptr<ParquetFileFormat> format_;
  std::shared_ptr<parquet::FileMetaData> metadata_;
  std::shared_ptr<parquet::arrow::SchemaManifest> manifest_;
  std::shared_ptr<Schema> physical_schema_;
  std::string base_path_;
  ParquetFactoryOptions options_;
  std::vector<std::pair<std::string, std::vector<int>>> paths_with_row_group_ids_;

 private:
  Result<std::vector<std::shared_ptr<FileFragment>>> CollectParquetFragments(
      const Partitioning& partitioning);

  Result<std::shared_ptr<Schema>> PartitionSchema();
};

/// @}

}  // namespace dataset
}  // namespace arrow
