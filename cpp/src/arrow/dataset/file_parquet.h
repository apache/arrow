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
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/dataset/discovery.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"

namespace parquet {
class ParquetFileReader;
class RowGroupMetaData;
class FileMetaData;
class FileDecryptionProperties;
class ReaderProperties;
class ArrowReaderProperties;
namespace arrow {
class FileReader;
};  // namespace arrow
}  // namespace parquet

namespace arrow {
namespace dataset {

class RowGroupInfo;

/// \brief A FileFormat implementation that reads from Parquet files
class ARROW_DS_EXPORT ParquetFileFormat : public FileFormat {
 public:
  ParquetFileFormat() = default;

  /// Convenience constructor which copies properties from a parquet::ReaderProperties.
  /// memory_pool will be ignored.
  explicit ParquetFileFormat(const parquet::ReaderProperties& reader_properties);

  std::string type_name() const override { return "parquet"; }

  bool splittable() const override { return true; }

  // Note: the default values are exposed in the python bindings and documented
  //       in the docstrings, if any of the default values gets changed please
  //       update there as well.
  struct ReaderOptions {
    /// \defgroup parquet-file-format-reader-properties properties which correspond to
    /// members of parquet::ReaderProperties.
    ///
    /// We don't embed parquet::ReaderProperties directly because we get memory_pool from
    /// ScanContext at scan time and provide differing defaults.
    ///
    /// @{
    bool use_buffered_stream = false;
    int64_t buffer_size = 1 << 13;
    std::shared_ptr<parquet::FileDecryptionProperties> file_decryption_properties;
    /// @}

    /// \defgroup parquet-file-format-arrow-reader-properties properties which correspond
    /// to members of parquet::ArrowReaderProperties.
    ///
    /// We don't embed parquet::ReaderProperties directly because we get batch_size from
    /// ScanOptions at scan time, and we will never pass use_threads == true (since we
    /// defer parallelization of the scan). Additionally column names (rather than
    /// indices) are used to indicate dictionary columns.
    ///
    /// @{
    std::unordered_set<std::string> dict_columns;
    /// @}
  } reader_options;

  Result<bool> IsSupported(const FileSource& source) const override;

  /// \brief Return the schema of the file if possible.
  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override;

  /// \brief Open a file for scanning
  Result<ScanTaskIterator> ScanFile(std::shared_ptr<ScanOptions> options,
                                    std::shared_ptr<ScanContext> context,
                                    FileFragment* file) const override;

  using FileFormat::MakeFragment;

  /// \brief Create a Fragment, restricted to the specified row groups.
  Result<std::shared_ptr<FileFragment>> MakeFragment(
      FileSource source, std::shared_ptr<Expression> partition_expression,
      std::vector<RowGroupInfo> row_groups,
      std::shared_ptr<Schema> physical_schema = NULLPTR);

  /// \brief Create a Fragment targeting all RowGroups.
  Result<std::shared_ptr<FileFragment>> MakeFragment(
      FileSource source, std::shared_ptr<Expression> partition_expression,
      std::shared_ptr<Schema> physical_schema) override;

  /// \brief Return a FileReader on the given source.
  Result<std::unique_ptr<parquet::arrow::FileReader>> GetReader(
      const FileSource& source, ScanOptions* = NULLPTR, ScanContext* = NULLPTR) const;
};

/// \brief Represents a parquet's RowGroup with extra information.
class ARROW_DS_EXPORT RowGroupInfo : public util::EqualityComparable<RowGroupInfo> {
 public:
  RowGroupInfo() : RowGroupInfo(-1) {}

  /// \brief Construct a RowGroup from an identifier.
  explicit RowGroupInfo(int id) : RowGroupInfo(id, -1, -1, NULLPTR) {}

  /// \brief Construct a RowGroup from an identifier with statistics.
  RowGroupInfo(int id, int64_t num_rows, int64_t total_byte_size,
               std::shared_ptr<StructScalar> statistics)
      : id_(id),
        num_rows_(num_rows),
        total_byte_size_(total_byte_size),
        statistics_(std::move(statistics)) {
    SetStatisticsExpression();
  }

  /// \brief Transform a vector of identifiers into a vector of RowGroupInfos
  static std::vector<RowGroupInfo> FromIdentifiers(const std::vector<int> ids);
  static std::vector<RowGroupInfo> FromCount(int count);

  /// \brief Return the RowGroup's identifier (index in the file).
  int id() const { return id_; }

  /// \brief Return the RowGroup's number of rows.
  ///
  /// If statistics are not provided, return -1.
  int64_t num_rows() const { return num_rows_; }

  /// \brief Return the RowGroup's total size in bytes.
  ///
  /// If statistics are not provided, return -1.
  int64_t total_byte_size() const { return total_byte_size_; }

  /// \brief Return the RowGroup's statistics as a StructScalar with a field for
  /// each column with statistics.
  /// Each field will also be a StructScalar with "min" and "max" fields.
  const std::shared_ptr<StructScalar>& statistics() const { return statistics_; }

  /// \brief Indicate if statistics are set.
  bool HasStatistics() const { return statistics_ != NULLPTR; }

  /// \brief Indicate if the RowGroup's statistics satisfy the predicate.
  ///
  /// This will return true if the RowGroup was not initialized with statistics
  /// (rather than silently reading metadata for a complete check).
  bool Satisfy(const Expression& predicate) const;

  /// \brief Indicate if the other RowGroup points to the same RowGroup.
  bool Equals(const RowGroupInfo& other) const { return id() == other.id(); }

 private:
  void SetStatisticsExpression();

  int id_;
  int64_t num_rows_;
  int64_t total_byte_size_;
  std::shared_ptr<Expression> statistics_expression_;
  std::shared_ptr<StructScalar> statistics_;
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
/// It can also attach optional statistics with each RowGroups, providing
/// pushdown predicate benefits before invoking any heavy IO. This can induce
/// significant performance boost when scanning high latency file systems.
class ARROW_DS_EXPORT ParquetFileFragment : public FileFragment {
 public:
  Result<FragmentVector> SplitByRowGroup(const std::shared_ptr<Expression>& predicate);

  /// \brief Return the RowGroups selected by this fragment. An empty list
  /// represents all RowGroups in the parquet file.
  const std::vector<RowGroupInfo>& row_groups() const { return row_groups_; }

  /// \brief Indicate if the attached statistics are complete and the physical schema
  /// is cached.
  ///
  /// The statistics are complete if the provided RowGroups (see `row_groups()`)
  /// is not empty / and all RowGroup return true on `RowGroup::HasStatistics()`.
  bool HasCompleteMetadata() const { return has_complete_metadata_; }

  /// \brief Ensure attached statistics are complete and the physical schema is cached.
  Status EnsureCompleteMetadata(parquet::arrow::FileReader* reader = NULLPTR);

 private:
  ParquetFileFragment(FileSource source, std::shared_ptr<FileFormat> format,
                      std::shared_ptr<Expression> partition_expression,
                      std::shared_ptr<Schema> physical_schema,
                      std::vector<RowGroupInfo> row_groups);

  // Overridden to opportunistically set metadata since a reader must be opened anyway.
  Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override {
    ARROW_RETURN_NOT_OK(EnsureCompleteMetadata());
    return physical_schema_;
  }

  // Return a filtered subset of RowGroupInfos.
  Result<std::vector<RowGroupInfo>> FilterRowGroups(const Expression& predicate);

  std::vector<RowGroupInfo> row_groups_;
  ParquetFileFormat& parquet_format_;
  bool has_complete_metadata_;

  friend class ParquetFileFormat;
};

struct ParquetFactoryOptions {
  // Either an explicit Partitioning or a PartitioningFactory to discover one.
  //
  // If a factory is provided, it will be used to infer a schema for partition fields
  // based on file and directory paths then construct a Partitioning. The default
  // is a Partitioning which will yield no partition information.
  //
  // The (explicit or discovered) partitioning will be applied to discovered files
  // and the resulting partition information embedded in the Dataset.
  PartitioningOrFactory partitioning{Partitioning::Default()};

  // For the purposes of applying the partitioning, paths will be stripped
  // of the partition_base_dir. Files not matching the partition_base_dir
  // prefix will be skipped for partition discovery. The ignored files will still
  // be part of the Dataset, but will not have partition information.
  //
  // Example:
  // partition_base_dir = "/dataset";
  //
  // - "/dataset/US/sales.csv" -> "US/sales.csv" will be given to the partitioning
  //
  // - "/home/john/late_sales.csv" -> Will be ignored for partition discovery.
  //
  // This is useful for partitioning which parses directory when ordering
  // is important, e.g. DirectoryPartitioning.
  std::string partition_base_dir;
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
  ParquetDatasetFactory(std::shared_ptr<fs::FileSystem> fs,
                        std::shared_ptr<ParquetFileFormat> format,
                        std::shared_ptr<parquet::FileMetaData> metadata,
                        std::string base_path, ParquetFactoryOptions options);

  std::shared_ptr<fs::FileSystem> filesystem_;
  std::shared_ptr<ParquetFileFormat> format_;
  std::shared_ptr<parquet::FileMetaData> metadata_;
  std::string base_path_;
  ParquetFactoryOptions options_;
  FragmentVector fragments_;

 private:
  Result<std::vector<std::string>> CollectPaths(
      const parquet::FileMetaData& metadata,
      const parquet::ArrowReaderProperties& properties);

  Result<std::vector<std::shared_ptr<FileFragment>>> CollectParquetFragments(
      const parquet::FileMetaData& metadata,
      const parquet::ArrowReaderProperties& properties, const Partitioning& partitioning);

  Result<std::shared_ptr<Schema>> PartitionSchema();
};

}  // namespace dataset
}  // namespace arrow
