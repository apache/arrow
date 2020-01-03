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

/// Logic for automatically determining the structure of multi-file
/// dataset with possible partitioning according to available
/// partition schemes

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "arrow/dataset/partition.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_forest.h"
#include "arrow/util/macros.h"
#include "arrow/util/variant.h"

namespace arrow {
namespace dataset {

/// \brief DataSourceDiscovery provides a way to inspect a DataSource potential
/// schema before materializing it. Thus, the user can peek the schema for
/// data sources and decide on a unified schema. The pseudocode would look like
///
/// def get_dataset(factories):
///   schemas = []
///   for f in factories:
///     schemas.append(f.Inspect())
///
///   common_schema = UnifySchemas(schemas)
///
///   sources = []
///   for f in factories:
///     f.SetSchema(common_schema)
///     sources.append(f.Finish())
///
///   return Dataset(sources, common_schema)
class ARROW_DS_EXPORT DataSourceDiscovery {
 public:
  /// \brief Get the schemas of the DataFragments and PartitionScheme.
  virtual Result<std::vector<std::shared_ptr<Schema>>> InspectSchemas() = 0;

  /// \brief Get unified schema for the resulting DataSource.
  virtual Result<std::shared_ptr<Schema>> Inspect();

  /// \brief Create a DataSource with the given schema.
  virtual Result<std::shared_ptr<DataSource>> Finish(
      const std::shared_ptr<Schema>& schema) = 0;

  /// \brief Create a DataSource using an inspected schema.
  virtual Result<std::shared_ptr<DataSource>> Finish();

  /// \brief Optional root partition for the resulting DataSource.
  const std::shared_ptr<Expression>& root_partition() const { return root_partition_; }
  Status SetRootPartition(std::shared_ptr<Expression> partition) {
    root_partition_ = partition;
    return Status::OK();
  }

  virtual ~DataSourceDiscovery() = default;

 protected:
  DataSourceDiscovery();

  std::shared_ptr<Expression> root_partition_;
};

struct FileSystemDiscoveryOptions {
  // Either an explicit PartitionScheme or a PartitionSchemeDiscovery to discover one.
  //
  // If a discovery is provided, it will be used to infer a schema for partition fields
  // based on file and directory paths then construct a PartitionScheme. The default
  // is a PartitionScheme which will yield no partition information.
  //
  // The (explicit or discovered) partition scheme will be applied to discovered files
  // and the resulting partition information embedded in the DataSource.
  PartitionSchemeOrDiscovery partition_scheme{PartitionScheme::Default()};

  // For the purposes of applying the partition scheme, paths will be stripped
  // of the partition_base_dir. Files not matching the partition_base_dir
  // prefix will be skipped for partition discovery. The ignored files will still
  // be part of the DataSource, but will not have partition information.
  //
  // Example:
  // partition_base_dir = "/dataset";
  //
  // - "/dataset/US/sales.csv" -> "US/sales.csv" will be given to the partition
  //                              scheme
  //
  // - "/home/john/late_sales.csv" -> Will be ignored for partition discovery.
  //
  // This is useful for partition schemes which parses directory when ordering
  // is important, e.g. SchemaPartitionScheme.
  std::string partition_base_dir;

  // Invalid files (via selector or explicitly) will be excluded by checking
  // with the FileFormat::IsSupported method.  This will incur IO for each files
  // in a serial and single threaded fashion. Disabling this feature will skip the
  // IO, but unsupported files may be present in the DataSource
  // (resulting in an error at scan time).
  bool exclude_invalid_files = true;

  // Files matching one of the following prefix will be ignored by the
  // discovery process. This is matched to the basename of a path.
  //
  // Example:
  // ignore_prefixes = {"_", ".DS_STORE" };
  //
  // - "/dataset/data.csv" -> not ignored
  // - "/dataset/_metadata" -> ignored
  // - "/dataset/.DS_STORE" -> ignored
  std::vector<std::string> ignore_prefixes = {
      ".",
      "_",
  };
};

/// \brief FileSystemDataSourceFactory creates a DataSource from a vector of
/// fs::FileStats or a fs::FileSelector.
class ARROW_DS_EXPORT FileSystemDataSourceDiscovery : public DataSourceDiscovery {
 public:
  /// \brief Build a FileSystemDataSourceDiscovery from an explicit list of
  /// paths.
  ///
  /// \param[in] filesystem passed to FileSystemDataSource
  /// \param[in] paths passed to FileSystemDataSource
  /// \param[in] format passed to FileSystemDataSource
  /// \param[in] options see FileSystemDiscoveryOptions for more information.
  static Result<std::shared_ptr<DataSourceDiscovery>> Make(
      std::shared_ptr<fs::FileSystem> filesystem, const std::vector<std::string>& paths,
      std::shared_ptr<FileFormat> format, FileSystemDiscoveryOptions options);

  /// \brief Build a FileSystemDataSourceDiscovery from a fs::FileSelector.
  ///
  /// The selector will expand to a vector of FileStats. The expansion/crawling
  /// is performed in this function call. Thus, the finalized DataSource is
  /// working with a snapshot of the filesystem.
  //
  /// If options.partition_base_dir is not provided, it will be overwritten
  /// with selector.base_dir.
  ///
  /// \param[in] filesystem passed to FileSystemDataSource
  /// \param[in] selector used to crawl and search files
  /// \param[in] format passed to FileSystemDataSource
  /// \param[in] options see FileSystemDiscoveryOptions for more information.
  static Result<std::shared_ptr<DataSourceDiscovery>> Make(
      std::shared_ptr<fs::FileSystem> filesystem, fs::FileSelector selector,
      std::shared_ptr<FileFormat> format, FileSystemDiscoveryOptions options);

  Result<std::vector<std::shared_ptr<Schema>>> InspectSchemas() override;

  Result<std::shared_ptr<DataSource>> Finish(
      const std::shared_ptr<Schema>& schema) override;

 protected:
  FileSystemDataSourceDiscovery(std::shared_ptr<fs::FileSystem> filesystem,
                                fs::PathForest forest, std::shared_ptr<FileFormat> format,
                                FileSystemDiscoveryOptions options);

  static Result<fs::PathForest> Filter(const std::shared_ptr<fs::FileSystem>& filesystem,
                                       const std::shared_ptr<FileFormat>& format,
                                       const FileSystemDiscoveryOptions& options,
                                       fs::PathForest forest);

  Result<std::shared_ptr<Schema>> PartitionSchema();

  std::shared_ptr<fs::FileSystem> fs_;
  fs::PathForest forest_;
  std::shared_ptr<FileFormat> format_;
  FileSystemDiscoveryOptions options_;
};

}  // namespace dataset
}  // namespace arrow
