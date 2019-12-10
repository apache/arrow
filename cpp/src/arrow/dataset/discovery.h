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

#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_forest.h"
#include "arrow/util/macros.h"

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
  /// \brief Get the schema for the resulting DataSource.
  virtual Result<std::shared_ptr<Schema>> Inspect() = 0;

  /// \brief Create a DataSource with a given partition.
  virtual Result<DataSourcePtr> Finish() = 0;

  std::shared_ptr<Schema> schema() const { return schema_; }
  Status SetSchema(std::shared_ptr<Schema> schema) {
    schema_ = schema;
    return Status::OK();
  }

  const PartitionSchemePtr& partition_scheme() const { return partition_scheme_; }
  Status SetPartitionScheme(PartitionSchemePtr partition_scheme) {
    partition_scheme_ = partition_scheme;
    return Status::OK();
  }

  const ExpressionPtr& root_partition() const { return root_partition_; }
  Status SetRootPartition(ExpressionPtr partition) {
    root_partition_ = partition;
    return Status::OK();
  }

  virtual ~DataSourceDiscovery() = default;

 protected:
  DataSourceDiscovery();

  std::shared_ptr<Schema> schema_;
  PartitionSchemePtr partition_scheme_;
  ExpressionPtr root_partition_;
};

struct FileSystemDiscoveryOptions {
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
  // IO, but unsupported files may will be present in the DataSource
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
/// fs::FileStats or a fs::Selector.
class ARROW_DS_EXPORT FileSystemDataSourceDiscovery : public DataSourceDiscovery {
 public:
  /// \brief Build a FileSystemDataSourceDiscovery from an explicit list of
  /// fs::FileStats.
  ///
  /// \param[in] filesystem passed to FileSystemDataSource
  /// \param[in] paths passed to FileSystemDataSource
  /// \param[in] format passed to FileSystemDataSource
  /// \param[in] options see FileSystemDiscoveryOptions for more information.
  static Result<DataSourceDiscoveryPtr> Make(fs::FileSystemPtr filesystem,
                                             fs::FileStatsVector paths,
                                             FileFormatPtr format,
                                             FileSystemDiscoveryOptions options);

  /// \brief Build a FileSystemDataSourceDiscovery from a fs::Selector.
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
  static Result<DataSourceDiscoveryPtr> Make(fs::FileSystemPtr filesystem,
                                             fs::Selector selector, FileFormatPtr format,
                                             FileSystemDiscoveryOptions options);

  Result<std::shared_ptr<Schema>> Inspect() override;

  Result<DataSourcePtr> Finish() override;

 protected:
  FileSystemDataSourceDiscovery(fs::FileSystemPtr filesystem,
                                std::vector<fs::FileStats> files, FileFormatPtr format,
                                FileSystemDiscoveryOptions options);

  fs::FileSystemPtr fs_;
  std::vector<fs::FileStats> files_;
  FileFormatPtr format_;
  FileSystemDiscoveryOptions options_;
};

}  // namespace dataset
}  // namespace arrow
