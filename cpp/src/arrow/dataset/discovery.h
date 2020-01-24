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
/// partitioning

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "arrow/dataset/partition.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_forest.h"
#include "arrow/result.h"
#include "arrow/util/macros.h"
#include "arrow/util/variant.h"

namespace arrow {
namespace dataset {

/// \brief SourceFactory provides a way to inspect/discover a Source's expected
/// schema before materializing said Source.
class ARROW_DS_EXPORT SourceFactory {
 public:
  /// \brief Get the schemas of the Fragments and Partitioning.
  virtual Result<std::vector<std::shared_ptr<Schema>>> InspectSchemas() = 0;

  /// \brief Get unified schema for the resulting Source.
  virtual Result<std::shared_ptr<Schema>> Inspect();

  /// \brief Create a Source with the given schema.
  virtual Result<std::shared_ptr<Source>> Finish(
      const std::shared_ptr<Schema>& schema) = 0;

  /// \brief Create a Source using the inspected schema.
  virtual Result<std::shared_ptr<Source>> Finish();

  /// \brief Optional root partition for the resulting Source.
  const std::shared_ptr<Expression>& root_partition() const { return root_partition_; }
  Status SetRootPartition(std::shared_ptr<Expression> partition) {
    root_partition_ = partition;
    return Status::OK();
  }

  virtual ~SourceFactory() = default;

 protected:
  SourceFactory();

  std::shared_ptr<Expression> root_partition_;
};

/// \brief DatasetFactory provides a way to inspect/discover a Dataset's
/// expected schema before materializing the Dataset and underlying Sources.
class ARROW_DS_EXPORT DatasetFactory {
 public:
  static Result<std::shared_ptr<DatasetFactory>> Make(
      std::vector<std::shared_ptr<SourceFactory>> factories);

  /// \brief Return the list of SourceFactory
  const std::vector<std::shared_ptr<SourceFactory>>& factories() const {
    return factories_;
  }

  /// \brief Get the schemas of the Sources.
  Result<std::vector<std::shared_ptr<Schema>>> InspectSchemas();

  /// \brief Get unified schema for the resulting Dataset.
  Result<std::shared_ptr<Schema>> Inspect();

  /// \brief Create a Dataset with the given schema.
  Result<std::shared_ptr<Dataset>> Finish(const std::shared_ptr<Schema>& schema);

  /// \brief Create a Dataset using the inspected schema.
  Result<std::shared_ptr<Dataset>> Finish();

 protected:
  explicit DatasetFactory(std::vector<std::shared_ptr<SourceFactory>> factories);

  std::vector<std::shared_ptr<SourceFactory>> factories_;
};

struct FileSystemFactoryOptions {
  // Either an explicit Partitioning or a PartitioningFactory to discover one.
  //
  // If a factory is provided, it will be used to infer a schema for partition fields
  // based on file and directory paths then construct a Partitioning. The default
  // is a Partitioning which will yield no partition information.
  //
  // The (explicit or discovered) partitioning will be applied to discovered files
  // and the resulting partition information embedded in the Source.
  PartitioningOrFactory partitioning{Partitioning::Default()};

  // For the purposes of applying the partitioning, paths will be stripped
  // of the partition_base_dir. Files not matching the partition_base_dir
  // prefix will be skipped for partition discovery. The ignored files will still
  // be part of the Source, but will not have partition information.
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

  // Invalid files (via selector or explicitly) will be excluded by checking
  // with the FileFormat::IsSupported method.  This will incur IO for each files
  // in a serial and single threaded fashion. Disabling this feature will skip the
  // IO, but unsupported files may be present in the Source
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

/// \brief FileSystemSourceFactory creates a Source from a vector of
/// fs::FileStats or a fs::FileSelector.
class ARROW_DS_EXPORT FileSystemSourceFactory : public SourceFactory {
 public:
  /// \brief Build a FileSystemSourceFactory from an explicit list of
  /// paths.
  ///
  /// \param[in] filesystem passed to FileSystemSource
  /// \param[in] paths passed to FileSystemSource
  /// \param[in] format passed to FileSystemSource
  /// \param[in] options see FileSystemFactoryOptions for more information.
  static Result<std::shared_ptr<SourceFactory>> Make(
      std::shared_ptr<fs::FileSystem> filesystem, const std::vector<std::string>& paths,
      std::shared_ptr<FileFormat> format, FileSystemFactoryOptions options);

  /// \brief Build a FileSystemSourceFactory from a fs::FileSelector.
  ///
  /// The selector will expand to a vector of FileStats. The expansion/crawling
  /// is performed in this function call. Thus, the finalized Source is
  /// working with a snapshot of the filesystem.
  //
  /// If options.partition_base_dir is not provided, it will be overwritten
  /// with selector.base_dir.
  ///
  /// \param[in] filesystem passed to FileSystemSource
  /// \param[in] selector used to crawl and search files
  /// \param[in] format passed to FileSystemSource
  /// \param[in] options see FileSystemFactoryOptions for more information.
  static Result<std::shared_ptr<SourceFactory>> Make(
      std::shared_ptr<fs::FileSystem> filesystem, fs::FileSelector selector,
      std::shared_ptr<FileFormat> format, FileSystemFactoryOptions options);

  Result<std::vector<std::shared_ptr<Schema>>> InspectSchemas() override;

  Result<std::shared_ptr<Source>> Finish(const std::shared_ptr<Schema>& schema) override;

 protected:
  FileSystemSourceFactory(std::shared_ptr<fs::FileSystem> filesystem,
                          fs::PathForest forest, std::shared_ptr<FileFormat> format,
                          FileSystemFactoryOptions options);

  static Result<fs::PathForest> Filter(const std::shared_ptr<fs::FileSystem>& filesystem,
                                       const std::shared_ptr<FileFormat>& format,
                                       const FileSystemFactoryOptions& options,
                                       fs::PathForest forest);

  Result<std::shared_ptr<Schema>> PartitionSchema();

  std::shared_ptr<fs::FileSystem> fs_;
  fs::PathForest forest_;
  std::shared_ptr<FileFormat> format_;
  FileSystemFactoryOptions options_;
};

}  // namespace dataset
}  // namespace arrow
