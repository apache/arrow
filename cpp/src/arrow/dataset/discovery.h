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
#include "arrow/filesystem/path_tree.h"
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
  virtual Status Inspect(std::shared_ptr<Schema>* out) = 0;

  std::shared_ptr<Schema> schema() const { return schema_; }
  Status SetSchema(std::shared_ptr<Schema> schema) {
    schema_ = schema;
    return Status::OK();
  }

  std::shared_ptr<PartitionScheme> partition_scheme() const { return partition_scheme_; }
  Status SetPartitionScheme(std::shared_ptr<PartitionScheme> partition_scheme) {
    partition_scheme_ = partition_scheme;
    return Status::OK();
  }

  std::shared_ptr<Expression> root_partition() const { return root_partition_; }
  Status SetRootPartition(std::shared_ptr<Expression> partition) {
    root_partition_ = partition;
    return Status::OK();
  }

  /// \brief Create a DataSource with a given partition.
  virtual Status Finish(std::shared_ptr<DataSource>* out) = 0;

  virtual ~DataSourceDiscovery() = default;

 protected:
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionScheme> partition_scheme_;
  std::shared_ptr<Expression> root_partition_;
};

/// \brief FileSystemDataSourceFactory creates a DataSource from a vector
/// of fs::FileStats or a fs::Selector.
class ARROW_DS_EXPORT FileSystemDataSourceDiscovery : public DataSourceDiscovery {
 public:
  static Status Make(fs::FileSystem* filesystem, std::vector<fs::FileStats> files,
                     std::shared_ptr<FileFormat> format,
                     std::shared_ptr<DataSourceDiscovery>* out);

  static Status Make(fs::FileSystem* filesystem, fs::Selector selector,
                     std::shared_ptr<FileFormat> format,
                     std::shared_ptr<DataSourceDiscovery>* out);

  Status Inspect(std::shared_ptr<Schema>* out) override;

  Status Finish(std::shared_ptr<DataSource>* out) override;

 protected:
  FileSystemDataSourceDiscovery(fs::FileSystem* filesystem,
                                std::vector<fs::FileStats> files,
                                std::shared_ptr<FileFormat> format);

  fs::FileSystem* fs_;
  std::vector<fs::FileStats> files_;
  std::shared_ptr<FileFormat> format_;
};

}  // namespace dataset
}  // namespace arrow
