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

#include "arrow/dataset/discovery.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/filesystem/path_tree.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/status.h"

namespace arrow {
namespace dataset {

FileSystemDataSourceDiscovery::FileSystemDataSourceDiscovery(
    fs::FileSystem* filesystem, std::vector<fs::FileStats> files,
    std::shared_ptr<FileFormat> format, FileSystemDiscoveryOptions options)
    : fs_(filesystem),
      files_(std::move(files)),
      format_(std::move(format)),
      options_(std::move(options)) {}

bool FilterFileByPrefix(const std::vector<std::string>& prefixes,
                        const fs::FileStats& file) {
  if (!file.IsFile()) return true;

  auto dir_base = fs::internal::GetAbstractPathParent(file.path());
  util::string_view basename{dir_base.second};

  auto matches_prefix = [&basename](const std::string& prefix) -> bool {
    return !prefix.empty() && basename.starts_with(prefix);
  };

  return std::none_of(prefixes.cbegin(), prefixes.cend(), matches_prefix);
}

Result<bool> FilterFileBySupportedFormat(const FileFormat& format,
                                         const fs::FileStats& file, fs::FileSystem* fs) {
  if (!file.IsFile()) return true;

  bool supported = false;
  RETURN_NOT_OK(format.IsSupported(FileSource(file.path(), fs), &supported));

  return supported;
}

Status FileSystemDataSourceDiscovery::Make(fs::FileSystem* filesystem,
                                           std::vector<fs::FileStats> files,
                                           std::shared_ptr<FileFormat> format,
                                           FileSystemDiscoveryOptions options,
                                           std::shared_ptr<DataSourceDiscovery>* out) {
  DCHECK_NE(format, nullptr);

  bool has_prefixes = !options.ignore_prefixes.empty();
  std::vector<fs::FileStats> filtered;
  for (const auto& stat : files) {
    if (has_prefixes && !FilterFileByPrefix(options.ignore_prefixes, stat)) continue;

    if (options.filter_supported_files) {
      // Propagate a real (likely IO) error.
      ARROW_ASSIGN_OR_RAISE(bool supported,
                            FilterFileBySupportedFormat(*format, stat, filesystem));
      if (!supported) continue;
    }

    filtered.push_back(stat);
  }

  out->reset(new FileSystemDataSourceDiscovery(filesystem, std::move(filtered),
                                               std::move(format), std::move(options)));

  return Status::OK();
}

Status FileSystemDataSourceDiscovery::Make(fs::FileSystem* filesystem,
                                           fs::Selector selector,
                                           std::shared_ptr<FileFormat> format,
                                           FileSystemDiscoveryOptions options,
                                           std::shared_ptr<DataSourceDiscovery>* out) {
  std::vector<fs::FileStats> files;
  RETURN_NOT_OK(filesystem->GetTargetStats(selector, &files));

  // By automatically setting the options base_dir to the selector's base_dir,
  // we provide a better experience for user providing PartitionScheme that are
  // relative to the base_dir instead of the full path.
  if (options.partition_base_dir.empty() && !selector.base_dir.empty()) {
    options.partition_base_dir = selector.base_dir;
  }

  return Make(filesystem, std::move(files), std::move(format), std::move(options), out);
}

static inline Status InspectSchema(fs::FileSystem* fs,
                                   const std::vector<fs::FileStats> stats,
                                   const std::shared_ptr<FileFormat>& format,
                                   std::shared_ptr<Schema>* out) {
  std::vector<std::shared_ptr<Schema>> schemas;

  for (const auto& f : stats) {
    if (!f.IsFile()) continue;

    std::shared_ptr<Schema> schema;
    RETURN_NOT_OK(format->Inspect(FileSource(f.path(), fs), &schema));
    schemas.push_back(schema);
  }

  if (schemas.size() > 0) {
    // TODO merge schemas.
    *out = schemas[0];
  }

  return Status::OK();
}

Status FileSystemDataSourceDiscovery::Inspect(std::shared_ptr<Schema>* out) {
  return InspectSchema(fs_, files_, format_, out);
}

Status FileSystemDataSourceDiscovery::Finish(std::shared_ptr<DataSource>* out) {
  PathPartitions partitions;

  if (partition_scheme_ != nullptr) {
    RETURN_NOT_OK(ApplyPartitionScheme(*partition_scheme_, options_.partition_base_dir,
                                       files_, &partitions));
  }

  return FileSystemBasedDataSource::Make(fs_, files_, root_partition(),
                                         std::move(partitions), format_, out);
}

}  // namespace dataset
}  // namespace arrow
