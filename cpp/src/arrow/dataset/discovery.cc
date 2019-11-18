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
    fs::FileSystemPtr filesystem, fs::FileStatsVector files, FileFormatPtr format,
    FileSystemDiscoveryOptions options)
    : fs_(std::move(filesystem)),
      files_(std::move(files)),
      format_(std::move(format)),
      options_(std::move(options)) {}

bool StartsWithAnyOf(const std::vector<std::string>& prefixes, const std::string& path) {
  auto dir_base = fs::internal::GetAbstractPathParent(path);
  util::string_view basename{dir_base.second};

  auto matches_prefix = [&basename](const std::string& prefix) -> bool {
    return !prefix.empty() && basename.starts_with(prefix);
  };

  return std::any_of(prefixes.cbegin(), prefixes.cend(), matches_prefix);
}

Result<DataSourceDiscoveryPtr> FileSystemDataSourceDiscovery::Make(
    fs::FileSystemPtr fs, fs::FileStatsVector files, FileFormatPtr format,
    FileSystemDiscoveryOptions options) {
  DCHECK_NE(format, nullptr);

  bool has_prefixes = !options.ignore_prefixes.empty();
  std::vector<fs::FileStats> filtered;
  for (const auto& stat : files) {
    if (stat.IsFile()) {
      const std::string& path = stat.path();

      if (has_prefixes && StartsWithAnyOf(options.ignore_prefixes, path)) {
        continue;
      }

      if (options.exclude_invalid_files) {
        ARROW_ASSIGN_OR_RAISE(auto supported,
                              format->IsSupported(FileSource(path, fs.get())));
        if (!supported) {
          continue;
        }
      }
    }

    filtered.push_back(stat);
  }

  return DataSourceDiscoveryPtr(new FileSystemDataSourceDiscovery(
      fs, std::move(filtered), std::move(format), std::move(options)));
}

Result<DataSourceDiscoveryPtr> FileSystemDataSourceDiscovery::Make(
    fs::FileSystemPtr filesystem, fs::Selector selector, FileFormatPtr format,
    FileSystemDiscoveryOptions options) {
  std::vector<fs::FileStats> files;
  RETURN_NOT_OK(filesystem->GetTargetStats(selector, &files));

  // By automatically setting the options base_dir to the selector's base_dir,
  // we provide a better experience for user providing PartitionScheme that are
  // relative to the base_dir instead of the full path.
  if (options.partition_base_dir.empty() && !selector.base_dir.empty()) {
    options.partition_base_dir = selector.base_dir;
  }

  return Make(std::move(filesystem), std::move(files), std::move(format),
              std::move(options));
}

static inline Result<std::shared_ptr<Schema>> InspectSchema(
    fs::FileSystem* fs, const std::vector<fs::FileStats> stats,
    const FileFormatPtr& format) {
  std::vector<std::shared_ptr<Schema>> schemas;

  for (const auto& f : stats) {
    if (!f.IsFile()) continue;

    ARROW_ASSIGN_OR_RAISE(auto schema, format->Inspect(FileSource(f.path(), fs)));
    schemas.push_back(schema);
  }

  if (schemas.size() > 0) {
    // TODO merge schemas.
    return schemas[0];
  }

  // If there is no files, return an empty schema.
  return std::shared_ptr<Schema>(nullptr);
}

Result<std::shared_ptr<Schema>> FileSystemDataSourceDiscovery::Inspect() {
  return InspectSchema(fs_.get(), files_, format_);
}

Result<DataSourcePtr> FileSystemDataSourceDiscovery::Finish() {
  PathPartitions partitions;

  if (partition_scheme_ != nullptr) {
    ARROW_ASSIGN_OR_RAISE(
        partitions,
        ApplyPartitionScheme(*partition_scheme_, options_.partition_base_dir, files_));
  }

  return FileSystemDataSource::Make(fs_, files_, root_partition(), std::move(partitions),
                                    format_);
}

}  // namespace dataset
}  // namespace arrow
