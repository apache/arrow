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
#include "arrow/filesystem/path_forest.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/status.h"

namespace arrow {
namespace dataset {

DataSourceDiscovery::DataSourceDiscovery()
    : schema_(arrow::schema({})),
      partition_scheme_(PartitionScheme::Default()),
      root_partition_(scalar(true)) {}

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
  ARROW_ASSIGN_OR_RAISE(auto files, filesystem->GetTargetStats(selector));

  // By automatically setting the options base_dir to the selector's base_dir,
  // we provide a better experience for user providing PartitionScheme that are
  // relative to the base_dir instead of the full path.
  if (options.partition_base_dir.empty() && !selector.base_dir.empty()) {
    options.partition_base_dir = selector.base_dir;
  }

  return Make(std::move(filesystem), std::move(files), std::move(format),
              std::move(options));
}

Result<std::shared_ptr<Schema>> FileSystemDataSourceDiscovery::Inspect() {
  std::vector<std::shared_ptr<Schema>> schemas;

  for (const auto& f : files_) {
    if (!f.IsFile()) continue;

    ARROW_ASSIGN_OR_RAISE(auto schema, format_->Inspect(FileSource(f.path(), fs_.get())));
    schemas.push_back(schema);
  }

  if (schemas.empty()) {
    // If there are no files, return the partition scheme's schema.
    return partition_scheme_->schema();
  }

  // TODO merge schemas.
  auto out_schema = arrow::schema(schemas[0]->fields(), schemas[0]->metadata());

  // add fields from partition_scheme_
  for (auto partition_field : partition_scheme_->schema()->fields()) {
    if (out_schema->GetFieldIndex(partition_field->name()) == -1) {
      RETURN_NOT_OK(
          out_schema->AddField(out_schema->num_fields(), partition_field, &out_schema));
    }
  }

  return out_schema;
}

Result<DataSourcePtr> FileSystemDataSourceDiscovery::Finish() {
  ExpressionVector partitions(files_.size(), scalar(true));

  ARROW_ASSIGN_OR_RAISE(auto forest, fs::PathForest::Make(files_));

  // apply partition_scheme to forest to derive partitions
  auto apply_partition_scheme = [&](fs::PathForest::Ref ref) {
    if (auto relative = fs::internal::RemoveAncestor(options_.partition_base_dir,
                                                     ref.stats().path())) {
      auto segments = fs::internal::SplitAbstractPath(relative->to_string());

      if (segments.size() > 0) {
        auto segment_index = static_cast<int>(segments.size()) - 1;
        auto maybe_partition = partition_scheme_->Parse(segments.back(), segment_index);

        partitions[ref.i] = std::move(maybe_partition).ValueOr(scalar(true));
      }
    }
    return Status::OK();
  };

  RETURN_NOT_OK(forest.Visit(apply_partition_scheme));

  return FileSystemDataSource::Make(fs_, std::move(forest), std::move(partitions),
                                    root_partition_, format_);
}

}  // namespace dataset
}  // namespace arrow
