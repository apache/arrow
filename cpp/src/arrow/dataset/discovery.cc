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
#include <unordered_set>
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

Result<std::shared_ptr<Schema>> DataSourceDiscovery::Inspect() {
  ARROW_ASSIGN_OR_RAISE(auto schemas, InspectSchemas());

  if (partition_scheme()) {
    schemas.push_back(partition_scheme()->schema());
  }

  if (schemas.empty()) {
    schemas.push_back(arrow::schema({}));
  }

  return UnifySchemas(schemas);
}

FileSystemDataSourceDiscovery::FileSystemDataSourceDiscovery(
    fs::FileSystemPtr filesystem, fs::PathForest forest, FileFormatPtr format,
    FileSystemDiscoveryOptions options)
    : fs_(std::move(filesystem)),
      forest_(std::move(forest)),
      format_(std::move(format)),
      options_(std::move(options)) {}

bool StartsWithAnyOf(const std::vector<std::string>& prefixes, const std::string& path) {
  if (prefixes.empty()) {
    return false;
  }

  auto dir_base = fs::internal::GetAbstractPathParent(path);
  util::string_view basename{dir_base.second};

  auto matches_prefix = [&basename](const std::string& prefix) -> bool {
    return !prefix.empty() && basename.starts_with(prefix);
  };

  return std::any_of(prefixes.cbegin(), prefixes.cend(), matches_prefix);
}

Result<fs::PathForest> FileSystemDataSourceDiscovery::Filter(
    const fs::FileSystemPtr& filesystem, const FileFormatPtr& format,
    const FileSystemDiscoveryOptions& options, fs::PathForest forest) {
  fs::FileStatsVector out;

  auto& stats = forest.stats();
  RETURN_NOT_OK(forest.Visit([&](fs::PathForest::Ref ref) -> fs::PathForest::MaybePrune {
    const auto& path = ref.stats().path();

    if (StartsWithAnyOf(options.ignore_prefixes, path)) {
      return fs::PathForest::Prune;
    }

    if (ref.stats().IsFile() && options.exclude_invalid_files) {
      ARROW_ASSIGN_OR_RAISE(auto supported,
                            format->IsSupported(FileSource(path, filesystem.get())));
      if (!supported) {
        return fs::PathForest::Continue;
      }
    }

    out.push_back(std::move(stats[ref.i]));
    return fs::PathForest::Continue;
  }));

  return fs::PathForest::MakeFromPreSorted(std::move(out));
}

Result<DataSourceDiscoveryPtr> FileSystemDataSourceDiscovery::Make(
    fs::FileSystemPtr filesystem, const std::vector<std::string>& paths,
    FileFormatPtr format, FileSystemDiscoveryOptions options) {
  ARROW_ASSIGN_OR_RAISE(auto files, filesystem->GetTargetStats(paths));
  ARROW_ASSIGN_OR_RAISE(auto forest, fs::PathForest::Make(std::move(files)));

  std::unordered_set<fs::FileStats, fs::FileStats::ByPath> missing;
  DCHECK_OK(forest.Visit([&](fs::PathForest::Ref ref) {
    util::string_view parent_path = options.partition_base_dir;
    if (auto parent = ref.parent()) {
      parent_path = parent.stats().path();
    }

    for (auto&& path :
         fs::internal::AncestorsFromBasePath(parent_path, ref.stats().path())) {
      ARROW_ASSIGN_OR_RAISE(auto file, filesystem->GetTargetStats(std::move(path)));
      missing.insert(std::move(file));
    }
    return Status::OK();
  }));

  files = std::move(forest).stats();
  std::move(missing.begin(), missing.end(), std::back_inserter(files));

  ARROW_ASSIGN_OR_RAISE(forest, fs::PathForest::Make(std::move(files)));

  ARROW_ASSIGN_OR_RAISE(forest, Filter(filesystem, format, options, std::move(forest)));

  return DataSourceDiscoveryPtr(new FileSystemDataSourceDiscovery(
      filesystem, std::move(forest), std::move(format), std::move(options)));
}

Result<DataSourceDiscoveryPtr> FileSystemDataSourceDiscovery::Make(
    fs::FileSystemPtr filesystem, fs::FileSelector selector, FileFormatPtr format,
    FileSystemDiscoveryOptions options) {
  ARROW_ASSIGN_OR_RAISE(auto files, filesystem->GetTargetStats(selector));

  ARROW_ASSIGN_OR_RAISE(auto forest, fs::PathForest::Make(std::move(files)));

  ARROW_ASSIGN_OR_RAISE(forest, Filter(filesystem, format, options, std::move(forest)));

  // By automatically setting the options base_dir to the selector's base_dir,
  // we provide a better experience for user providing PartitionScheme that are
  // relative to the base_dir instead of the full path.
  if (options.partition_base_dir.empty() && !selector.base_dir.empty()) {
    options.partition_base_dir = selector.base_dir;
  }

  return DataSourceDiscoveryPtr(new FileSystemDataSourceDiscovery(
      filesystem, std::move(forest), std::move(format), std::move(options)));
}

Result<std::vector<std::shared_ptr<Schema>>>
FileSystemDataSourceDiscovery::InspectSchemas() {
  std::vector<std::shared_ptr<Schema>> schemas;

  for (const auto& f : forest_.stats()) {
    if (!f.IsFile()) continue;
    auto source = FileSource(f.path(), fs_.get());
    ARROW_ASSIGN_OR_RAISE(auto schema, format_->Inspect(source));
    schemas.push_back(schema);
  }

  return schemas;
}

Result<DataSourcePtr> FileSystemDataSourceDiscovery::Finish() {
  ExpressionVector partitions(forest_.size(), scalar(true));

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

  RETURN_NOT_OK(forest_.Visit(apply_partition_scheme));
  return FileSystemDataSource::Make(fs_, forest_, std::move(partitions), root_partition_,
                                    format_);
}

}  // namespace dataset
}  // namespace arrow
