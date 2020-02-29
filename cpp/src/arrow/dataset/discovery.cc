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

namespace arrow {
namespace dataset {

DatasetFactory::DatasetFactory() : root_partition_(scalar(true)) {}

Result<std::shared_ptr<Schema>> DatasetFactory::Inspect() {
  ARROW_ASSIGN_OR_RAISE(auto schemas, InspectSchemas());

  if (schemas.empty()) {
    schemas.push_back(arrow::schema({}));
  }

  return UnifySchemas(schemas);
}

UnionDatasetFactory::UnionDatasetFactory(
    std::vector<std::shared_ptr<DatasetFactory>> factories)
    : factories_(std::move(factories)) {}

Result<std::shared_ptr<DatasetFactory>> UnionDatasetFactory::Make(
    std::vector<std::shared_ptr<DatasetFactory>> factories) {
  for (const auto& factory : factories) {
    if (factory == nullptr) {
      return Status::Invalid("Can't accept nullptr DatasetFactory");
    }
  }

  return std::shared_ptr<UnionDatasetFactory>{
      new UnionDatasetFactory(std::move(factories))};
}

Result<std::vector<std::shared_ptr<Schema>>> UnionDatasetFactory::InspectSchemas() {
  std::vector<std::shared_ptr<Schema>> schemas;

  for (const auto& child_factory : factories_) {
    ARROW_ASSIGN_OR_RAISE(auto schema, child_factory->Inspect());
    schemas.emplace_back(schema);
  }

  return schemas;
}

Result<std::shared_ptr<Schema>> UnionDatasetFactory::Inspect() {
  ARROW_ASSIGN_OR_RAISE(auto schemas, InspectSchemas());

  if (schemas.empty()) {
    return arrow::schema({});
  }

  return UnifySchemas(schemas);
}

Result<std::shared_ptr<Dataset>> UnionDatasetFactory::Finish(
    const std::shared_ptr<Schema>& schema) {
  std::vector<std::shared_ptr<Dataset>> children;

  for (const auto& child_factory : factories_) {
    ARROW_ASSIGN_OR_RAISE(auto child, child_factory->Finish(schema));
    children.emplace_back(child);
  }

  return std::shared_ptr<Dataset>(new UnionDataset(schema, std::move(children)));
}

Result<std::shared_ptr<Dataset>> UnionDatasetFactory::Finish() {
  ARROW_ASSIGN_OR_RAISE(auto schema, Inspect());
  return Finish(schema);
}

FileSystemDatasetFactory::FileSystemDatasetFactory(
    std::shared_ptr<fs::FileSystem> filesystem, fs::PathForest forest,
    std::shared_ptr<FileFormat> format, FileSystemFactoryOptions options)
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

Result<fs::PathForest> FileSystemDatasetFactory::Filter(
    const std::shared_ptr<fs::FileSystem>& filesystem,
    const std::shared_ptr<FileFormat>& format, const FileSystemFactoryOptions& options,
    fs::PathForest forest) {
  std::vector<fs::FileInfo> out;

  auto& infos = forest.infos();
  RETURN_NOT_OK(forest.Visit([&](fs::PathForest::Ref ref) -> fs::PathForest::MaybePrune {
    const auto& path = ref.info().path();

    if (StartsWithAnyOf(options.ignore_prefixes, path)) {
      return fs::PathForest::Prune;
    }

    if (ref.info().IsFile() && options.exclude_invalid_files) {
      ARROW_ASSIGN_OR_RAISE(auto supported,
                            format->IsSupported(FileSource(path, filesystem.get())));
      if (!supported) {
        return fs::PathForest::Continue;
      }
    }

    out.push_back(std::move(infos[ref.i]));
    return fs::PathForest::Continue;
  }));

  return fs::PathForest::MakeFromPreSorted(std::move(out));
}

Result<std::shared_ptr<DatasetFactory>> FileSystemDatasetFactory::Make(
    std::shared_ptr<fs::FileSystem> filesystem, const std::vector<std::string>& paths,
    std::shared_ptr<FileFormat> format, FileSystemFactoryOptions options) {
  ARROW_ASSIGN_OR_RAISE(auto files, filesystem->GetTargetInfos(paths));
  ARROW_ASSIGN_OR_RAISE(auto forest, fs::PathForest::Make(std::move(files)));

  std::unordered_set<fs::FileInfo, fs::FileInfo::ByPath> missing;
  DCHECK_OK(forest.Visit([&](fs::PathForest::Ref ref) {
    util::string_view parent_path = options.partition_base_dir;
    if (auto parent = ref.parent()) {
      parent_path = parent.info().path();
    }

    for (auto&& path :
         fs::internal::AncestorsFromBasePath(parent_path, ref.info().path())) {
      ARROW_ASSIGN_OR_RAISE(auto file, filesystem->GetTargetInfo(std::move(path)));
      missing.insert(std::move(file));
    }
    return Status::OK();
  }));

  files = std::move(forest).infos();
  std::move(missing.begin(), missing.end(), std::back_inserter(files));

  ARROW_ASSIGN_OR_RAISE(forest, fs::PathForest::Make(std::move(files)));

  ARROW_ASSIGN_OR_RAISE(forest, Filter(filesystem, format, options, std::move(forest)));

  return std::shared_ptr<DatasetFactory>(new FileSystemDatasetFactory(
      std::move(filesystem), std::move(forest), std::move(format), std::move(options)));
}

Result<std::shared_ptr<DatasetFactory>> FileSystemDatasetFactory::Make(
    std::shared_ptr<fs::FileSystem> filesystem, fs::FileSelector selector,
    std::shared_ptr<FileFormat> format, FileSystemFactoryOptions options) {
  ARROW_ASSIGN_OR_RAISE(auto files, filesystem->GetTargetInfos(selector));

  ARROW_ASSIGN_OR_RAISE(auto forest, fs::PathForest::Make(std::move(files)));

  ARROW_ASSIGN_OR_RAISE(forest, Filter(filesystem, format, options, std::move(forest)));

  // By automatically setting the options base_dir to the selector's base_dir,
  // we provide a better experience for user providing Partitioning that are
  // relative to the base_dir instead of the full path.
  if (options.partition_base_dir.empty() && !selector.base_dir.empty()) {
    options.partition_base_dir = selector.base_dir;
  }

  return std::shared_ptr<DatasetFactory>(new FileSystemDatasetFactory(
      filesystem, std::move(forest), std::move(format), std::move(options)));
}

Result<std::shared_ptr<Schema>> FileSystemDatasetFactory::PartitionSchema() {
  if (auto partitioning = options_.partitioning.partitioning()) {
    return partitioning->schema();
  }

  std::vector<util::string_view> paths;
  for (const auto& info : forest_.infos()) {
    if (auto relative =
            fs::internal::RemoveAncestor(options_.partition_base_dir, info.path())) {
      paths.push_back(*relative);
    }
  }

  return options_.partitioning.factory()->Inspect(paths);
}

Result<std::vector<std::shared_ptr<Schema>>> FileSystemDatasetFactory::InspectSchemas() {
  std::vector<std::shared_ptr<Schema>> schemas;

  for (const auto& f : forest_.infos()) {
    if (!f.IsFile()) continue;
    FileSource src(f.path(), fs_.get());
    ARROW_ASSIGN_OR_RAISE(auto schema, format_->Inspect(src));
    schemas.push_back(schema);
  }

  ARROW_ASSIGN_OR_RAISE(auto partition_schema, PartitionSchema());
  schemas.push_back(partition_schema);

  return schemas;
}

Result<std::shared_ptr<Dataset>> DatasetFactory::Finish() {
  ARROW_ASSIGN_OR_RAISE(auto schema, Inspect());
  return Finish(schema);
}

Result<std::shared_ptr<Dataset>> FileSystemDatasetFactory::Finish(
    const std::shared_ptr<Schema>& schema) {
  // This validation can be costly, but better safe than sorry.
  ARROW_ASSIGN_OR_RAISE(auto schemas, InspectSchemas());
  for (const auto& s : schemas) {
    RETURN_NOT_OK(SchemaBuilder::AreCompatible({schema, s}));
  }

  ExpressionVector partitions(forest_.size(), scalar(true));
  std::shared_ptr<Partitioning> partitioning = options_.partitioning.partitioning();
  if (partitioning == nullptr) {
    auto factory = options_.partitioning.factory();
    ARROW_ASSIGN_OR_RAISE(partitioning, factory->Finish(schema));
  }

  // apply partitioning to forest to derive partitions
  auto apply_partitioning = [&](fs::PathForest::Ref ref) {
    if (auto relative = fs::internal::RemoveAncestor(options_.partition_base_dir,
                                                     ref.info().path())) {
      auto segments = fs::internal::SplitAbstractPath(relative->to_string());

      if (segments.size() > 0) {
        auto segment_index = static_cast<int>(segments.size()) - 1;
        auto maybe_partition = partitioning->Parse(segments.back(), segment_index);

        partitions[ref.i] = std::move(maybe_partition).ValueOr(scalar(true));
      }
    }
    return Status::OK();
  };

  RETURN_NOT_OK(forest_.Visit(apply_partitioning));

  return FileSystemDataset::Make(schema, root_partition_, format_, fs_, forest_,
                                 std::move(partitions));
}

}  // namespace dataset
}  // namespace arrow
