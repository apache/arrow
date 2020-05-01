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

Result<std::shared_ptr<Schema>> DatasetFactory::Inspect(InspectOptions options) {
  ARROW_ASSIGN_OR_RAISE(auto schemas, InspectSchemas(std::move(options)));

  if (schemas.empty()) {
    return arrow::schema({});
  }

  return UnifySchemas(schemas);
}

Result<std::shared_ptr<Dataset>> DatasetFactory::Finish() {
  FinishOptions options;
  return Finish(options);
}

Result<std::shared_ptr<Dataset>> DatasetFactory::Finish(std::shared_ptr<Schema> schema) {
  FinishOptions options;
  options.schema = schema;
  return Finish(std::move(options));
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

Result<std::vector<std::shared_ptr<Schema>>> UnionDatasetFactory::InspectSchemas(
    InspectOptions options) {
  std::vector<std::shared_ptr<Schema>> schemas;

  for (const auto& child_factory : factories_) {
    ARROW_ASSIGN_OR_RAISE(auto child_schemas, child_factory->InspectSchemas(options));
    ARROW_ASSIGN_OR_RAISE(auto child_schema, UnifySchemas(child_schemas));
    schemas.emplace_back(child_schema);
  }

  return schemas;
}

Result<std::shared_ptr<Dataset>> UnionDatasetFactory::Finish(FinishOptions options) {
  std::vector<std::shared_ptr<Dataset>> children;

  if (options.schema == nullptr) {
    // Set the schema in the option directly for use in `child_factory->Finish()`
    ARROW_ASSIGN_OR_RAISE(options.schema, Inspect(options.inspect_options));
  }

  for (const auto& child_factory : factories_) {
    ARROW_ASSIGN_OR_RAISE(auto child, child_factory->Finish(options));
    children.emplace_back(child);
  }

  return std::shared_ptr<Dataset>(new UnionDataset(options.schema, std::move(children)));
}

FileSystemDatasetFactory::FileSystemDatasetFactory(
    std::vector<std::string> paths, std::shared_ptr<fs::FileSystem> filesystem,
    std::shared_ptr<FileFormat> format, FileSystemFactoryOptions options)
    : paths_(std::move(paths)),
      fs_(std::move(filesystem)),
      format_(std::move(format)),
      options_(std::move(options)) {}

util::optional<util::string_view> FileSystemDatasetFactory::RemovePartitionBaseDir(
    util::string_view path) {
  const util::string_view partition_base_dir{options_.partition_base_dir};
  return fs::internal::RemoveAncestor(partition_base_dir, path);
}

Result<std::shared_ptr<DatasetFactory>> FileSystemDatasetFactory::Make(
    std::shared_ptr<fs::FileSystem> filesystem, const std::vector<std::string>& paths,
    std::shared_ptr<FileFormat> format, FileSystemFactoryOptions options) {
  std::vector<std::string> filtered_paths;
  for (const auto& path : paths) {
    if (options.exclude_invalid_files) {
      ARROW_ASSIGN_OR_RAISE(auto supported,
                            format->IsSupported(FileSource(path, filesystem)));
      if (!supported) {
        continue;
      }
    }

    filtered_paths.push_back(path);
  }

  return std::shared_ptr<DatasetFactory>(
      new FileSystemDatasetFactory(std::move(filtered_paths), std::move(filesystem),
                                   std::move(format), std::move(options)));
}

bool StartsWithAnyOf(const std::string& path, const std::vector<std::string>& prefixes) {
  if (prefixes.empty()) {
    return false;
  }

  auto parts = fs::internal::SplitAbstractPath(path);
  return std::any_of(parts.cbegin(), parts.cend(), [&](util::string_view part) {
    return std::any_of(prefixes.cbegin(), prefixes.cend(), [&](util::string_view prefix) {
      return util::string_view(part).starts_with(prefix);
    });
  });
}

Result<std::shared_ptr<DatasetFactory>> FileSystemDatasetFactory::Make(
    std::shared_ptr<fs::FileSystem> filesystem, fs::FileSelector selector,
    std::shared_ptr<FileFormat> format, FileSystemFactoryOptions options) {
  // By automatically setting the options base_dir to the selector's base_dir,
  // we provide a better experience for user providing Partitioning that are
  // relative to the base_dir instead of the full path.
  if (options.partition_base_dir.empty() && !selector.base_dir.empty()) {
    options.partition_base_dir = selector.base_dir;
  }

  ARROW_ASSIGN_OR_RAISE(auto files, filesystem->GetFileInfo(selector));

  std::vector<std::string> paths;
  for (const auto& info : files) {
    const auto& path = info.path();

    if (!info.IsFile()) {
      // TODO(fsaintjacques): push this filtering into Selector logic so we
      // don't copy big vector around.
      continue;
    }

    if (StartsWithAnyOf(path, options.selector_ignore_prefixes)) {
      continue;
    }

    paths.push_back(path);
  }

  // Sorting by path guarantees a stability sometimes needed by unit tests.
  std::sort(paths.begin(), paths.end());

  return Make(std::move(filesystem), std::move(paths), std::move(format),
              std::move(options));
}

Result<std::shared_ptr<Schema>> FileSystemDatasetFactory::PartitionSchema() {
  if (auto partitioning = options_.partitioning.partitioning()) {
    return partitioning->schema();
  }

  std::vector<util::string_view> relative_paths;
  for (const auto& path : paths_) {
    if (auto relative = RemovePartitionBaseDir(path)) {
      relative_paths.push_back(*relative);
    }
  }

  return options_.partitioning.factory()->Inspect(relative_paths);
}

Result<std::vector<std::shared_ptr<Schema>>> FileSystemDatasetFactory::InspectSchemas(
    InspectOptions options) {
  std::vector<std::shared_ptr<Schema>> schemas;

  const bool has_fragments_limit = options.fragments >= 0;
  int fragments = options.fragments;
  for (const auto& path : paths_) {
    if (has_fragments_limit && fragments-- == 0) break;
    ARROW_ASSIGN_OR_RAISE(auto schema, format_->Inspect({path, fs_}));
    schemas.push_back(schema);
  }

  ARROW_ASSIGN_OR_RAISE(auto partition_schema, PartitionSchema());
  schemas.push_back(partition_schema);

  return schemas;
}

Result<std::shared_ptr<Dataset>> FileSystemDatasetFactory::Finish(FinishOptions options) {
  std::shared_ptr<Schema> schema = options.schema;
  bool schema_missing = schema == nullptr;
  if (schema_missing) {
    ARROW_ASSIGN_OR_RAISE(schema, Inspect(options.inspect_options));
  }

  if (options.validate_fragments && !schema_missing) {
    // If the schema was not explicitly provided we don't need to validate
    // since Inspect has already succeeded in producing a valid unified schema.
    ARROW_ASSIGN_OR_RAISE(auto schemas, InspectSchemas(options.inspect_options));
    for (const auto& s : schemas) {
      RETURN_NOT_OK(SchemaBuilder::AreCompatible({schema, s}));
    }
  }

  std::shared_ptr<Partitioning> partitioning = options_.partitioning.partitioning();
  if (partitioning == nullptr) {
    auto factory = options_.partitioning.factory();
    ARROW_ASSIGN_OR_RAISE(partitioning, factory->Finish(schema));
  }

  std::vector<std::shared_ptr<FileFragment>> fragments;
  for (const auto& path : paths_) {
    std::shared_ptr<Expression> partition = scalar(true);
    if (auto relative = RemovePartitionBaseDir(path)) {
      partition = partitioning->Parse(relative->to_string()).ValueOr(scalar(true));
    }

    ARROW_ASSIGN_OR_RAISE(auto fragment, format_->MakeFragment({path, fs_}, partition));
    fragments.push_back(fragment);
  }

  return FileSystemDataset::Make(schema, root_partition_, format_, fragments);
}

}  // namespace dataset
}  // namespace arrow
