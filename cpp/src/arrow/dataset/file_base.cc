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

#include "arrow/dataset/file_base.h"

#include <algorithm>
#include <vector>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/iterator.h"
#include "arrow/util/task_group.h"

namespace arrow {
namespace dataset {

Result<std::shared_ptr<arrow::io::RandomAccessFile>> FileSource::Open() const {
  if (type() == PATH) {
    return filesystem()->OpenInputFile(path());
  }

  return std::make_shared<::arrow::io::BufferReader>(buffer());
}

Result<std::shared_ptr<arrow::io::OutputStream>> FileSource::OpenWritable() const {
  if (!writable_) {
    return Status::Invalid("file source '", path(), "' is not writable");
  }

  if (type() == PATH) {
    return filesystem()->OpenOutputStream(path());
  }

  auto b = internal::checked_pointer_cast<ResizableBuffer>(buffer());
  return std::make_shared<::arrow::io::BufferOutputStream>(b);
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<ScanOptions> options) {
  return MakeFragment(std::move(source), std::move(options), scalar(true));
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<ScanOptions> options,
    std::shared_ptr<Expression> partition_expression) {
  return std::shared_ptr<FileFragment>(new FileFragment(
      std::move(source), shared_from_this(), options, std::move(partition_expression)));
}

Result<std::shared_ptr<WriteTask>> FileFormat::WriteFragment(
    FileSource destination, std::shared_ptr<Fragment> fragment,
    std::shared_ptr<ScanContext> scan_context) {
  return Status::NotImplemented("writing fragment of format ", type_name());
}

Result<ScanTaskIterator> FileFragment::Scan(std::shared_ptr<ScanContext> context) {
  return format_->ScanFile(source_, scan_options_, std::move(context));
}

FileSystemDataset::FileSystemDataset(std::shared_ptr<Schema> schema,
                                     std::shared_ptr<Expression> root_partition,
                                     std::shared_ptr<FileFormat> format,
                                     std::shared_ptr<fs::FileSystem> filesystem,
                                     fs::PathForest forest,
                                     ExpressionVector file_partitions)
    : Dataset(std::move(schema), std::move(root_partition)),
      format_(std::move(format)),
      filesystem_(std::move(filesystem)),
      forest_(std::move(forest)),
      partitions_(std::move(file_partitions)) {
  DCHECK_EQ(static_cast<size_t>(forest_.size()), partitions_.size());
}

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Make(
    std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
    std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
    std::vector<fs::FileInfo> infos) {
  ExpressionVector partitions(infos.size(), scalar(true));
  return Make(std::move(schema), std::move(root_partition), std::move(format),
              std::move(filesystem), std::move(infos), std::move(partitions));
}

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Make(
    std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
    std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
    std::vector<fs::FileInfo> infos, ExpressionVector partitions) {
  ARROW_ASSIGN_OR_RAISE(auto forest, fs::PathForest::Make(std::move(infos), &partitions));
  return Make(std::move(schema), std::move(root_partition), std::move(format),
              std::move(filesystem), std::move(forest), std::move(partitions));
}

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Make(
    std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
    std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
    fs::PathForest forest, ExpressionVector partitions) {
  return std::shared_ptr<FileSystemDataset>(new FileSystemDataset(
      std::move(schema), std::move(root_partition), std::move(format),
      std::move(filesystem), std::move(forest), std::move(partitions)));
}

Result<std::shared_ptr<Dataset>> FileSystemDataset::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  RETURN_NOT_OK(CheckProjectable(*schema_, *schema));
  return std::shared_ptr<Dataset>(
      new FileSystemDataset(std::move(schema), partition_expression_, format_,
                            filesystem_, forest_, partitions_));
}

std::vector<std::string> FileSystemDataset::files() const {
  std::vector<std::string> files;

  DCHECK_OK(forest_.Visit([&](fs::PathForest::Ref ref) {
    if (ref.info().IsFile()) {
      files.push_back(ref.info().path());
    }
    return Status::OK();
  }));

  return files;
}

std::string FileSystemDataset::ToString() const {
  std::string repr = "FileSystemDataset:";

  if (forest_.size() == 0) {
    return repr + " []";
  }

  DCHECK_OK(forest_.Visit([&](fs::PathForest::Ref ref) {
    repr += "\n" + ref.info().path();

    if (!partitions_[ref.i]->Equals(true)) {
      repr += ": " + partitions_[ref.i]->ToString();
    }

    return Status::OK();
  }));

  return repr;
}

std::shared_ptr<Expression> FoldingAnd(const std::shared_ptr<Expression>& l,
                                       const std::shared_ptr<Expression>& r) {
  if (l->Equals(true)) return r;
  if (r->Equals(true)) return l;
  return and_(l, r);
}

FragmentIterator FileSystemDataset::GetFragmentsImpl(
    std::shared_ptr<ScanOptions> root_options) {
  FragmentVector fragments;

  std::vector<std::shared_ptr<ScanOptions>> options(forest_.size());

  ExpressionVector fragment_partitions(forest_.size());

  auto collect_fragments = [&](fs::PathForest::Ref ref) -> fs::PathForest::MaybePrune {
    auto partition = partitions_[ref.i];

    // if available, copy parent's filter and projector
    // (which are appropriately simplified and loaded with default values)
    if (auto parent = ref.parent()) {
      options[ref.i].reset(new ScanOptions(*options[parent.i]));
      fragment_partitions[ref.i] =
          FoldingAnd(fragment_partitions[parent.i], partitions_[ref.i]);
    } else {
      options[ref.i].reset(new ScanOptions(*root_options));
      fragment_partitions[ref.i] = FoldingAnd(partition_expression_, partitions_[ref.i]);
    }

    // simplify filter by partition information
    auto filter = options[ref.i]->filter->Assume(partition);
    options[ref.i]->filter = filter;

    if (filter->IsNull() || filter->Equals(false)) {
      // directories (and descendants) which can't satisfy the filter are pruned
      return fs::PathForest::Prune;
    }

    // if possible, extract a partition key and pass it to the projector
    RETURN_NOT_OK(KeyValuePartitioning::SetDefaultValuesFromKeys(
        *partition, &options[ref.i]->projector));

    if (ref.info().IsFile()) {
      // generate a fragment for this file
      FileSource src(ref.info().path(), filesystem_.get());
      ARROW_ASSIGN_OR_RAISE(auto fragment,
                            format_->MakeFragment(std::move(src), options[ref.i],
                                                  std::move(fragment_partitions[ref.i])));
      fragments.push_back(std::move(fragment));
    }

    return fs::PathForest::Continue;
  };

  auto status = forest_.Visit(collect_fragments);
  if (!status.ok()) {
    return MakeErrorIterator<std::shared_ptr<Fragment>>(status);
  }

  return MakeVectorIterator(std::move(fragments));
}

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Write(
    const WritePlan& plan, std::shared_ptr<ScanContext> scan_context) {
  std::vector<std::shared_ptr<ScanOptions>> options(plan.paths.size());

  auto filesystem = plan.filesystem;
  if (filesystem == nullptr) {
    filesystem = std::make_shared<fs::LocalFileSystem>();
  }

  std::vector<fs::FileInfo> files(plan.paths.size());
  ExpressionVector partition_expressions(plan.paths.size(), scalar(true));
  auto task_group = scan_context->TaskGroup();

  auto partition_base_dir = fs::internal::EnsureTrailingSlash(plan.partition_base_dir);
  auto extension = "." + plan.format->type_name();

  for (size_t i = 0; i < plan.paths.size(); ++i) {
    const auto& op = plan.fragment_or_partition_expressions[i];
    if (util::holds_alternative<std::shared_ptr<Expression>>(op)) {
      files[i].set_type(fs::FileType::Directory);
      files[i].set_path(partition_base_dir + plan.paths[i]);

      partition_expressions[i] = util::get<std::shared_ptr<Expression>>(op);
    } else {
      files[i].set_type(fs::FileType::File);
      files[i].set_path(partition_base_dir + plan.paths[i] + extension);

      const auto& fragment = util::get<std::shared_ptr<Fragment>>(op);

      FileSource dest(files[i].path(), filesystem.get());
      ARROW_ASSIGN_OR_RAISE(
          auto write_task,
          plan.format->WriteFragment(std::move(dest), fragment, scan_context));

      task_group->Append([write_task] { return write_task->Execute(); });
    }
  }

  RETURN_NOT_OK(task_group->Finish());

  ARROW_ASSIGN_OR_RAISE(auto forest, fs::PathForest::MakeFromPreSorted(std::move(files)));

  auto partition_expression = scalar(true);
  return Make(plan.schema, partition_expression, plan.format, std::move(filesystem),
              std::move(forest), std::move(partition_expressions));
}

Status WriteTask::CreateDestinationParentDir() const {
  if (auto filesystem = destination_.filesystem()) {
    auto parent = fs::internal::GetAbstractPathParent(destination_.path()).first;
    return filesystem->CreateDir(parent, /* recursive = */ true);
  }

  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow
