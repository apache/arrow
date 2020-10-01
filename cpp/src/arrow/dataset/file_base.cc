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
#include "arrow/dataset/scanner_internal.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/map.h"
#include "arrow/util/mutex.h"
#include "arrow/util/task_group.h"

namespace arrow {
namespace dataset {

Result<std::shared_ptr<io::RandomAccessFile>> FileSource::Open() const {
  if (filesystem_) {
    return filesystem_->OpenInputFile(file_info_);
  }

  if (buffer_) {
    return std::make_shared<io::BufferReader>(buffer_);
  }

  return custom_open_();
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Schema> physical_schema) {
  return MakeFragment(std::move(source), scalar(true), std::move(physical_schema));
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression) {
  return MakeFragment(std::move(source), std::move(partition_expression), nullptr);
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression,
    std::shared_ptr<Schema> physical_schema) {
  return std::shared_ptr<FileFragment>(
      new FileFragment(std::move(source), shared_from_this(),
                       std::move(partition_expression), std::move(physical_schema)));
}

Result<std::shared_ptr<Schema>> FileFragment::ReadPhysicalSchemaImpl() {
  return format_->Inspect(source_);
}

Result<ScanTaskIterator> FileFragment::Scan(std::shared_ptr<ScanOptions> options,
                                            std::shared_ptr<ScanContext> context) {
  return format_->ScanFile(std::move(options), std::move(context), this);
}

FileSystemDataset::FileSystemDataset(std::shared_ptr<Schema> schema,
                                     std::shared_ptr<Expression> root_partition,
                                     std::shared_ptr<FileFormat> format,
                                     std::shared_ptr<fs::FileSystem> filesystem,
                                     std::vector<std::shared_ptr<FileFragment>> fragments)
    : Dataset(std::move(schema), std::move(root_partition)),
      format_(std::move(format)),
      filesystem_(std::move(filesystem)),
      fragments_(std::move(fragments)) {}

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Make(
    std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
    std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
    std::vector<std::shared_ptr<FileFragment>> fragments) {
  return std::shared_ptr<FileSystemDataset>(new FileSystemDataset(
      std::move(schema), std::move(root_partition), std::move(format),
      std::move(filesystem), std::move(fragments)));
}

Result<std::shared_ptr<Dataset>> FileSystemDataset::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  RETURN_NOT_OK(CheckProjectable(*schema_, *schema));
  return std::shared_ptr<Dataset>(new FileSystemDataset(
      std::move(schema), partition_expression_, format_, filesystem_, fragments_));
}

std::vector<std::string> FileSystemDataset::files() const {
  std::vector<std::string> files;

  for (const auto& fragment : fragments_) {
    files.push_back(fragment->source().path());
  }

  return files;
}

std::string FileSystemDataset::ToString() const {
  std::string repr = "FileSystemDataset:";

  if (fragments_.empty()) {
    return repr + " []";
  }

  for (const auto& fragment : fragments_) {
    repr += "\n" + fragment->source().path();

    const auto& partition = fragment->partition_expression();
    if (!partition->Equals(true)) {
      repr += ": " + partition->ToString();
    }
  }

  return repr;
}

FragmentIterator FileSystemDataset::GetFragmentsImpl(
    std::shared_ptr<Expression> predicate) {
  FragmentVector fragments;

  for (const auto& fragment : fragments_) {
    if (predicate->IsSatisfiableWith(fragment->partition_expression())) {
      fragments.push_back(fragment);
    }
  }

  return MakeVectorIterator(std::move(fragments));
}

Status FileWriter::Write(RecordBatchReader* batches) {
  for (std::shared_ptr<RecordBatch> batch;;) {
    RETURN_NOT_OK(batches->ReadNext(&batch));
    if (batch == nullptr) break;
    RETURN_NOT_OK(Write(batch));
  }
  return Status::OK();
}

struct NextBasenameGenerator {
  static Result<NextBasenameGenerator> Make(const std::string& basename_template) {
    if (basename_template.find(fs::internal::kSep) != std::string::npos) {
      return Status::Invalid("basename_template contained '/'");
    }
    size_t token_start = basename_template.find(token());
    if (token_start == std::string::npos) {
      return Status::Invalid("basename_template did not contain '{i}'");
    }
    return NextBasenameGenerator{basename_template, 0, token_start,
                                 token_start + token().size()};
  }

  static const std::string& token() {
    static const std::string token = "{i}";
    return token;
  }

  const std::string& template_;
  size_t i_, token_start_, token_end_;

  std::string operator()() {
    return template_.substr(0, token_start_) + std::to_string(i_++) +
           template_.substr(token_end_);
  }
};

using MutexedWriter = util::Mutexed<std::shared_ptr<FileWriter>>;

struct WriterSet {
  WriterSet(NextBasenameGenerator next_basename,
            const FileSystemDatasetWriteOptions& write_options)
      : next_basename_(std::move(next_basename)),
        base_dir_(fs::internal::EnsureTrailingSlash(write_options.base_dir)),
        write_options_(write_options) {}

  Result<std::shared_ptr<MutexedWriter>> Get(const Expression& partition_expression,
                                             const std::shared_ptr<Schema>& schema) {
    ARROW_ASSIGN_OR_RAISE(auto part_segments,
                          write_options_.partitioning->Format(partition_expression));
    std::string dir = base_dir_ + part_segments;

    util::Mutex::Guard writer_lock;

    auto set_lock = mutex_.Lock();

    auto writer =
        internal::GetOrInsertGenerated(&dir_to_writer_, dir, [&](const std::string&) {
          auto writer = std::make_shared<MutexedWriter>();
          writer_lock = writer->Lock();
          return writer;
        })->second;

    if (writer_lock) {
      // NB: next_basename_() must be invoked with the set_lock held
      auto path = fs::internal::ConcatAbstractPath(dir, next_basename_());
      set_lock.Unlock();

      RETURN_NOT_OK(write_options_.filesystem->CreateDir(dir));

      ARROW_ASSIGN_OR_RAISE(auto destination,
                            write_options_.filesystem->OpenOutputStream(path));

      ARROW_ASSIGN_OR_RAISE(**writer, write_options_.format()->MakeWriter(
                                          std::move(destination), schema,
                                          write_options_.file_write_options));
    }

    return writer;
  }

  Status FinishAll(internal::TaskGroup* task_group) {
    for (const auto& dir_writer : dir_to_writer_) {
      task_group->Append([&] {
        std::shared_ptr<FileWriter> writer = **dir_writer.second;
        return writer->Finish();
      });
    }

    return Status::OK();
  }

  // There should only be a single writer open for each partition directory at a time
  util::Mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<MutexedWriter>> dir_to_writer_;
  NextBasenameGenerator next_basename_;
  std::string base_dir_;
  const FileSystemDatasetWriteOptions& write_options_;
};

Status FileSystemDataset::Write(const FileSystemDatasetWriteOptions& write_options,
                                std::shared_ptr<Scanner> scanner) {
  auto task_group = scanner->context()->TaskGroup();

  // Things we'll un-lazy for the sake of simplicity, with the tradeoff they represent:
  //
  // - Fragment iteration. Keeping this lazy would allow us to start partitioning/writing
  //   any fragments we have before waiting for discovery to complete. This isn't
  //   currently implemented for FileSystemDataset anyway: ARROW-8613
  //
  // - ScanTask iteration. Keeping this lazy would save some unnecessary blocking when
  //   writing Fragments which produce scan tasks slowly. No Fragments do this.
  //
  // NB: neither of these will have any impact whatsoever on the common case of writing
  //     an in-memory table to disk.
  ARROW_ASSIGN_OR_RAISE(FragmentVector fragments, scanner->GetFragments().ToVector());
  ScanTaskVector scan_tasks;
  std::vector<const Fragment*> fragment_for_task;

  // Avoid contention with multithreaded readers
  auto context = std::make_shared<ScanContext>(*scanner->context());
  context->use_threads = false;

  for (const auto& fragment : fragments) {
    auto options = std::make_shared<ScanOptions>(*scanner->options());
    ARROW_ASSIGN_OR_RAISE(auto scan_task_it,
                          Scanner(fragment, std::move(options), context).Scan());
    for (auto maybe_scan_task : scan_task_it) {
      ARROW_ASSIGN_OR_RAISE(auto scan_task, maybe_scan_task);
      scan_tasks.push_back(std::move(scan_task));
      fragment_for_task.push_back(fragment.get());
    }
  }

  ARROW_ASSIGN_OR_RAISE(auto next_basename,
                        NextBasenameGenerator::Make(write_options.basename_template));
  WriterSet writers(std::move(next_basename), write_options);

  auto write_task_group = task_group->MakeSubGroup();
  auto fragment_for_task_it = fragment_for_task.begin();
  for (const auto& scan_task : scan_tasks) {
    const Fragment* fragment = *fragment_for_task_it++;

    write_task_group->Append([&, scan_task, fragment] {
      ARROW_ASSIGN_OR_RAISE(auto batches, scan_task->Execute());

      for (auto maybe_batch : batches) {
        ARROW_ASSIGN_OR_RAISE(auto batch, maybe_batch);
        ARROW_ASSIGN_OR_RAISE(auto groups, write_options.partitioning->Partition(batch));
        batch.reset();  // drop to hopefully conserve memory

        struct PendingWrite {
          std::shared_ptr<MutexedWriter> writer;
          std::shared_ptr<RecordBatch> batch;

          Result<bool> TryWrite() {
            if (auto lock = writer->TryLock()) {
              RETURN_NOT_OK(writer->get()->Write(batch));
              return true;
            }
            return false;
          }
        };
        std::vector<PendingWrite> pending_writes(groups.batches.size());

        for (size_t i = 0; i < groups.batches.size(); ++i) {
          AndExpression partition_expression(std::move(groups.expressions[i]),
                                             fragment->partition_expression());

          ARROW_ASSIGN_OR_RAISE(auto writer, writers.Get(partition_expression,
                                                         groups.batches[i]->schema()));

          pending_writes[i] = {std::move(writer), std::move(groups.batches[i])};
        }

        for (size_t i = 0; !pending_writes.empty(); ++i) {
          if (i >= pending_writes.size()) {
            i = 0;
          }

          ARROW_ASSIGN_OR_RAISE(auto success, pending_writes[i].TryWrite());
          if (success) {
            std::swap(pending_writes.back(), pending_writes[i]);
            pending_writes.pop_back();
          }
        }
      }

      return Status::OK();
    });
  }
  RETURN_NOT_OK(write_task_group->Finish());

  RETURN_NOT_OK(writers.FinishAll(task_group.get()));
  return task_group->Finish();
}

}  // namespace dataset
}  // namespace arrow
